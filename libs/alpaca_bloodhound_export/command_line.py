"""Offline export; only the final Parquet is a persistent output."""

import json
import os
import resource
import tempfile
from contextlib import ExitStack
from datetime import date
from pathlib import Path

from alpaca_end_of_day import StandaloneReader, StandaloneReaderError
from libs.alpaca_heartbeat import diagnostic
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from . import VERSION, validate_version
from .memory import DEFAULT_MEMORY_BYTES, OutputMemory
from .projection import COLUMNS, ExportError, checked_ids, project_history


class SnapshotChangedError(ExportError):
    """A moving input aborts the export under either selection policy."""


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _diagnose(errors, stage, callback):
    """A diagnostic cannot undo a completed publication."""
    try:
        callback()
    except Exception as error:
        errors.append({"stage": stage, "error_type": type(error).__name__, "message": str(error)})


def schema():
    import pyarrow as pa

    return pa.schema(
        [
            (
                name,
                pa.timestamp("ns")
                if name == "date"
                else pa.int64()
                if name == "ohlcv_series_id"
                else pa.string()
                if index < 9
                else pa.float64(),
            )
            for index, name in enumerate(COLUMNS)
        ]
    )


def export(
    reader,
    output,
    *,
    symbols=None,
    public_ids=None,
    start=None,
    end=None,
    include_all_passed=False,
    replace=False,
    progress=None,
    memory_bytes=DEFAULT_MEMORY_BYTES,
):
    """Freeze metadata, consume each ID once, validate output, publish atomically."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    validate_version()
    output = Path(output).absolute()
    if output.suffix.lower() != ".parquet":
        raise ExportError("--output must name a .parquet file")
    if not output.parent.is_dir():
        raise ExportError("output parent directory must already exist")
    if output.is_symlink() or (output.exists() and (not replace or not output.is_file())):
        raise ExportError("output exists or is unsafe; use --replace only for an existing export file")
    if output.resolve().is_relative_to(reader.store.resolve()):
        raise ExportError("output must be outside the source datastore")
    if start:
        start = date.fromisoformat(start).isoformat()
    if end:
        end = date.fromisoformat(end).isoformat()
    if start and end and start > end:
        raise ExportError("--from must not be after --through")
    requested = set(public_ids or ())
    checked_ids(requested)
    # This public roster selects completed lineage generations, not eligibility
    # or current readability. Each selected group supplies its own locked body;
    # DO NOT add a preliminary receipt/payload census to authenticate the roster.
    roster = reader.list_published_sources()
    frozen = {item["symbol"]: item["generation"] for item in roster["sources"]}
    selected = sorted(set(symbols or frozen))
    selected_generations = {name: frozen.get(name) for name in selected}
    groups, securities, identities, reverse = {}, {}, {}, {}
    failure_count, first_failure = 0, None
    seen, pinned = set(), {}

    def failed(locator, error, public_id=None):
        nonlocal failure_count, first_failure
        failure_count += 1
        message = f"rejected source={locator or '-'} public_id={public_id or '-'}: {error}"
        if first_failure is None:
            first_failure = message
        if progress:
            progress(message)

    collected = OutputMemory(memory_bytes)
    configured_cache = reader.cache_info().get("maximum_bytes", 0)
    output_schema = schema()
    temporary = None
    published = False
    diagnostic_errors = []
    mappings, row_count = [], 0
    try:
        with collected:
            if progress:
                progress("collecting accepted Arrow data in RAM; no Parquet writes during acquisition")
            for position, symbol in enumerate(selected, 1):
                if symbol in seen:
                    continue
                with ExitStack() as scope:
                    try:
                        group = scope.enter_context(reader.open_group(symbol))
                        evidence = group.summary
                    except (StandaloneReaderError, ValueError, KeyError) as error:
                        failed(symbol, error)
                        if not include_all_passed:
                            raise ExportError(f"{first_failure}; no file published") from error
                        continue
                    for name, source in evidence["sources"].items():
                        if source["lineage_generation"] != frozen.get(name):
                            raise SnapshotChangedError(
                                "publication changed after source roster selection; retry export"
                            )
                    generation = evidence["generation"]
                    group.set_cache_limit(min(configured_cache, collected.available_bytes))
                    seen.update(evidence["source_symbols"])
                    pinned.update(
                        {name: source["lineage_generation"] for name, source in evidence["sources"].items()}
                    )
                    groups[generation] = {
                        "read_symbol": symbol,
                        "sources": evidence["sources"],
                        "snapshot": group,
                    }
                    selected_ids = []
                    for security in evidence["securities"]:
                        public_id = security["public_id"]
                        if requested and public_id not in requested:
                            continue
                        prior = securities.get(public_id)
                        if prior and prior["generation"] != generation:
                            raise SnapshotChangedError(
                                f"public ID appears in distinct publication groups: {public_id}"
                            )
                        number = checked_ids([public_id])[public_id]
                        if number in reverse and reverse[number] != public_id:
                            raise ExportError(f"int64 identity collision: {public_id} and {reverse[number]}")
                        identities[public_id], reverse[number] = number, public_id
                        securities[public_id] = {**security, "generation": generation}
                        selected_ids.append(public_id)
                    if not evidence["securities"] and progress:
                        progress(f"not selected source={symbol}: no resolved public IDs")
                    for public_id in sorted(selected_ids, key=identities.get):
                        security = securities[public_id]
                        try:
                            history = group.get_histories(public_id=public_id)
                            if history["generation"] != generation:
                                raise SnapshotChangedError(
                                    "publication changed after identity selection; retry export"
                                )
                            rows, mapping = project_history(history, identities[public_id], start=start, end=end)
                        except SnapshotChangedError:
                            raise
                        except (StandaloneReaderError, ValueError, KeyError, StopIteration) as error:
                            failed(security["read_symbol"], error, public_id)
                            if not include_all_passed:
                                raise ExportError(f"{public_id}: {error}; no file published") from error
                            continue
                        if not rows:
                            if progress:
                                progress(
                                    f"not selected public_id={public_id}: no observations in requested window"
                                )
                            continue
                        collected.add(pa.Table.from_pylist(rows, schema=output_schema))
                        mappings.append(mapping)
                        row_count += len(rows)
                        del rows, history
                        group.set_cache_limit(min(configured_cache, collected.available_bytes))
                    if progress:
                        progress(
                            f"processed {position}/{len(selected)} source locators; "
                            f"converted {len(mappings)} identities; {row_count} rows"
                        )
            for public_id in sorted(requested - securities.keys()):
                failed("", "requested public ID not found in selected completed groups", public_id)
            if failure_count and not include_all_passed:
                raise ExportError(f"{first_failure}; no file published")
            if progress:
                progress(f"selected {len(securities)} public IDs from {len(selected)} source locators")
            if not row_count:
                raise ExportError("no valid observations in the requested window; no file published")
            published_generations = {mapping["generation"] for mapping in mappings}
            manifest = {
                "converter_version": VERSION,
                "format": "bloodhound_18_column_parquet",
                "compatibility": "schema_compatible_not_catalog_registered_or_action_certified",
                "price_basis": "alpaca_all_preserved",
                "volume_basis": "alpaca_raw_as_traded",
                "split_factor_basis": "eight_view_joint_model_provider_implied_not_action_certified",
                "share_volume_basis": "unavailable_without_independent_action_semantics",
                "bloodhound_admission": False,
                "symbol_policy": "latest_observed_ticker_for_full_public_id_history",
                "dated_type_and_exchange": "UNKNOWN",
                "optional_identifiers": "null",
                "adjustment_vintage": "stored_provider_snapshot_not_request_asof",
                "identity_rule": "1 + int(public_id,16) % (2**63 - 1); collision_checked",
                "source_roster_generation": roster["generation"],
                "groups": {
                    key: {field: value[field] for field in ("read_symbol", "sources")}
                    for key, value in groups.items()
                    if key in published_generations
                },
                "calendar_sha256": us_market_hours_sha256(),
                "range": {"from": start, "to": end, "inclusive": True},
                "rows": row_count,
                "published_identities": len(mappings),
                "identities": mappings,
                "writer": {"pyarrow": pa.__version__, "compression": "zstd", "row_group_size": 131072},
            }
            if progress:
                progress(
                    f"serializing {row_count} rows from RAM in one Parquet write; "
                    f"{collected.data_bytes} columnar bytes, {collected.used_bytes} accounted retained bytes"
                )
            descriptor, temporary = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=output.parent)
            os.close(descriptor)
            temporary = Path(temporary)
            with pq.ParquetWriter(temporary, output_schema, compression="zstd", version="2.6") as writer:
                writer.write_table(collected.table(), row_group_size=131072)
                collected.clear()
                if progress:
                    progress(f"checking {len(groups)} consumed groups before publication")
                # One final locked freshness gate, after serialization. Never reopen prices.
                # Group consumption released its locks, so old tokens alone do
                # not establish freshness. Recheck selection, including omitted
                # inputs, without replaying conversion or reopening price bodies.
                current = {
                    item["symbol"]: item["generation"] for item in reader.list_published_sources()["sources"]
                }
                if any(
                    current.get(name) != generation
                    for name, generation in {**selected_generations, **pinned}.items()
                ):
                    raise ExportError("selected lineage publications changed during export; retry export")
                for group in groups.values():
                    group["snapshot"].check_current()
                writer.add_key_value_metadata({"alpaca_bloodhound": _json(manifest)})
        # This is validation of the exporter's newly serialized artifact, not
        # authentication of upstream histories. Once atomically installed, the
        # file is authoritative; diagnostic/cleanup failures cannot undo success.
        with pq.ParquetFile(temporary) as parquet:
            if parquet.metadata.num_rows != row_count or not parquet.schema_arrow.equals(
                schema(), check_metadata=False
            ):
                raise ExportError("written Parquet schema or row count differs from conversion")
        if replace:
            os.replace(temporary, output)
        else:
            # Atomic no-clobber publication; the temporary name is then removed.
            os.link(temporary, output)
        published = True
        if progress:
            _diagnose(
                diagnostic_errors,
                "publication_progress",
                lambda: progress(
                    f"published {len(mappings)} identities; {row_count} rows; "
                    f"{failure_count} source/identity checks rejected"
                ),
            )
        return {
            "output": str(output),
            "rows": row_count,
            "identities": len(mappings),
            "diagnostic_errors": diagnostic_errors,
        }
    finally:
        if temporary is not None:
            if published:
                _diagnose(diagnostic_errors, "temporary_cleanup", lambda: temporary.unlink(missing_ok=True))
            else:
                temporary.unlink(missing_ok=True)


def _progress_callback(reporter):
    def progress(message):
        if reporter is None or message.startswith(("rejected ", "not selected ")):
            diagnostic(reporter, message, level="warning" if message.startswith("rejected ") else "info")
        else:
            reporter.update("EXPORT", message=message, context={"estimated_remaining_seconds": "TBD"})

    return progress


def run_arguments(args):
    if args.cache_mib < 0 or args.cache_mib > 6144:
        raise ValueError(
            "--cache-mib must be between 0 and 6144 (leave headroom for active histories and writer)"
        )
    if not 256 <= args.memory_mib <= 8192:
        raise ValueError("--memory-mib must be between 256 and 8192")
    try:
        reader = StandaloneReader(args.store, wait=args.wait, cache_bytes=args.cache_mib * 1024**2)
        result = export(
            reader,
            args.output,
            symbols=[s.strip().upper() for s in args.symbol] if args.symbol else None,
            public_ids=args.public_id,
            start=args.start,
            end=args.end,
            include_all_passed=args.include_all_passed,
            replace=args.replace,
            progress=_progress_callback(getattr(args, "_lifecycle", None)),
            memory_bytes=args.memory_mib * 1024**2,
        )
        errors = result["diagnostic_errors"]
        _diagnose(
            errors,
            "cache_reporting",
            lambda: diagnostic(getattr(args, "_lifecycle", None), "cache " + _json(reader.cache_info())),
        )
        _diagnose(
            errors,
            "memory_reporting",
            lambda: diagnostic(
                getattr(args, "_lifecycle", None),
                f"peak_rss_mib={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.2f}",
            ),
        )
        _diagnose(errors, "result_reporting", lambda: print(_json(result)))
        return 0
    except ImportError as error:
        diagnostic(
            getattr(args, "_lifecycle", None),
            f"error: install requirements-bloodhound.txt with Python 3.11: {error}",
            level="error",
        )
    except (StandaloneReaderError, ExportError, OSError, ValueError) as error:
        diagnostic(getattr(args, "_lifecycle", None), f"error: {error}", level="error")
    return 1
