"""Read the standalone twins' published assignments and prices, without a catalogue.

This is a resolved-data interface, not DataLibrary's online-availability contract.
The source symbol scopes a published dependency group; it never becomes an ID.
"""

import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
from pathlib import Path, PurePosixPath

from libs.alpaca_end_of_day_reader.errors import EndOfDayReaderError
from libs.alpaca_end_of_day_reader.history import _resolve_bar_candidates
from libs.alpaca_lineage_build.acquisition import check_unavailable_pricing, unavailable_pricing
from libs.alpaca_lineage_build.contract import (
    LineageBusyError,
    document_digest,
    reusable_versions,
    selected_document,
)
from libs.alpaca_lineage_build.storage import (
    check_store,
    publication_lock,
    read_boundaries,
    read_index,
    reader_snapshot,
    select_inputs,
)
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    COLUMNS,
    NEW_YORK,
    SUPPORTED_VERSIONS,
    decode_csv,
    symbol_label,
    timestamp,
)
from libs.alpaca_pricing_download.storage import load_source, published_path, read_document, read_update

from ._standalone_cache import GenerationCache


class StandaloneReaderError(RuntimeError):
    """A completed, consistent standalone publication cannot be read."""


class ReaderBusyError(StandaloneReaderError):
    """A producer owns a required lock; retry later or explicitly wait."""


class AmbiguousTickerError(StandaloneReaderError):
    """The ticker has multiple published identities; select a public ID."""


class UnservableHistoryError(StandaloneReaderError):
    """Completed assignments select conflicting or duplicate daily observations."""


class HistoryPrefixChanged(StandaloneReaderError):
    """An extension changes earlier source selection; select the affected history."""


def _date(value, name):
    if value is None or type(value) is date:
        return value
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
            if parsed.isoformat() == value:
                return parsed
        except ValueError:
            pass
    raise ValueError(f"{name} must be a date or YYYY-MM-DD")


def _public_id(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError("public_id must be a 64-hex ID")
    return value.lower()


def _symbol(value):
    if not isinstance(value, str) or re.fullmatch(r"[A-Z][A-Z0-9.-]*", value.strip().upper()) is None:
        raise ValueError("symbol must be a ticker label")
    return symbol_label(value)


def _assignments(boundaries, receipt, *, include_unresolved=False):
    for kind in ("completed_years", "current_year", "current_month"):
        for entry in boundaries[kind]["adjustments"]:
            for segment in entry["segments"]:
                if kind == "current_year" and segment["from"][:7] >= receipt["anchor_month"]:
                    continue
                if include_unresolved or segment["branch"] == "correlated":
                    yield entry["adjustment"], kind, segment


def _identity(segment):
    return segment["security_identity"]["security_id"]


def _matches(assignments, symbol=None):
    selected = {
        _identity(segment)
        for source_symbol, items in assignments.items()
        for _, _, segment in items
        if symbol is None or source_symbol == symbol or segment.get("historical_symbol") == symbol
    }
    results = []
    for public_id in sorted(selected):
        matching = [
            (adjustment, segment)
            for items in assignments.values()
            for adjustment, _, segment in items
            if _identity(segment) == public_id
        ]
        results.append(
            {
                "public_id": public_id,
                "tickers": sorted({s["historical_symbol"] for _, s in matching}),
                "first_session": min(s["from"] for _, s in matching),
                "last_session": max(s["to"] for _, s in matching),
                "adjustments": [name for name in ADJUSTMENTS if any(a == name for a, _ in matching)],
            }
        )
    return results


def _group_securities(assignments):
    securities = _matches(assignments)
    for security in securities:
        matching = [
            (source, segment)
            for source, items in assignments.items()
            for _, _, segment in items
            if _identity(segment) == security["public_id"]
        ]
        security["read_symbol"] = min(source for source, _ in matching)
        security["latest_tickers"] = sorted(
            {
                segment["historical_symbol"]
                for _, segment in matching
                if segment["to"] == security["last_session"]
            }
        )
    return securities


def _group_evidence(assignments, sources, receipts, generations, generation):
    """Project published evidence, without inferring metadata or action ownership.

    A dependency group can contain unrelated securities. Action symbols, current
    asset lookups and the request's asof are evidence, not historical ownership,
    dated listing metadata or an adjustment-calculation timestamp.
    """
    securities = _group_securities(assignments)
    projected = {}
    for symbol, items in assignments.items():
        compact = {}
        for adjustment, _, segment in items:
            value = {
                name: segment.get(name)
                for name in (
                    "from",
                    "to",
                    "historical_symbol",
                    "canonical_security",
                    "security_identity",
                    "ticker_ownership_interval",
                )
            }
            key = document_digest(value)
            if key not in compact:
                compact[key] = {**value, "adjustments": []}
            if adjustment not in compact[key]["adjustments"]:
                compact[key]["adjustments"].append(adjustment)
        projected[symbol] = {
            "lineage_generation": generations[symbol],
            "pricing_generation": receipts[symbol]["source_sha256"],
            "pricing_request": sources[symbol]["latest_request"],
            "history_start": sources[symbol]["history_start"],
            "processed_through": receipts[symbol]["processed_through"],
            "evidence": receipts[symbol]["evidence"],
            "assignments": list(compact.values()),
        }
    return {
        "generation": generation,
        "source_symbols": sorted(sources),
        "securities": securities,
        "sources": projected,
    }


def _selected_path(cache, root, relative):
    # Only reused inside one locked read call. A later snapshot starts fresh.
    key = "selected_path", root, relative
    if key not in cache:
        cache[key] = published_path(root, relative)
    return cache[key]


def _rows(root, source, kind, adjustment, cache, retained=None):
    # Only the selected physical payload is opened. Alias equality and source
    # validation belong to the downloader, not to this consumer.
    relative = source["boundaries"][kind]["manifest"]
    manifest_path = _selected_path(cache, root, relative)
    if manifest_path not in cache:
        cache[manifest_path] = (
            retained.document(manifest_path, source["boundaries"][kind]["sha256"])
            if retained is not None
            else read_document(manifest_path)
        )
    elif retained is not None:
        retained.ledger.record("manifest_reuses")
    manifest = cache[manifest_path]
    if (
        manifest.get("schema_version") not in SUPPORTED_VERSIONS
        or manifest.get("symbol") != source["symbol"]
        or manifest.get("boundary") != "alpaca_pricing_" + kind
    ):
        raise StandaloneReaderError("pricing boundary has a different owner or version")
    entry = next(item for item in manifest["adjustments"] if item["adjustment"] == adjustment)
    if entry["storage"] == "empty":
        return ()
    if entry["storage"] not in {"file", "exact_copy"}:
        raise StandaloneReaderError("unsupported pricing storage selection")
    relative = str(PurePosixPath(relative).parent / entry["file"])
    payload = _selected_path(cache, root, relative)
    if payload not in cache:
        cache[payload] = (
            retained.rows(payload, entry["sha256"]) if retained is not None else decode_csv(payload.read_bytes())
        )
    elif retained is not None:
        retained.ledger.record("payload_reuses")
    return cache[payload]


def _history(
    assignments,
    sources,
    store,
    public_id,
    adjustment,
    cache,
    retained=None,
    *,
    prefix=None,
    coverage_out=None,
    io_cache=None,
):
    from ._history_coverage import SelectedCoverage, extend_coverage, preserves_selection

    selected = []
    for symbol, items in assignments.items():
        by_boundary = defaultdict(list)
        for view, kind, segment in items:
            if view == adjustment and _identity(segment) == public_id:
                by_boundary[kind].append(segment)
        for kind, segments in by_boundary.items():
            if prefix is not None:
                skip = prefix["rows"].get(symbol, {}).get(adjustment, 0)
                offset = prefix["offsets"][symbol][adjustment][kind]
                segments = [
                    {**segment, "row_start": max(segment["row_start"], skip - offset + 1)}
                    for segment in segments
                    if offset + segment["row_end"] > skip
                ]
                if not segments:
                    continue
            root = _selected_path(cache, store, f"end_of_day/pricing/{symbol}")
            row_cache = io_cache.setdefault(symbol, {}) if io_cache is not None else cache
            rows = _rows(root, sources[symbol], kind, adjustment, row_cache, retained)
            for segment in segments:
                start, end = segment["row_start"], segment["row_end"]
                if not 1 <= start <= end <= len(rows):
                    raise StandaloneReaderError("lineage row selection exceeds its pricing boundary")
                selected.append((symbol, kind, rows, start, end, segment["historical_symbol"]))
    # Exact physical aliases with identical row selections and overlap policy
    # need only one assembly. Keep raw's different source policy in the key.
    # Decoded rows remain alive in this call's cache, so their object IDs are stable.
    key = (
        "assembled_history",
        public_id,
        adjustment != "raw",
        tuple((symbol, kind, id(rows), start, end, label) for symbol, kind, rows, start, end, label in selected),
    )
    if key in cache and prefix is None:
        if coverage_out is not None:
            coverage_out.update(cache[("coverage_order", key)])
        # Public view results remain independently mutable.
        return [{**bar, "tickers": list(bar["tickers"])} for bar in cache[key]]
    observations, overlaps, coverage = {}, {}, defaultdict(set)
    for symbol, _, rows, start, end, label in selected:
        for row in rows[start - 1 : end]:
            if prefix is not None:
                session_key = "session", row[0]
                if session_key not in cache:
                    cache[session_key] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                if cache[session_key] <= prefix["last_session"]:
                    raise HistoryPrefixChanged("new assigned observations overlap the approved history")
            coverage[symbol].add(row[0])
            observation = row, symbol, label
            if row[0] in observations:
                overlaps.setdefault(row[0], [observations[row[0]]]).append(observation)
            else:
                observations[row[0]] = observation
    old_order = prefix["coverage"][adjustment] if prefix is not None else None
    order = extend_coverage(coverage, old_order)
    if prefix is not None and adjustment != "raw" and not preserves_selection(old_order, order):
        raise HistoryPrefixChanged("extended coverage changes historical source selection")
    if coverage_out is not None:
        coverage_out.update(order)
    cache[("coverage_order", key)] = order
    # Reuse the established history-source policy, not a new identity rule:
    # adjusted overlaps prefer strictly broader correlated coverage, then the
    # historical ticker; raw overlaps use the historical ticker. Unsettled
    # conflicts fail, and exact aliases collapse to a single observation.
    if not overlaps:
        # There is no ownership/price competition to resolve. Preserve exact
        # approved values and their sole label without building comparison keys
        # or temporary resolution/provenance records for every observation.
        chosen = ((row, [label]) for _, (row, _, label) in sorted(observations.items()))
    else:
        candidates = {
            instant: [
                {
                    "values": dict(zip(COLUMNS, row)),
                    "provenance": {"source_id": symbol, "symbol": symbol, "historical_symbol": label},
                    "integrity_error_ids": [],
                }
                for row, symbol, label in items
            ]
            for instant, items in overlaps.items()
        }
        try:
            bars, _ = _resolve_bar_candidates(
                candidates,
                {symbol: SelectedCoverage(order, symbol) for symbol in coverage},
                allow_strict_superset_selection=adjustment != "raw",
            )
        except EndOfDayReaderError as error:
            raise UnservableHistoryError(str(error)) from error
        resolved = {bar["timestamp"]: bar for bar in bars}

        def selected_rows():
            for instant, (row, _, label) in sorted(observations.items()):
                if instant in resolved:
                    bar = resolved[instant]
                    yield (
                        tuple(bar[column] for column in COLUMNS),
                        sorted({p["historical_symbol"] for p in bar["provenance"]}),
                    )
                else:
                    yield row, [label]

        chosen = selected_rows()
    if retained is not None:
        retained.ledger.record("uncontested_observations", len(observations) - len(overlaps))
    result, sessions, session_reuses = [], set(), 0
    for row, tickers in chosen:
        session_key = "session", row[0]
        if session_key not in cache:
            cache[session_key] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
        else:
            session_reuses += 1
        session = cache[session_key]
        if session in sessions:
            raise UnservableHistoryError("multiple observations for one daily session")
        sessions.add(session)
        record = {"session": session, "tickers": tickers}
        record.update(zip(COLUMNS, row))
        result.append(record)
    cache[key] = result
    if not overlaps:
        # Raw/adjusted source policies are equivalent when there is no choice.
        # Exact payload/range/label aliases can then share this assembly too.
        cache[(key[0], key[1], not key[2], key[3])] = result
        cache[("coverage_order", (key[0], key[1], not key[2], key[3]))] = order
    if retained is not None:
        retained.ledger.record("session_reuses", session_reuses)
    return result


def _read_views(
    assignments,
    sources,
    store,
    public_id,
    adjustments,
    start,
    end,
    retained=None,
    *,
    prefix=None,
    coverage_out=None,
    io_cache=None,
):
    views, cache = {}, {}
    for adjustment in adjustments:
        selected_coverage = {} if coverage_out is not None else None
        incremental = (
            {"prefix": prefix, "coverage_out": selected_coverage}
            if prefix is not None or coverage_out is not None
            else {}
        )
        if io_cache is not None:
            incremental["io_cache"] = io_cache
        bars = _history(
            assignments,
            sources,
            store,
            public_id,
            adjustment,
            cache,
            retained,
            **incremental,
        )
        if coverage_out is not None:
            coverage_out[adjustment] = selected_coverage
        # Bounds follow source selection, never change the winning source.
        bars = [
            bar
            for bar in bars
            if (start is None or bar["session"] >= start.isoformat())
            and (end is None or bar["session"] <= end.isoformat())
        ]
        views[adjustment] = {"bar_count": len(bars), "bars": bars}
    return views


class StandaloneReader:
    """Credential-free resolved pricing from an explicit standalone store.

    Each method pins its selected group with sorted pricing shared locks.
    A brief publication handshake leaves unrelated groups free to publish.
    No persistent index, cache, catalogue, download or acknowledgement is created.
    Results are snapshots of one completed group, not promises of online status.
    """

    def __init__(self, store: Path | str, *, wait: bool = False, cache_bytes: int = 0):
        from . import _validate_api_change_log

        _validate_api_change_log()
        if type(wait) is not bool:
            raise ValueError("wait must be a boolean")
        self.store = Path(store).absolute()
        self.wait = wait
        self._cache = GenerationCache(cache_bytes)

    def cache_info(self) -> dict:
        """Return bounded RAM-cache accounting; the cache is never a source selector."""
        return self._cache.info()

    @contextmanager
    def _snapshot(
        self,
        symbol,
        *,
        include_assignments=True,
        reuse_documents=False,
        expected=None,
        include_unresolved=False,
        before_payloads=None,
    ):
        try:
            loader = self._cache.document if self._cache.maximum_bytes else None
            if reuse_documents:
                # Private to this lock handshake: parse each selected document
                # once, with no serialization merely to cache it. Nothing from
                # this private dictionary is exposed to callers.
                documents = {}

                def loader(path, generation):
                    key = path, generation
                    if key not in documents:
                        documents[key] = selected_document(path, generation)
                        self._cache.ledger.record("selected_document_reads")
                    else:
                        self._cache.ledger.record("selected_document_reuses")
                    return documents[key]

            def select(root):
                if expected is None:
                    return select_inputs(root, symbol, selected_loader=loader)
                selected = {name: read_index(root, name)["symbols"].get(name) for name in expected["lineage"]}
                if any(
                    (entry["sha256"] if entry is not None else None) != expected["lineage"][name]
                    for name, entry in selected.items()
                ):
                    raise StandaloneReaderError("selected lineage generation changed")
                return selected

            with reader_snapshot(
                self.store,
                select,
                wait=self.wait,
                selected_loader=loader,
                include_boundaries=False,
            ) as snap:
                seed = snap["receipts"][symbol]
                selected_sources = seed["input_sources"]
                if expected is not None and selected_sources != expected["pricing"]:
                    raise StandaloneReaderError("selected pricing generations changed")
                absent = unavailable_pricing(seed)
                check_unavailable_pricing(self.store, absent, snap["locked"])
                sources, assignments, generations, all_assignments = {}, {}, {}, {}
                for name in sorted(selected_sources):
                    pricing = published_path(self.store, f"end_of_day/pricing/{name}")
                    if name not in snap["locked"] or published_path(pricing, ".pending.json").exists():
                        raise StandaloneReaderError("pricing publication incomplete; resume downloader first")
                    source = load_source(pricing, name)
                    if (
                        source is None
                        or read_update(pricing) is not None
                        or document_digest(source) != selected_sources[name]
                    ):
                        raise StandaloneReaderError("lineage awaits changed pricing; run lineage first")
                    receipt = snap["receipts"][name]
                    if receipt is None or not reusable_versions(receipt["versions"]):
                        raise StandaloneReaderError("lineage is missing or uses older rules; run lineage first")
                    if (
                        receipt["input_sources"] != selected_sources
                        or unavailable_pricing(receipt) != absent
                        or receipt["source_sha256"] != selected_sources[name]
                        or receipt["pricing_boundaries"] != source["boundaries"]
                        or receipt["anchor_month"] != source["anchor_month"]
                    ):
                        raise StandaloneReaderError("lineage group selects different input generations")
                    sources[name] = source
                    generations[name] = snap["entries"][name]["sha256"]
                receipts = {name: snap["receipts"][name] for name in selected_sources}
                if before_payloads is not None:
                    before_payloads()
                if include_assignments:
                    root = self.store / "end_of_day/lineage"
                    for name, receipt in receipts.items():
                        boundaries = read_boundaries(root, receipt, selected_loader=loader)
                        assignments[name] = tuple(_assignments(boundaries, receipt))
                        if include_unresolved:
                            all_assignments[name] = tuple(
                                _assignments(boundaries, receipt, include_unresolved=True)
                            )
                values = assignments, sources, document_digest(generations), receipts, generations
                yield (*values, all_assignments) if include_unresolved else values
        except LineageBusyError as error:
            raise ReaderBusyError(str(error)) from error
        except StandaloneReaderError:
            raise
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, StopIteration) as error:
            raise StandaloneReaderError(f"cannot read standalone publication: {error}") from error

    def resolve_ticker(self, symbol: str) -> dict:
        """List IDs assigned to this source or bearing its historical label in the group.

        Source aliases need not equal the dated trading label. This lookup selects
        existing assignments; it never creates an identity from the request label.
        No price payload is read.
        """
        from . import API_VERSION

        symbol = _symbol(symbol)
        with self._snapshot(symbol) as (assignments, _, generation, _, _):
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_ticker_resolution",
                "generation": generation,
                "symbol": symbol,
                "matches": _matches(assignments, symbol),
            }

    def list_published_sources(self) -> dict:
        """List existing indexed source publications, without opening prices.

        This is a roster of publication locators, not an online catalogue or a
        promise that each source is currently readable. Resolve groups through
        get_evidence/get_histories; compare generations to detect changed inputs.
        """
        from . import API_VERSION

        try:
            check_store(self.store)
            root = published_path(self.store, "end_of_day/lineage")
            with publication_lock(root, wait=self.wait):
                if published_path(root, ".pending.json").exists():
                    raise StandaloneReaderError("lineage publication incomplete; resume lineage first")
                index = read_index(root)
                generations = {symbol: entry["sha256"] for symbol, entry in index["symbols"].items()}
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_published_sources",
                "generation": document_digest(generations),
                "sources": [
                    {"symbol": symbol, "generation": generation} for symbol, generation in generations.items()
                ],
            }
        except LineageBusyError as error:
            raise ReaderBusyError(str(error)) from error
        except StandaloneReaderError:
            raise
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            raise StandaloneReaderError(f"cannot read standalone publication: {error}") from error

    def get_evidence(self, symbol: str) -> dict:
        """Read a completed group's published evidence and resolved assignments.

        Securities include a source locator and all latest observed labels; a
        tie is not silently broken. No prices are decoded and no rules replayed.
        Asset metadata is a provider lookup snapshot, not a dated listing fact.
        Corporate actions remain group evidence, not assigned security events.
        """
        from . import API_VERSION

        symbol = _symbol(symbol)
        with self._snapshot(symbol) as (assignments, sources, generation, receipts, generations):
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_group_evidence",
                "symbol": symbol,
                **_group_evidence(assignments, sources, receipts, generations, generation),
            }

    def get_generation(self, symbol: str) -> str:
        """Check current receipts/pricing/pending work without loading assignment boundaries.

        Uses the same lock handshake and freshness gates as a history read. This
        checks a previously consumed boundary; it does not reconstruct its content.
        """
        symbol = _symbol(symbol)
        with self._snapshot(symbol, include_assignments=False) as (_, _, generation, _, _):
            return generation

    @contextmanager
    def open_group(self, symbol: str):
        """Consume a completed group's full histories under one lock handshake.

        The returned snapshot exposes a compact summary and explicit-ID history
        reads. Closing releases all working data and pricing locks; its small
        freshness token can still check_current() before downstream publication.
        """
        with self._open_group(symbol) as group:
            yield group

    @contextmanager
    def _open_group(
        self, symbol, *, expected=None, include_unresolved=False, shared_cache=False, before_payloads=None
    ):
        """Shared locked consumption for inspection, serving production and reads."""
        from ._standalone_group import GroupSnapshot

        symbol = _symbol(symbol)
        options = {"reuse_documents": True}
        if expected is not None:
            options["expected"] = expected
        if include_unresolved:
            options["include_unresolved"] = True
        if before_payloads is not None:
            options["before_payloads"] = before_payloads
        manager = self._snapshot(symbol, **options)
        snapshot = manager.__enter__()
        group = None
        try:
            group = GroupSnapshot(self, symbol, *snapshot, shared_cache=shared_cache)
            yield group
        finally:
            if group is not None:
                group.close()
            # Do not inject consumer exceptions into _snapshot's owner-error
            # translation. A downstream publication failure must retain its type.
            manager.__exit__(None, None, None)

    def get_history(self, symbol: str, adjustment: str = "raw", *, public_id=None, start=None, end=None) -> dict:
        """Read a resolved continuation, with inclusive session bounds.

        The symbol locates the publication group. If reused, an explicit public ID
        is required. Bounds filter values, never guess which identity was intended.
        Numeric CSV values remain exact strings; no float conversion occurs.
        """
        result = self.get_histories(symbol, (adjustment,), public_id=public_id, start=start, end=end)
        return {
            "api_version": result["api_version"],
            "boundary": "standalone_resolved_pricing",
            "generation": result["generation"],
            "symbol": result["symbol"],
            "security": result["security"],
            "adjustment": adjustment,
            "range": result["range"],
            **result["views"][adjustment],
        }

    def get_histories(
        self,
        symbol: str,
        adjustments=ADJUSTMENTS,
        *,
        public_id=None,
        start=None,
        end=None,
        include_evidence=False,
    ) -> dict:
        """Read distinct requested views from one snapshot, in first-requested order.

        adjustments is a nonempty list/tuple of exact view names; commas belong
        to compound names. Physical aliases share decoded rows within the call;
        optional bounded RAM caching also retains them across calls. Selection,
        overlap policy and inclusive bounds match get_history.
        include_evidence adds the published group evidence from this same locked
        snapshot, never an independently selected or reacquired metadata view.
        """
        from . import API_VERSION

        symbol = _symbol(symbol)
        if type(include_evidence) is not bool:
            raise ValueError("include_evidence must be a boolean")
        if not isinstance(adjustments, (tuple, list)) or not adjustments:
            raise ValueError("adjustments must be a nonempty list or tuple")
        if any(adjustment not in ADJUSTMENTS for adjustment in adjustments):
            raise ValueError("unsupported adjustment")
        adjustments = tuple(dict.fromkeys(adjustments))
        public_id = _public_id(public_id) if public_id is not None else None
        start, end = _date(start, "start"), _date(end, "end")
        if start is not None and end is not None and start > end:
            raise ValueError("start must not be after end")
        with self._snapshot(symbol) as (assignments, sources, generation, receipts, generations):
            matches = _matches(assignments, symbol)
            if public_id is None:
                if len(matches) > 1:
                    raise AmbiguousTickerError("ticker has multiple public IDs; use --resolve then --public-id")
                if not matches:
                    raise StandaloneReaderError("no resolved pricing identity for this ticker")
                public_id = matches[0]["public_id"]
            selected = next((item for item in matches if item["public_id"] == public_id), None)
            if selected is None:
                raise StandaloneReaderError("public ID is unavailable for this ticker")
            if any(adjustment not in selected["adjustments"] for adjustment in adjustments):
                raise StandaloneReaderError("adjustment is unavailable for this public ID")
            views = _read_views(
                assignments,
                sources,
                self.store,
                public_id,
                adjustments,
                start,
                end,
                self._cache if self._cache.maximum_bytes else None,
            )
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_resolved_pricing_views",
                "generation": generation,
                "symbol": symbol,
                "security": selected,
                "range": {
                    "start": start.isoformat() if start else None,
                    "end": end.isoformat() if end else None,
                    "inclusive": True,
                },
                "views": views,
                **(
                    {"evidence": _group_evidence(assignments, sources, receipts, generations, generation)}
                    if include_evidence
                    else {}
                ),
            }
