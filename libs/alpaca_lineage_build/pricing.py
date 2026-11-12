"""In-memory adapter to the established rules. No legacy runtime paths are read."""

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_end_of_day_security_correlation.settings import (
    VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
    VERIFIED_SESSION_EXCLUSIONS_VERSION,
)
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    FIELDS,
    HISTORY_START,
    NEW_YORK,
    month_of,
    timestamp,
)
from libs.alpaca_pricing_download.evidence import PublishedPricingEvidence
from libs.alpaca_pricing_download.handoff import load_published_boundary, load_published_views
from libs.alpaca_pricing_download.storage import load_source, regular_path, trace_source_selection
from libs.common.debug_trace import event, traced
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .contract import LineageError, document_digest
from .correction_prefix import correction_from, prefix_assignments
from .resume import assignment_summary, unchanged_rows


@dataclass
class PricingInput:
    symbol: str
    source: dict
    source_sha256: str
    views: dict
    downloads: dict
    local: dict
    materialization_scope: str
    previous_assignment_summary: dict | None = None
    correction_from: str | None = None
    prefix_boundaries: dict | None = None
    previous_coverage_end: str | None = None
    accepted_identity_memberships: dict | None = None
    accepted_report: dict | None = None
    reprojected_sessions: dict | None = None
    accepted_current_source_ownership: dict | None = None
    accepted_sourced_rename_ownership: dict | None = None
    accepted_sourced_rename_membership: dict | None = None
    accepted_documented_membership: dict | None = None

    def reader(self, _repository_root, _directory, descriptor):
        # Ignored paths satisfy the injected interface; they must not trigger filesystem access.
        return self.downloads[descriptor["file"]]


@traced("lineage_payload")
def load_pricing(
    store: Path,
    symbol: str,
    *,
    source=None,
    active_only=False,
    previous_boundaries=None,
    resume_from=None,
    previous=None,
    partitions=None,
    source_sha256=None,
    selected_correction=...,
    selected_change_from=None,
    projection_from=None,
) -> PricingInput:
    """Caller owns the ticker EX lock, including every later read and acknowledgement."""
    # TRUST BOUNDARY: pricing already validated the selected source and payload
    # map. Read the data needed for this correlation scope; DO NOT authenticate
    # gzip hashes or enumerate unselected boundaries before accepting its facts.
    selected = source is not None
    materialization_scope = (
        "retained_suffix"
        if resume_from is not None
        else "active_partition"
        if active_only
        else "complete_history"
    )
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    source = load_source(root, symbol) if source is None else source
    if source is None:
        raise LineageError(f"no published pricing for {symbol}")
    generation = document_digest(source) if source_sha256 is None else source_sha256
    trace_source_selection(source, generation, symbol=symbol, reason="lineage_input")
    # The operation retains immutable decoded partitions across lock reacquisition.
    # The newly selected source supplies their exact generation. Envelope-only
    # changes do not change a selected partition; changed partitions get new keys.
    partitions = {} if partitions is None else partitions

    def read(root, source, kind, *, selected=True):
        from .recovery_handoffs import remember_partition

        key = (str(root), kind, source["boundaries"][kind]["sha256"])
        if key not in partitions:
            partitions[key] = load_published_boundary(root, source, kind, selected=selected)
        else:
            event(
                "lineage_partition_reuse",
                symbol=symbol,
                boundary=kind,
                boundary_sha256=key[-1],
                source_sha256=generation,
            )
        remember_partition(store, symbol, kind, key[-1], partitions[key])
        return dict(partitions[key])

    corrected = projection_from or (
        (correction_from(source, previous) if selected_correction is ... else selected_correction)
        if resume_from is not None
        else None
    )
    prefix_ends = {}
    # Pricing already published the actual first changed session. Carry that
    # accepted scope into append assembly: a weekend before month/year rollover
    # cannot require reopening a closed partition with no newly produced rows.
    # Corrections/projections retain their independently selected owning scope.
    assembly_from = (
        max(resume_from, selected_change_from)
        if resume_from is not None and selected_change_from and not corrected
        else resume_from
    )
    views = (
        {adjustment: () for adjustment in ADJUSTMENTS}
        if resume_from is not None
        and projection_from is None
        and unchanged_rows(source, previous, selected=generation)
        else _added_views(root, source, assembly_from, read=read, prefix_ends=prefix_ends if corrected else None)
        if resume_from is not None
        else (
            read(root, source, "current_month", selected=selected)
            if active_only
            else load_published_views(root, source, selected=selected, read=read)
        )
    )
    request = source["latest_request"]
    # The downloader's latest accepted asof selects the effective view. Original
    # per-boundary acquisition provenance remains in its selected source receipt.
    context = {
        "request_from": source.get("history_start", HISTORY_START),
        "request_to": request["end_exclusive"],
        "asof": request["asof"],
        "feed": request["feed"],
        "timeframe": request["timeframe"],
    }
    downloads, descriptors = {}, []
    converted_views = {}
    prior_summary = assignment_summary(previous, previous_boundaries) if resume_from is not None else None
    clipped = None
    removed = None
    if corrected:
        removed = {
            name: tuple(
                day
                for row in values
                if (day := timestamp(row[0]).astimezone(NEW_YORK).date().isoformat())
                <= previous["assignment_summary"][name]["last_session"]
            )
            for name, values in views.items()
        }
        prior_summary, clipped = prefix_assignments(
            previous, previous_boundaries, corrected, prefix_ends, removed_sessions=removed
        )
    for adjustment in ADJUSTMENTS:
        values = views[adjustment]
        rows = converted_views.get(id(values))
        if rows is None:
            rows = [dict(zip(FIELDS, row)) for row in values]
            converted_views[id(values)] = rows
        filename = adjustment.replace(",", "_") + ".csv.gz"
        manifest = {
            "symbol": symbol,
            "adjustment": adjustment,
            # Legacy rules need an input identity, not a second digest of bars.
            "sha256": document_digest({"source": generation, "adjustment": adjustment}),
            "rows": len(values),
            **context,
        }
        downloads[filename] = SimpleNamespace(
            path=Path(filename),  # A logical name, never a file to materialize.
            manifest=manifest,
            rows=rows,
            pipeline_state="source_published",
            pipeline_diagnostic=None,
            integrity_failure=None,
        )
        descriptors.append(
            {
                "file": filename,
                **manifest,
                "sessions": len(values),
                "from": values[0][0] if values else None,
                "to": values[-1][0] if values else None,
                "integrity_failure": None,
            }
        )
        if resume_from is not None:
            prior = prior_summary[adjustment]
            descriptors[-1]["rows"] += prior["rows"]
            descriptors[-1]["sessions"] = descriptors[-1]["rows"]
            descriptors[-1]["from"] = prior["first_timestamp"] or descriptors[-1]["from"]
            descriptors[-1]["to"] = descriptors[-1]["to"] or prior["last_timestamp"]
        elif active_only:
            earlier = [
                entry
                for kind in ("completed_years", "current_year")
                for item in previous_boundaries[kind]["adjustments"]
                if item["adjustment"] == adjustment
                for entry in item["segments"]
                if entry["from"][:7] < source["anchor_month"]
            ]
            descriptors[-1]["rows"] += sum(item["row_count"] for item in earlier)
            descriptors[-1]["sessions"] = descriptors[-1]["rows"]
            if earlier:
                descriptors[-1]["from"] = earlier[0]["first_timestamp"]
                if not values:
                    descriptors[-1]["to"] = earlier[-1]["last_timestamp"]
    # This handoff includes retained/empty view counts even when only a suffix
    # was decoded. Disposition consumers must use it instead of reopening files
    # to infer completeness from the amount of data loaded for this operation.
    local = {
        "directory": f"end_of_day/pricing/{symbol}",
        "present": True,
        "download_count": len(descriptors),
        "download_sha256_authentication_performed": False,
        "published_source_trusted": True,
        "published_pricing": PublishedPricingEvidence(
            symbol=symbol,
            generation=generation,
            **context,
            row_counts=tuple((item["adjustment"], item["rows"]) for item in descriptors),
        ).to_report(),
        "downloads": descriptors,
        "manifest_errors": [],
    }
    return PricingInput(
        symbol=symbol,
        source=source,
        source_sha256=generation,
        views=views,
        downloads=downloads,
        local=local,
        materialization_scope=materialization_scope,
        previous_assignment_summary=prior_summary,
        correction_from=corrected,
        prefix_boundaries=clipped,
        previous_coverage_end=previous["processed_through"] if resume_from is not None and previous else None,
        reprojected_sessions=removed,
    )


def _added_views(root, source, start, *, read=load_published_boundary, prefix_ends=None):
    """Read only pricing partitions intersecting the producer-approved added range."""
    anchor = source["anchor_month"]
    kinds = ["current_month"]
    if start[:7] < anchor:
        kinds.insert(0, "current_year")
    if start[:4] < anchor[:4]:
        kinds.insert(0, "completed_years")
    parts = {kind: read(root, source, kind, selected=True) for kind in kinds}
    assembled, dates, views = {}, {}, {}
    for adjustment in ADJUSTMENTS:
        key = tuple(id(parts[kind][adjustment]) for kind in kinds)
        if key not in assembled:
            rows = []
            for kind in kinds:
                for row in parts[kind][adjustment]:
                    if kind == "current_year" and month_of(row) >= anchor:
                        continue
                    if row[0] not in dates:
                        dates[row[0]] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                    if dates[row[0]] >= start:
                        rows.append(row)
                    elif prefix_ends is not None:
                        prefix_ends[adjustment] = row[0]
            assembled[key] = tuple(rows)
        views[adjustment] = assembled[key]
        if prefix_ends is not None and adjustment not in prefix_ends:
            # Aliases shared the preceding assembly; lend its accepted seam.
            for prior in views:
                if views[prior] is views[adjustment] and prior in prefix_ends:
                    prefix_ends[adjustment] = prefix_ends[prior]
                    break
    return views


def empty_exclusions(_root, symbol):
    # Keep the injected interface and representation without consulting filesystem sidecars.
    return {
        "manifest_type": VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
        "manifest_version": VERIFIED_SESSION_EXCLUSIONS_VERSION,
        "symbol": symbol,
        "input_file": None,
        "input_sha256": None,
        "calendar_sha256": us_market_hours_sha256(),
        "events": [],
    }


def rule_loaders(inputs: dict, preliminary: dict, cache: CorrelationCache):
    def sessions(symbol):
        view = inputs.get(symbol)
        if view is None:
            return None
        grids = [cache.session_dates(item.rows) for item in view.downloads.values()]
        if any(grid != grids[0] for grid in grids[1:]):
            return None
        return grids[0], tuple(
            (symbol, item.manifest["adjustment"], item.manifest["sha256"]) for item in view.downloads.values()
        )

    def correlation(symbol):
        if symbol in getattr(cache, "closed_sources", {}):
            report = cache.closed_report_loader(symbol)
            return SimpleNamespace(
                correlated=report["correlated"],
                sources_by_filename={item["data_file"]: item for item in report["source_downloads"]},
            )
        if symbol not in preliminary:
            raise SecurityCorrelationError(f"no current correlation for {symbol}")
        report = preliminary[symbol]
        return SimpleNamespace(
            correlated=report["correlated"],
            sources_by_filename={item["data_file"]: item for item in report["source_downloads"]},
        )

    def rows(symbol, filename):
        if symbol not in inputs or filename not in inputs[symbol].downloads:
            raise SecurityCorrelationError(f"no pricing input for {symbol}/{filename}")
        item = inputs[symbol].downloads[filename]
        return item.rows, item.manifest["sha256"]

    return sessions, correlation, rows


@dataclass
class MetadataSourceControl:
    """Selected pricing control without a price reader or materialized rows."""

    store: str
    symbol: str
    source: dict
    source_sha256: str
    local: dict
    previous: dict | None
    materialized: bool = False
    kind: str = "metadata_source_control"
    previous_receipt_generation: str | None = None
    accepted_reuse: tuple | None = None


@dataclass
class ClosedAcceptedSource(MetadataSourceControl):
    """A real nonempty accepted source, closed to historical price work."""

    kind: str = "closed_accepted_source"
    accepted_report: dict | None = None
    accepted_identity_memberships: dict | None = None
    accepted_public_common_membership: dict | None = None
    accepted_security_relations: dict | None = None
    accepted_sourced_rename_membership: dict | None = None
    accepted_documented_membership: dict | None = None
    accepted_common_bounds: dict | None = None


def metadata_source_control(store, symbol, source, generation, previous, *, accepted_reuse=None):
    """Use accepted counts only where their source equality binding applies.

    A new/changed source explicitly has unknown counts until its selected data
    producer supplies them. It is not represented as an accepted empty source.
    Metadata acquisition needs only this narrow symbol/local/source interface.
    """
    summary = (
        previous.get("assignment_summary")
        if previous
        and (
            accepted_reuse[0]
            if accepted_reuse is not None
            else unchanged_rows(source, previous, selected=generation)
        )
        else None
    )
    request = source["latest_request"]
    context = {
        "request_from": source.get("history_start", HISTORY_START),
        "request_to": request["end_exclusive"],
        "asof": request["asof"],
        "feed": request["feed"],
        "timeframe": request["timeframe"],
    }
    descriptors = []
    for adjustment in ADJUSTMENTS:
        count = summary[adjustment]["rows"] if summary else None
        descriptors.append(
            {
                "file": adjustment.replace(",", "_") + ".csv.gz",
                "symbol": symbol,
                "adjustment": adjustment,
                **context,
                "rows": count,
                "sessions": count,
                "from": summary[adjustment]["first_timestamp"] if summary else None,
                "to": summary[adjustment]["last_timestamp"] if summary else None,
                "integrity_failure": None,
            }
        )
    local = {
        "directory": f"end_of_day/pricing/{symbol}",
        "present": True,
        "download_count": len(ADJUSTMENTS),
        "counts_known": summary is not None,
        "downloads": descriptors,
        "manifest_errors": [],
        "published_source_trusted": True,
        "download_sha256_authentication_performed": False,
    }
    if summary:
        local["published_pricing"] = PublishedPricingEvidence(
            symbol=symbol,
            generation=generation,
            **context,
            row_counts=tuple((name, summary[name]["rows"]) for name in ADJUSTMENTS),
        ).to_report()
    return MetadataSourceControl(
        str(store), symbol, source, generation, local, previous, accepted_reuse=accepted_reuse
    )
