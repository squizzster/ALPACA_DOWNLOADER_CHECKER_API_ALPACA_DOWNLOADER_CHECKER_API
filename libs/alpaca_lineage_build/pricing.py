"""In-memory adapter to the established rules. No legacy runtime paths are read."""

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_end_of_day_security_correlation.session_exclusions import (
    VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
    VERIFIED_SESSION_EXCLUSIONS_VERSION,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS, HISTORY_START
from libs.alpaca_pricing_download.handoff import load_published_boundary, load_published_views
from libs.alpaca_pricing_download.storage import load_source, regular_path
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .contract import LineageError, document_digest


@dataclass
class PricingInput:
    symbol: str
    source: dict
    source_sha256: str
    views: dict
    downloads: dict
    local: dict

    def reader(self, _repository_root, _directory, descriptor):
        return self.downloads[descriptor["file"]]


def load_pricing(
    store: Path, symbol: str, *, source=None, active_only=False, previous_boundaries=None
) -> PricingInput:
    """Caller owns the ticker EX lock, including every later read and acknowledgement."""
    selected = source is not None
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    source = load_source(root, symbol) if source is None else source
    if source is None:
        raise LineageError(f"no published pricing for {symbol}")
    views = (
        load_published_boundary(root, source, "current_month", selected=selected)
        if active_only
        else load_published_views(root, source, selected=selected)
    )
    generation = document_digest(source)
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
                "sidecar_present": True,
                "sidecar_readable": True,
                "gzip_present": bool(values),
                "stat_matches_sidecar": True,
                "pipeline_receipt_matches": False,
                "integrity_failure": None,
            }
        )
        if active_only:
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
            descriptors[-1]["gzip_present"] = bool(descriptors[-1]["rows"])
            if earlier:
                descriptors[-1]["from"] = earlier[0]["first_timestamp"]
                if not values:
                    descriptors[-1]["to"] = earlier[-1]["last_timestamp"]
    local = {
        "directory": f"end_of_day/pricing/{symbol}",
        "present": True,
        "download_count": len(descriptors),
        "download_sha256_authentication_performed": False,
        "published_source_trusted": True,
        "downloads": descriptors,
        "manifest_errors": [],
    }
    return PricingInput(symbol, source, generation, views, downloads, local)


def empty_exclusions(_root, symbol):
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
