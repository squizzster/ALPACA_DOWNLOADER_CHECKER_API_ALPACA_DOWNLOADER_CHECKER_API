"""Read the standalone twins' published assignments and prices, without a catalogue.

This is a resolved-data interface, not DataLibrary's online-availability contract.
The source symbol scopes a published dependency group; it never becomes an ID.
"""

import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
from pathlib import Path, PurePosixPath

from libs.alpaca_end_of_day_reader.history import _resolve_bar_candidates
from libs.alpaca_lineage_build.contract import LineageBusyError, document_digest, reusable_versions
from libs.alpaca_lineage_build.storage import reader_snapshot, select_inputs
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    COLUMNS,
    NEW_YORK,
    SUPPORTED_VERSIONS,
    decode_csv,
    symbol_label,
    timestamp,
)
from libs.alpaca_pricing_download.storage import load_source, read_document, read_update, regular_path


class StandaloneReaderError(RuntimeError):
    """A completed, consistent standalone publication cannot be read."""


class ReaderBusyError(StandaloneReaderError):
    """A producer owns a required lock; retry later or explicitly wait."""


class AmbiguousTickerError(StandaloneReaderError):
    """The ticker has multiple published identities; select a public ID."""


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


def _assignments(boundaries, receipt):
    for kind in ("completed_years", "current_year", "current_month"):
        for entry in boundaries[kind]["adjustments"]:
            for segment in entry["segments"]:
                if kind == "current_year" and segment["from"][:7] >= receipt["anchor_month"]:
                    continue
                if segment["branch"] == "correlated":
                    yield entry["adjustment"], kind, segment


def _identity(segment):
    return segment["security_identity"]["security_id"]


def _matches(assignments, symbol):
    selected = {
        _identity(segment)
        for source_symbol, items in assignments.items()
        for _, _, segment in items
        if source_symbol == symbol or segment.get("historical_symbol") == symbol
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


def _rows(root, source, kind, adjustment):
    # Only the selected physical payload is opened. Alias equality and source
    # validation belong to the downloader, not to this consumer.
    relative = source["boundaries"][kind]["manifest"]
    manifest = read_document(regular_path(root, relative))
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
    return decode_csv(regular_path(root, relative).read_bytes())


def _history(assignments, sources, store, public_id, adjustment):
    candidates, coverage = defaultdict(list), defaultdict(set)
    for symbol, items in assignments.items():
        by_boundary = defaultdict(list)
        for view, kind, segment in items:
            if view == adjustment and _identity(segment) == public_id:
                by_boundary[kind].append(segment)
        for kind, segments in by_boundary.items():
            root = regular_path(store, f"end_of_day/pricing/{symbol}")
            rows = _rows(root, sources[symbol], kind, adjustment)
            for segment in segments:
                start, end = segment["row_start"], segment["row_end"]
                if not 1 <= start <= end <= len(rows):
                    raise StandaloneReaderError("lineage row selection exceeds its pricing boundary")
                for row in rows[start - 1 : end]:
                    coverage[symbol].add(row[0])
                    candidates[row[0]].append(
                        {
                            "values": dict(zip(COLUMNS, row)),
                            "provenance": {
                                "source_id": symbol,
                                "symbol": symbol,
                                "historical_symbol": segment["historical_symbol"],
                            },
                            "integrity_error_ids": [],
                        }
                    )
    # Reuse the established history-source policy, not a new identity rule:
    # adjusted overlaps prefer strictly broader correlated coverage, then the
    # historical ticker; raw overlaps use the historical ticker. Unsettled
    # conflicts fail, and exact aliases collapse to a single observation.
    bars, _ = _resolve_bar_candidates(
        candidates,
        {symbol: frozenset(values) for symbol, values in coverage.items()},
        allow_strict_superset_selection=adjustment != "raw",
    )
    result, sessions = [], set()
    for bar in bars:
        session = timestamp(bar["timestamp"]).astimezone(NEW_YORK).date().isoformat()
        if session in sessions:
            raise StandaloneReaderError("multiple observations for one daily session")
        sessions.add(session)
        result.append(
            {
                "session": session,
                "tickers": sorted({p["historical_symbol"] for p in bar["provenance"]}),
                **{column: bar[column] for column in COLUMNS},
            }
        )
    return result


class StandaloneReader:
    """Credential-free resolved pricing from an explicit standalone store.

    Each method pins its selected group with sorted pricing shared locks.
    A brief publication handshake leaves unrelated groups free to publish.
    No persistent index, cache, catalogue, download or acknowledgement is created.
    Results are snapshots of one completed group, not promises of online status.
    """

    def __init__(self, store: Path | str, *, wait: bool = False):
        from . import _validate_api_change_log

        _validate_api_change_log()
        if type(wait) is not bool:
            raise ValueError("wait must be a boolean")
        self.store = Path(store).absolute()
        self.wait = wait

    @contextmanager
    def _snapshot(self, symbol):
        try:
            with reader_snapshot(self.store, lambda root: select_inputs(root, symbol), wait=self.wait) as snap:
                seed = snap["receipts"][symbol]
                expected = seed["input_sources"]
                sources, assignments, generations = {}, {}, {}
                for name in sorted(expected):
                    pricing = regular_path(self.store, f"end_of_day/pricing/{name}")
                    if name not in snap["locked"] or regular_path(pricing, ".pending.json").exists():
                        raise StandaloneReaderError("pricing publication incomplete; resume downloader first")
                    source = load_source(pricing, name)
                    if (
                        source is None
                        or read_update(pricing) is not None
                        or document_digest(source) != expected[name]
                    ):
                        raise StandaloneReaderError("lineage awaits changed pricing; run lineage first")
                    receipt = snap["receipts"][name]
                    if receipt is None or not reusable_versions(receipt["versions"]):
                        raise StandaloneReaderError("lineage is missing or uses older rules; run lineage first")
                    if (
                        receipt["input_sources"] != expected
                        or receipt["source_sha256"] != expected[name]
                        or receipt["pricing_boundaries"] != source["boundaries"]
                        or receipt["anchor_month"] != source["anchor_month"]
                    ):
                        raise StandaloneReaderError("lineage group selects different input generations")
                    sources[name] = source
                    assignments[name] = tuple(_assignments(snap["boundaries"][name], receipt))
                    generations[name] = snap["entries"][name]["sha256"]
                yield assignments, sources, document_digest(generations)
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
        with self._snapshot(symbol) as (assignments, _, generation):
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_ticker_resolution",
                "generation": generation,
                "symbol": symbol,
                "matches": _matches(assignments, symbol),
            }

    def get_history(self, symbol: str, adjustment: str = "raw", *, public_id=None, start=None, end=None) -> dict:
        """Read a resolved continuation, with inclusive session bounds.

        The symbol locates the publication group. If reused, an explicit public ID
        is required. Bounds filter values, never guess which identity was intended.
        Numeric CSV values remain exact strings; no float conversion occurs.
        """
        from . import API_VERSION

        symbol = _symbol(symbol)
        if adjustment not in ADJUSTMENTS:
            raise ValueError("unsupported adjustment")
        public_id = _public_id(public_id) if public_id is not None else None
        start, end = _date(start, "start"), _date(end, "end")
        if start is not None and end is not None and start > end:
            raise ValueError("start must not be after end")
        with self._snapshot(symbol) as (assignments, sources, generation):
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
            if adjustment not in selected["adjustments"]:
                raise StandaloneReaderError("adjustment is unavailable for this public ID")
            bars = _history(assignments, sources, self.store, public_id, adjustment)
            # Apply bounds after source selection so changing the query window
            # cannot change which adjusted series wins an overlapping session.
            bars = [
                bar
                for bar in bars
                if (start is None or bar["session"] >= start.isoformat())
                and (end is None or bar["session"] <= end.isoformat())
            ]
            return {
                "api_version": API_VERSION,
                "boundary": "standalone_resolved_pricing",
                "generation": generation,
                "symbol": symbol,
                "security": selected,
                "adjustment": adjustment,
                "range": {
                    "start": start.isoformat() if start else None,
                    "end": end.isoformat() if end else None,
                    "inclusive": True,
                },
                "bar_count": len(bars),
                "bars": bars,
            }
