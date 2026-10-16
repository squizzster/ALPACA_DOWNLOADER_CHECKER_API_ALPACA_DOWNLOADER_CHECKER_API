"""Bind an old-label source prefix to an independently resolved successor view."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .errors import SecurityCorrelationError
from .identity_partition import _session_date
from .ownership_interval_rules import TickerOwnershipInterval
from .uuid_lineage import UuidLineageEdge

PREDECESSOR_PREFIX_RULE_NAME = "exact_independently_resolved_predecessor_prefix"
_INDEPENDENT_RULE = "asof_selected_uuid_lineage_interval"
_FIELDS = ("t", "o", "h", "l", "c", "v", "n", "vw")
# Ownership follows the original transaction fingerprint. Later corporate actions
# can legitimately give two request labels different adjusted representations.
_PRICE_COMPARISON_VIEWS = frozenset(("raw",))


@dataclass(frozen=True)
class PredecessorPrefix:
    first_session: date
    interval: TickerOwnershipInterval
    last_session: date
    row_count: int
    donor_symbol: str
    action_id: str

    def contains(self, session: date) -> bool:
        return session >= self.first_session and self.interval.contains(session)

    def as_report_object(self):
        # Transient explanation for the evidence CLI, not another price payload
        # or persistent audit boundary. The ordinary receipt binds source versions.
        return {
            "donor_symbol": self.donor_symbol,
            "corporate_action_id": self.action_id,
            "first_session": self.first_session.isoformat(),
            "last_session": self.last_session.isoformat(),
            "rows": self.row_count,
            "asset_id": self.interval.asset_id,
            "cusip": self.interval.cusip,
            "exact_raw_prices_and_timestamps": True,
            "all_adjustment_session_grids_match": True,
            "independent_donor_rule": _INDEPENDENT_RULE,
        }


def predecessor_prefix_candidates(symbol, lineage, intervals, edges) -> tuple[UuidLineageEdge, ...]:
    # Ordinary uniquely owned labels already resolve without reading other bars.
    occupants = {
        item.get("asset_id")
        for item in lineage.get("symbol_occupancy_evidence", [])
        if item.get("symbol") == symbol and item.get("asset_id")
    } | {item.asset_id for item in intervals if item.historical_symbol == symbol}
    if len(occupants) < 2:
        return ()
    return tuple(
        edge
        for edge in edges
        if edge.edge_type == "name_change"
        and edge.predecessor_symbol == symbol
        and edge.successor_symbol != symbol
        and any(
            interval.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and interval.end_date_exclusive == edge.effective_date
            for interval in intervals
        )
    )


def _independent_prefix_rows(report, descriptor, edge, first_session, source, source_rows_loader):
    filename = descriptor["data_file"]
    rows, generation = source_rows_loader(edge.successor_symbol, filename)
    if generation != descriptor["download_sha256"]:
        return None
    indexed = [
        (position, row)
        for position, row in enumerate(rows, 1)
        if first_session <= _session_date(row.get("t")) < edge.effective_date
    ]
    if not indexed:
        return None
    # An earlier, independent rule must already own every donor row. An alias
    # inferred by this rule cannot in turn become another alias's evidence.
    covered = set()
    positions = {position for position, _ in indexed}
    for segment in report.correlated["segments"]:
        if segment["data_file"] != filename:
            continue
        overlap = positions.intersection(range(segment["row_start"], segment["row_end"] + 1))
        if not overlap:
            continue
        security = segment.get("canonical_security", {})
        if (
            segment["correlation_rule"]["name"] != _INDEPENDENT_RULE
            or segment.get("historical_symbol") != edge.predecessor_symbol
            or security.get("alpaca_asset_id") != edge.predecessor_asset_id
            or security.get("cusip") != edge.predecessor_cusip
            or segment.get("feed") != source["feed"]
            or segment.get("timeframe") != source["timeframe"]
            or segment.get("download_sha256") != generation
            or covered.intersection(overlap)
        ):
            return None
        covered.update(overlap)
    if covered != positions:
        return None
    return [row for _, row in indexed]


def assess_predecessor_prefixes(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
) -> tuple[PredecessorPrefix, ...]:
    if correlation_loader is None or source_rows_loader is None:
        return ()
    candidates = predecessor_prefix_candidates(symbol, lineage, intervals, edges)
    if not candidates:
        return ()
    sources = {}
    for download, source, sessions in work_items:
        adjustment = source["adjustment"]
        if adjustment in sources or not sessions:
            return ()
        sources[adjustment] = (download.rows, source, sessions)
    if set(sources) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    qualified = []
    for edge in candidates:
        matching_intervals = [
            interval
            for interval in intervals
            if interval.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and interval.end_date_exclusive == edge.effective_date
        ]
        if len(matching_intervals) != 1:
            continue
        interval = matching_intervals[0]
        try:
            report = correlation_loader(edge.successor_symbol)
        except SecurityCorrelationError:
            # The first shared pass has no independent decisions yet.
            continue
        descriptors = {}
        for descriptor in report.sources_by_filename.values():
            adjustment = descriptor["adjustment"]
            if adjustment in descriptors:
                descriptors = {}
                break
            descriptors[adjustment] = descriptor
        if set(descriptors) != set(sources):
            continue
        first_session = None
        prefix_sessions = None
        for adjustment, (rows, source, sessions) in sources.items():
            prefix = tuple(session for session in sessions if session < edge.effective_date)
            if len(prefix) < 2 or any(not interval.contains(session) for session in prefix):
                break
            if prefix_sessions is not None and prefix != prefix_sessions:
                break
            first_session, prefix_sessions = prefix[0], prefix
            try:
                tail = expected_market_session_coverage(prefix[-1].isoformat(), edge.effective_date.isoformat())
            except MarketCalendarError:
                break
            if tail.session_dates != (prefix[-1].isoformat(),):
                break
            if adjustment not in _PRICE_COMPARISON_VIEWS:
                continue
            source_prefix = rows[: len(prefix)]
            # A constant/zero-trade carry-forward sequence is not a price identity.
            traded_prices = {
                tuple(row[field] for field in ("o", "h", "l", "c"))
                for row in source_prefix
                if Decimal(str(row["v"])) > 0 and Decimal(str(row["n"])) > 0
            }
            if len(traded_prices) < 2:
                break
            try:
                donor = _independent_prefix_rows(
                    report, descriptors[adjustment], edge, first_session, source, source_rows_loader
                )
            except SecurityCorrelationError:
                break
            if donor is None or [tuple(row[field] for field in _FIELDS) for row in donor] != [
                tuple(row[field] for field in _FIELDS) for row in source_prefix
            ]:
                break
        else:
            qualified.append(
                PredecessorPrefix(
                    first_session,
                    interval,
                    prefix_sessions[-1],
                    len(prefix_sessions),
                    edge.successor_symbol,
                    edge.action_id,
                )
            )
    # Competing donors must not be resolved by iteration order or longest match.
    if len({item.interval.identity_key() for item in qualified}) != 1:
        return ()
    return tuple(dict.fromkeys(qualified))
