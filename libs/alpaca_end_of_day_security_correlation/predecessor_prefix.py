"""Bind an old-label source prefix to an independently resolved successor view."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES,
)
from libs.alpaca_pricing_download.contract import (
    FIELDS,
)
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .errors import SecurityCorrelationError
from .identity_partition import _session_date
from .ownership_interval_rules import TickerOwnershipInterval
from .terminal_episode import (
    _inert,
    _positive_competing_owner,
    has_informative_trading,
    has_prolonged_carry_forward,
    shared_inert_bridges,
)
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES, UuidLineageEdge

PREDECESSOR_PREFIX_RULE_NAME = "exact_independently_resolved_predecessor_prefix"
MAPPED_CONTINUATION_RULE_NAME = "accepted_name_change_mapped_continuation"
MAPPED_CONTINUATION_WARNING = "mapped_predecessor_duplicate"
_INDEPENDENT_RULE = "asof_selected_uuid_lineage_interval"
_FIELDS = ("t", "o", "h", "l", "c", "v", "n", "vw")
# Ownership follows the original transaction fingerprint. Later corporate actions
# can legitimately give two request labels different adjusted representations.
_PRICE_COMPARISON_VIEWS = frozenset(("raw",))
INDEPENDENT_HISTORY_WARNING = "independently_dated_matching_history"


@dataclass(frozen=True)
class IndependentHistoricalInterval:
    first: date
    last: date
    interval: TickerOwnershipInterval


@dataclass(frozen=True)
class MappedContinuation:
    sessions: frozenset[date]
    interval: TickerOwnershipInterval
    donor_symbol: str
    action_id: str

    def contains(self, session: date) -> bool:
        return session in self.sessions


def _known_label_asset_ids(lineage, symbol):
    known = {
        item.get("id")
        for item in lineage.get("identities", [])
        if item.get("symbol") == symbol and item.get("id")
    }
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        known.update(
            item.get("asset_id")
            for item in lineage.get(field, [])
            if item.get("symbol") == symbol and item.get("asset_id")
        )
    known.update(
        item["asset"].get("id")
        for item in lineage.get("asset_lookups", [])
        if isinstance(item.get("asset"), dict)
        and item["asset"].get("id")
        and item.get("lookup_kind") == "symbol"
        and item.get("lookup") == symbol
    )
    return known


def _mapped_donor_positions(report, descriptor, edge, source):
    positions = set()
    for segment in report.correlated["segments"]:
        if segment.get("data_file") != descriptor["data_file"]:
            continue
        interval = segment.get("ticker_ownership_interval") or {}
        security = segment.get("canonical_security") or {}
        if (
            segment.get("historical_symbol") != edge.successor_symbol
            or security.get("alpaca_asset_id") != edge.successor_asset_id
            or security.get("cusip") != edge.successor_cusip
            or interval.get("historical_symbol") != edge.successor_symbol
            or interval.get("asset_id") != edge.successor_asset_id
            or interval.get("cusip") != edge.successor_cusip
            or interval.get("start_date_inclusive") != edge.effective_date.isoformat()
            or edge.action_id not in interval.get("supporting_corporate_action_ids", ())
            or segment.get("correlation_method") == MAPPED_CONTINUATION_RULE_NAME
            or segment.get("feed") != source["feed"]
            or segment.get("timeframe") != source["timeframe"]
            or segment.get("download_sha256") != descriptor["download_sha256"]
        ):
            continue
        covered = set(range(segment["row_start"], segment["row_end"] + 1))
        if positions.intersection(covered):
            return set()
        positions.update(covered)
    return positions


def assess_mapped_continuations(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
):
    """Bind mapped predecessor rows only after the successor independently owns them."""
    if correlation_loader is None or source_rows_loader is None:
        return ()
    sources = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(sources) != len(work_items) or set(sources) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    grids = {sessions for _, _, sessions in sources.values()}
    if len(grids) != 1:
        return ()
    qualified = []
    for edge in edges:
        if (
            edge.edge_type != "name_change"
            or edge.predecessor_symbol != symbol
            or edge.successor_symbol == symbol
            or edge.predecessor_asset_id == edge.successor_asset_id
            or _known_label_asset_ids(lineage, symbol) != {edge.predecessor_asset_id}
        ):
            continue
        predecessors = [
            interval
            for interval in intervals
            if interval.identity_key()
            == (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and interval.end_date_exclusive == edge.effective_date
        ]
        successors = [
            interval
            for interval in intervals
            if interval.identity_key() == (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
            and interval.start_date_inclusive == edge.effective_date
        ]
        if len(predecessors) != 1 or len(successors) != 1:
            continue
        try:
            asof = date.fromisoformat(sources["raw"][1]["asof"][:10])
        except (KeyError, TypeError, ValueError):
            continue
        if not predecessors[0].contains(asof):
            continue
        outgoing = [
            candidate
            for candidate in edges
            if candidate.edge_type == "name_change"
            and (
                candidate.predecessor_symbol,
                candidate.predecessor_cusip,
                candidate.predecessor_asset_id,
            )
            == (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        ]
        incoming = [
            candidate
            for candidate in edges
            if candidate.edge_type == "name_change"
            and (
                candidate.successor_symbol,
                candidate.successor_cusip,
                candidate.successor_asset_id,
            )
            == (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        ]
        if outgoing != [edge] or incoming != [edge]:
            continue
        try:
            report = correlation_loader(edge.successor_symbol)
        except SecurityCorrelationError:
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
        successor = successors[0]
        sessions = next(iter(grids))
        matching = {session for session in sessions if successor.contains(session)}
        for adjustment, (rows, source, _) in sources.items():
            descriptor = descriptors[adjustment]
            try:
                donor_rows, generation = source_rows_loader(edge.successor_symbol, descriptor["data_file"])
            except SecurityCorrelationError:
                matching.clear()
                break
            if generation != descriptor["download_sha256"]:
                matching.clear()
                break
            donor = {_session_date(row.get("t")): (position, row) for position, row in enumerate(donor_rows, 1)}
            if len(donor) != len(donor_rows):
                matching.clear()
                break
            owned = _mapped_donor_positions(report, descriptor, edge, source)
            source_by_session = dict(zip(sessions, rows, strict=True))
            matching = {
                session
                for session in matching
                if session in donor
                and donor[session][0] in owned
                and all(
                    source_by_session[session].get(field) is not None
                    and source_by_session[session].get(field) == donor[session][1].get(field)
                    for field in FIELDS
                )
            }
            if not matching:
                break
        if matching:
            qualified.append(
                MappedContinuation(frozenset(matching), successor, edge.successor_symbol, edge.action_id)
            )
    return tuple(dict.fromkeys(qualified))


def _same_label_split_chain(lineage, edges, handoff):
    """Reject extra label occupants connected only through a later successor."""
    symbol = handoff.predecessor_symbol
    known = {
        item.get("asset_id")
        for item in lineage.get("symbol_occupancy_evidence", [])
        if item.get("symbol") == symbol and item.get("asset_id")
    } | {
        item.get("id")
        for item in lineage.get("identities", [])
        if item.get("symbol") == symbol and item.get("id")
    }
    known.add(handoff.predecessor_asset_id)
    current = handoff.predecessor_asset_id
    visited = {current}
    before = handoff.effective_date
    while True:
        # Certify the history before this handoff. A later split may join an ID
        # backfilled onto the old label, without contradicting its dated donor.
        # Extra known occupants still need the complete earlier same-label chain.
        incoming = {
            (edge.predecessor_asset_id, edge.effective_date)
            for edge in edges
            if edge.successor_asset_id == current
            and edge.predecessor_asset_id != current
            and edge.effective_date < before
        }
        if not incoming:
            break
        if len(incoming) != 1:
            return frozenset()
        predecessor, when = next(iter(incoming))
        connecting = [
            edge
            for edge in edges
            if edge.predecessor_asset_id == predecessor and edge.successor_asset_id == current
        ]
        if (
            predecessor in visited
            or any(
                edge.edge_type not in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
                or edge.predecessor_symbol != symbol
                or edge.successor_symbol != symbol
                for edge in connecting
            )
            or any(
                edge.successor_asset_id != current for edge in edges if edge.predecessor_asset_id == predecessor
            )
        ):
            return frozenset()
        visited.add(predecessor)
        current, before = predecessor, when
    return frozenset(visited) if known == visited else frozenset()


def _independent_donor_segment(segment, edge, split_chain):
    return (
        segment["correlation_rule"]["name"] == "requested_symbol_dated_uuid_interval"
        and INDEPENDENT_HISTORY_WARNING not in segment.get("warnings", [])
        and segment.get("historical_symbol") == edge.predecessor_symbol
        and segment.get("canonical_security", {}).get("alpaca_asset_id") == edge.predecessor_asset_id
        and segment.get("canonical_security", {}).get("cusip") == edge.predecessor_cusip
        and (
            "dated_action_occupancy" in segment.get("warnings", [])
            or bool(split_chain)
            and split_chain.issubset(segment.get("security_identity", {}).get("member_alpaca_asset_ids", []))
        )
    )


def assess_independent_historical_intervals(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
):
    if correlation_loader is None or source_rows_loader is None:
        return ()
    sources = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(sources) != len(work_items) or set(sources) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    qualified = []
    for edge in edges:
        if (
            edge.edge_type != "name_change"
            or edge.successor_symbol != symbol
            or edge.predecessor_symbol == symbol
        ):
            continue
        owners = [
            item
            for item in intervals
            if item.identity_key()
            == (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and item.end_date_exclusive == edge.effective_date
        ]
        if len(owners) != 1:
            continue
        try:
            report = correlation_loader(edge.predecessor_symbol)
        except SecurityCorrelationError:
            continue
        descriptors = {item["adjustment"]: item for item in report.sources_by_filename.values()}
        if len(descriptors) != len(report.sources_by_filename) or set(descriptors) != set(sources):
            continue
        split_chain = _same_label_split_chain(lineage, edges, edge)
        anchors = [
            segment
            for segment in report.correlated["segments"]
            if segment["adjustment"] == "raw" and _independent_donor_segment(segment, edge, split_chain)
        ]
        for anchor in anchors:
            first = date.fromisoformat(anchor["first_session_date"])
            last = date.fromisoformat(anchor["last_session_date"])
            if not owners[0].contains(first) or not owners[0].contains(last):
                continue
            tail = expected_market_session_coverage(last.isoformat(), edge.effective_date.isoformat())
            if tail.session_dates != (last.isoformat(),):
                continue
            agreed_grid = None
            for adjustment, (rows, source, sessions) in sources.items():
                source_selected = [
                    (row, session) for row, session in zip(rows, sessions) if first <= session <= last
                ]
                grid = tuple(session for _, session in source_selected)
                expected = expected_market_session_coverage(first.isoformat(), edge.effective_date.isoformat())
                if tuple(session.isoformat() for session in grid) != expected.session_dates or (
                    agreed_grid is not None and grid != agreed_grid
                ):
                    break
                agreed_grid = grid
                descriptor = descriptors[adjustment]
                try:
                    donor, generation = source_rows_loader(edge.predecessor_symbol, descriptor["data_file"])
                except SecurityCorrelationError:
                    break
                if generation != descriptor["download_sha256"]:
                    break
                donor_selected = [
                    (position, row)
                    for position, row in enumerate(donor, 1)
                    if first <= _session_date(row["t"]) <= last
                ]
                if tuple(_session_date(row["t"]) for _, row in donor_selected) != grid:
                    break
                positions = {position for position, _ in donor_selected}
                covered = set()
                for segment in report.correlated["segments"]:
                    if segment["data_file"] != descriptor["data_file"]:
                        continue
                    overlap = positions.intersection(range(segment["row_start"], segment["row_end"] + 1))
                    if not overlap:
                        continue
                    security = segment.get("canonical_security", {})
                    if (
                        not _independent_donor_segment(segment, edge, split_chain)
                        or segment.get("historical_symbol") != edge.predecessor_symbol
                        or (security.get("alpaca_asset_id"), security.get("cusip"))
                        != (edge.predecessor_asset_id, edge.predecessor_cusip)
                        or segment.get("feed") != source["feed"]
                        or segment.get("timeframe") != source["timeframe"]
                        or segment.get("download_sha256") != generation
                        or covered.intersection(overlap)
                    ):
                        break
                    covered.update(overlap)
                else:
                    if covered != positions or len(donor_selected) != len(source_selected):
                        break
                    if adjustment == "raw":
                        raw = [row for row, _ in source_selected]
                        if (
                            not has_informative_trading(raw)
                            or has_prolonged_carry_forward(raw)
                            or [tuple(row[key] for key in _FIELDS) for _, row in donor_selected]
                            != [tuple(row[key] for key in _FIELDS) for row in raw]
                        ):
                            break
                    continue
                break
            else:
                qualified.append(IndependentHistoricalInterval(first, last, owners[0]))
    return tuple(dict.fromkeys(qualified))


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
    occupants = (
        {
            item.get("asset_id")
            for item in lineage.get("symbol_occupancy_evidence", [])
            if item.get("symbol") == symbol and item.get("asset_id")
        }
        | {item.asset_id for item in intervals if item.historical_symbol == symbol}
        | {
            item.get("id")
            for item in lineage.get("identities", [])
            if item.get("symbol") == symbol and item.get("id")
        }
        | {
            item["asset"].get("id")
            for item in lineage.get("asset_lookups", [])
            if item.get("found") is True
            and isinstance(item.get("asset"), dict)
            and item["asset"].get("symbol") == symbol
            and item["asset"].get("id")
        }
    )
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


def _independent_prefix_rows(
    report, descriptor, edge, first_session, source, source_rows_loader, last_session=None
):
    filename = descriptor["data_file"]
    rows, generation = source_rows_loader(edge.successor_symbol, filename)
    if generation != descriptor["download_sha256"]:
        return None
    indexed = [
        (position, row)
        for position, row in enumerate(rows, 1)
        if first_session <= _session_date(row.get("t")) < edge.effective_date
        and (last_session is None or _session_date(row.get("t")) <= last_session)
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
            or INDEPENDENT_HISTORY_WARNING in segment.get("warnings", [])
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


def assess_bridged_initial_prefix(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
):
    bridges = shared_inert_bridges(work_items)
    if not bridges or correlation_loader is None or source_rows_loader is None:
        return ()
    sources = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    rows, _, sessions = sources["raw"]
    stop, bridge_end = bridges[0]
    first, last = sessions[0], sessions[stop - 1]
    expected = expected_market_session_coverage(first.isoformat(), sessions[stop].isoformat())
    if tuple(day.isoformat() for day in sessions[:stop]) != expected.session_dates or any(
        not has_informative_trading(values[:stop])
        or has_prolonged_carry_forward(values[:stop])
        or Decimal(str(values[stop - 1]["c"])) != _inert(values[stop])
        for values, _, _ in sources.values()
    ):
        return ()
    qualified = []
    for edge in predecessor_prefix_candidates(symbol, lineage, intervals, edges):
        if not sessions[stop] <= edge.effective_date <= sessions[bridge_end]:
            continue
        owners = [
            item
            for item in intervals
            if item.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and item.end_date_exclusive == edge.effective_date
            and item.contains(first)
            and item.contains(last)
        ]
        if len(owners) != 1 or _positive_competing_owner(
            symbol, intervals, edge.predecessor_asset_id, first, last
        ):
            continue
        try:
            report = correlation_loader(edge.successor_symbol)
        except SecurityCorrelationError:
            continue
        descriptors = {item["adjustment"]: item for item in report.sources_by_filename.values()}
        if len(descriptors) != len(report.sources_by_filename) or set(descriptors) != set(sources):
            continue
        for adjustment, (values, source, _) in sources.items():
            try:
                donor = _independent_prefix_rows(
                    report, descriptors[adjustment], edge, first, source, source_rows_loader, last
                )
            except SecurityCorrelationError:
                break
            if (
                donor is None
                or tuple(_session_date(row["t"]).isoformat() for row in donor) != expected.session_dates
            ):
                break
            if adjustment == "raw" and [tuple(row[key] for key in _FIELDS) for row in donor] != [
                tuple(row[key] for key in _FIELDS) for row in values[:stop]
            ]:
                break
        else:
            qualified.append(IndependentHistoricalInterval(first, last, owners[0]))
    return (
        tuple(dict.fromkeys(qualified)) if len({item.interval.identity_key() for item in qualified}) == 1 else ()
    )


def assess_independent_donor_prefixes(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
):
    """Certify a complete donor prefix within an earlier, mixed source history."""
    if correlation_loader is None or source_rows_loader is None:
        return ()
    sources = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(sources) != len(work_items) or set(sources) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    qualified = []
    for edge in predecessor_prefix_candidates(symbol, lineage, intervals, edges):
        owners = [
            item
            for item in intervals
            if item.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            and item.end_date_exclusive == edge.effective_date
        ]
        if len(owners) != 1:
            continue
        try:
            report = correlation_loader(edge.successor_symbol)
        except SecurityCorrelationError:
            continue
        descriptors = {item["adjustment"]: item for item in report.sources_by_filename.values()}
        if len(descriptors) != len(report.sources_by_filename) or set(descriptors) != set(sources):
            continue
        try:
            raw_rows, generation = source_rows_loader(edge.successor_symbol, descriptors["raw"]["data_file"])
        except SecurityCorrelationError:
            continue
        donor_prefix = [row for row in raw_rows if _session_date(row["t"]) < edge.effective_date]
        if not donor_prefix or generation != descriptors["raw"]["download_sha256"] or not sources["raw"][2]:
            continue
        first = _session_date(donor_prefix[0]["t"])
        # Ordinary complete-source prefixes retain their existing certificate.
        # This extension cannot search through an informative donor to find an island.
        if first <= sources["raw"][2][0]:
            continue
        pad = 0
        leading_value = _inert(donor_prefix[0])
        while (
            pad < len(donor_prefix) and leading_value is not None and _inert(donor_prefix[pad]) == leading_value
        ):
            pad += 1
        traded = donor_prefix[pad:]
        if (
            not has_informative_trading(traded)
            or has_prolonged_carry_forward(traded)
            or Decimal(str(traded[0]["v"])) <= 0
            or Decimal(str(traded[0]["n"])) <= 0
        ):
            continue
        start, end = _session_date(traded[0]["t"]), _session_date(traded[-1]["t"])
        if not owners[0].contains(first) or not owners[0].contains(end):
            continue
        if any(
            item.historical_symbol == symbol
            and item.asset_id != owners[0].asset_id
            and item.start_date_inclusive is not None
            and item.start_date_inclusive <= end
            and (item.end_date_exclusive is None or item.end_date_exclusive > start)
            for item in intervals
        ):
            continue
        expected = expected_market_session_coverage(first.isoformat(), edge.effective_date.isoformat())
        for adjustment, (rows, source, sessions) in sources.items():
            selected = [row for row, session in zip(rows, sessions) if first <= session < edge.effective_date]
            try:
                donor = _independent_prefix_rows(
                    report, descriptors[adjustment], edge, first, source, source_rows_loader
                )
            except SecurityCorrelationError:
                break
            if (
                donor is None
                or tuple(_session_date(row["t"]).isoformat() for row in donor) != expected.session_dates
                or tuple(_session_date(row["t"]).isoformat() for row in selected) != expected.session_dates
                or (
                    pad
                    and (
                        None in {_inert(row) for row in donor[:pad]}
                        or len({_inert(row) for row in donor[:pad]}) != 1
                    )
                )
                or has_prolonged_carry_forward(donor[pad:])
                or has_prolonged_carry_forward(selected[pad:])
                or not has_informative_trading(donor[pad:])
                or (
                    adjustment == "raw"
                    and [tuple(row[key] for key in _FIELDS) for row in donor]
                    != [tuple(row[key] for key in _FIELDS) for row in selected]
                )
            ):
                break
        else:
            qualified.append(IndependentHistoricalInterval(start, end, owners[0]))
    return tuple(dict.fromkeys(qualified))


def assess_predecessor_prefixes(
    symbol, lineage, intervals, edges, work_items, *, correlation_loader, source_rows_loader
) -> tuple[PredecessorPrefix, ...]:
    # The caller supplies readers for selected independent successor evidence.
    # This producer proves only the returned prefix; consumers must retain its
    # bounds rather than reopen histories to extend it into an unobserved past.
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
