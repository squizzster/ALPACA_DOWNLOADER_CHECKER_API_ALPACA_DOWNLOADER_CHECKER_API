"""A resumed traded episode can be anchored by its own dated departure."""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from itertools import groupby

from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .action_identity import exact_cusip_lookup_asset
from .current_symbol_reuse import _complete_corporate_action_query, _current_identity
from .errors import SecurityCorrelationError
from .identity_partition import _session_date
from .occupancy_anchor import positive_occupancy_action_date
from .ownership_interval_rules import TickerOwnershipInterval
from .security_identity import security_identity_for_alpaca_assets
from .ticker_reuse_gap import adjustment_session_consensus, assess_ticker_reuse_gap
from .uuid_lineage import build_uuid_lineage_edges

VACANT_TICKER_BRIDGE_CORRELATION_VERSION = 69
ACTION_ANCHORED_VACANT_TICKER_BRIDGE_CORRELATION_VERSION = 74
VACANT_TICKER_BRIDGE_REASON = "certified_vacant_ticker_bridge_non_observation"


@dataclass(frozen=True)
class TerminalTradedEpisode:
    first: date
    last: date
    asset_id: str
    cusip: str
    terminated_during_reuse_gap: bool = False
    bounded_by_inert_bridges: bool = False
    initial_before_reuse_gap: bool = False
    preceding_inert_first: date | None = None
    preceding_inert_last: date | None = None


@dataclass(frozen=True)
class DatedDeparturePrefix:
    first: date
    last: date
    interval: TickerOwnershipInterval


@dataclass(frozen=True)
class HistoricalAsOfGap:
    first: date
    asof: date
    next_observed: date
    end_exclusive: date
    interval: TickerOwnershipInterval
    successor_asset_id: str
    successor_cusip: str
    action_id: str

    def contains_unresolved_tail(self, session: date) -> bool:
        return self.asof < session < self.end_exclusive


@dataclass(frozen=True)
class VacantTickerBridge:
    first: date
    last: date
    departure_action_id: str
    arrival_action_id: str

    def contains(self, session: date) -> bool:
        return self.first <= session <= self.last


def _current_same_uuid_alias(lineage, action, asset):
    if action.get("action_type") != "name_change":
        return False
    successor = exact_cusip_lookup_asset(lineage, action.get("new_cusip"))
    if successor is None or successor["id"] != asset["id"] or successor.get("symbol") != asset.get("symbol"):
        return False
    edges, _ = build_uuid_lineage_edges(lineage)
    current = (action.get("new_symbol"), action.get("new_cusip"))
    before = date.fromisoformat(action["process_date"])
    seen = set()
    while current[0] != asset.get("symbol"):
        if current in seen:
            return False
        seen.add(current)
        outgoing = [
            edge
            for edge in edges
            if (edge.predecessor_symbol, edge.predecessor_cusip) == current
            and edge.predecessor_asset_id == asset["id"]
            and edge.effective_date > before
        ]
        if len(outgoing) != 1:
            return False
        edge = outgoing[0]
        if edge.edge_type != "name_change" or edge.successor_asset_id != asset["id"]:
            return False
        current = (edge.successor_symbol, edge.successor_cusip)
        before = edge.effective_date
    return True


def terminal_episode_candidates(symbol, lineage):
    occupants = {
        item["id"] for item in lineage.get("identities", []) if item.get("symbol") == symbol and item.get("id")
    } | {
        item["asset_id"]
        for item in lineage.get("symbol_occupancy_evidence", [])
        if item.get("symbol") == symbol and item.get("asset_id")
    }
    if len(occupants) < 2:
        return ()
    candidates = []
    for action in lineage.get("corporate_actions", []):
        kind = action.get("action_type")
        role = "old" if kind == "name_change" else "acquiree"
        if kind not in ("name_change", "cash_merger", "stock_merger") or action.get(role + "_symbol") != symbol:
            continue
        cusip = action.get(role + "_cusip")
        asset = exact_cusip_lookup_asset(lineage, cusip)
        if asset is None or not action.get("id"):
            continue
        try:
            when = date.fromisoformat(action["process_date" if kind == "name_change" else "effective_date"])
        except (KeyError, TypeError, ValueError):
            continue
        if asset.get("symbol") != symbol:
            if not _current_same_uuid_alias(lineage, action, asset) or not any(
                (item.get("symbol"), item.get("cusip"), item.get("asset_id")) == (symbol, cusip, asset["id"])
                and any(
                    record.get("action_id") == action["id"]
                    and record.get("role") == "old"
                    and record.get("action_type") == "name_change"
                    for record in item.get("evidence", [])
                )
                for item in lineage.get("symbol_occupancy_evidence", [])
            ):
                continue
        candidates.append((when, asset["id"], cusip))
    return tuple(candidates)


def _inert(row):
    try:
        prices = tuple(Decimal(str(row[key])) for key in ("o", "h", "l", "c"))
        if Decimal(str(row["v"])) == 0 and Decimal(str(row["n"])) == 0 and len(set(prices)) == 1:
            return prices[0]
    except (KeyError, InvalidOperation, ValueError):
        pass
    return None


def _traded_prices(rows):
    return {
        tuple(row[key] for key in ("o", "h", "l", "c"))
        for row in rows
        if Decimal(str(row.get("v", "0"))) > 0 and Decimal(str(row.get("n", "0"))) > 0
    }


def has_prolonged_carry_forward(rows):
    return any(value is not None and sum(1 for _ in group) >= 100 for value, group in groupby(rows, _inert))


def has_traded_observation(rows):
    return bool(_traded_prices(rows))


def has_informative_trading(rows):
    return len(_traded_prices(rows)) >= 2


class PreparedSource(list):
    """Immutable selected source items with completed, price-only assessments."""

    def __init__(self, items):
        super().__init__(items)
        self.inert_bridges = {}


def shared_inert_bridges(
    work_items, *, include_edges=False, before=None, allowed_value_change_dates=frozenset()
):
    cache = getattr(work_items, "inert_bridges", None)
    key = include_edges, before, allowed_value_change_dates
    if cache is None:
        return _shared_inert_bridges(
            work_items,
            include_edges=include_edges,
            before=before,
            allowed_value_change_dates=allowed_value_change_dates,
        )
    if key not in cache:
        cache[key] = _shared_inert_bridges(
            work_items,
            include_edges=include_edges,
            before=before,
            allowed_value_change_dates=allowed_value_change_dates,
        )
    return cache[key]


def _shared_inert_bridges(
    work_items,
    *,
    include_edges=False,
    before=None,
    allowed_value_change_dates=frozenset(),
):
    """Return complete inert separators whose value changes are explicitly dated."""
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return ()
    if before is not None:
        stop = next((i for i, day in enumerate(sessions) if day >= before), len(sessions))
        views = {mode: (values[:stop], grid[:stop]) for mode, (values, grid) in views.items()}
        rows, sessions = views["raw"]
    bridges = []
    for value, group in groupby(enumerate(rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        first, last = positions[0], positions[-1]
        if value is None or len(positions) < 100 or not include_edges and (first == 0 or last + 1 == len(rows)):
            continue

        def supported_inert_values(other):
            values = [_inert(row) for row in other[first : last + 1]]
            return None not in values and all(
                left == right or sessions[first + index] in allowed_value_change_dates
                for index, (left, right) in enumerate(zip(values, values[1:]), start=1)
            )

        if all(supported_inert_values(other) for other, _ in views.values()):
            bridges.append((first, last))
    return tuple(bridges)


def _independently_trades_through_vacancy(
    symbol,
    asset_id,
    cusip,
    expected_sessions,
    source,
    *,
    correlation_loader,
    source_rows_loader,
):
    """Require independently assigned positive raw trades on every vacant session."""
    try:
        report = correlation_loader(symbol)
        descriptors = {}
        for descriptor in report.sources_by_filename.values():
            adjustment = descriptor["adjustment"]
            if adjustment in descriptors:
                return False
            descriptors[adjustment] = descriptor
        if set(descriptors) != set(ALPACA_ADJUSTMENT_MODES):
            return False
        descriptor = descriptors["raw"]
        rows, generation = source_rows_loader(symbol, descriptor["data_file"])
        if generation != descriptor["download_sha256"]:
            return False
        selected = [
            (position, row)
            for position, row in enumerate(rows, 1)
            if _session_date(row.get("t")) in expected_sessions
        ]
        if tuple(_session_date(row.get("t")) for _, row in selected) != expected_sessions:
            return False
        selected_rows = [row for _, row in selected]
        if not has_informative_trading(selected_rows) or any(
            _inert(row) is not None
            or Decimal(str(row.get("v", "0"))) <= 0
            or Decimal(str(row.get("n", "0"))) <= 0
            for row in selected_rows
        ):
            return False
        positions = {position for position, _ in selected}
        covered = set()
        for segment in report.correlated["segments"]:
            if segment.get("data_file") != descriptor["data_file"]:
                continue
            overlap = positions.intersection(range(segment["row_start"], segment["row_end"] + 1))
            if not overlap:
                continue
            security = segment.get("canonical_security") or {}
            if (
                segment.get("adjustment") != "raw"
                or segment.get("historical_symbol") != symbol
                or (security.get("alpaca_asset_id"), security.get("cusip")) != (asset_id, cusip)
                or segment.get("feed") != source["feed"]
                or segment.get("timeframe") != source["timeframe"]
                or segment.get("download_sha256") != generation
                or covered.intersection(overlap)
            ):
                return False
            covered.update(overlap)
        return covered == positions
    except (
        AttributeError,
        InvalidOperation,
        KeyError,
        SecurityCorrelationError,
        TypeError,
        ValueError,
    ):
        return False


def _dated_action_values(action):
    values = []
    for field in ("ex_date", "effective_date", "process_date", "record_date", "payable_date"):
        value = action.get(field)
        if value is None:
            continue
        try:
            values.append(date.fromisoformat(value))
        except (TypeError, ValueError):
            return None
    return tuple(values)


def _known_label_occupants(lineage, symbol):
    occupants = {
        item.get("id")
        for item in lineage.get("identities", [])
        if item.get("symbol") == symbol and item.get("id")
    }
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        occupants.update(
            item.get("asset_id")
            for item in lineage.get(field, [])
            if item.get("symbol") == symbol and item.get("asset_id")
        )
    occupants.update(
        item["asset"].get("id")
        for item in lineage.get("asset_lookups", [])
        if item.get("lookup_kind") == "symbol"
        and item.get("lookup") == symbol
        and isinstance(item.get("asset"), dict)
        and item["asset"].get("id")
    )
    return occupants


def _action_anchored_vacant_ticker_bridges(
    symbol,
    lineage,
    intervals,
    edges,
    work_items,
):
    """Exclude an inert gap between closed and action-anchored label owners.

    The ownership producers already proved both endpoints.  This consumer does
    not replay their identity decision: it only verifies that every retained
    row strictly between those endpoints is one complete synthetic carry and
    that no third occupancy or action claims the gap.
    """
    unresolved_cusips = lineage.get("unresolved_cusips")
    if not isinstance(unresolved_cusips, list) or any(
        not isinstance(cusip, str) or not cusip for cusip in unresolved_cusips
    ):
        return ()
    actions = {
        action.get("id"): action
        for action in lineage.get("corporate_actions", ())
        if isinstance(action, dict) and isinstance(action.get("id"), str) and action["id"]
    }
    if len(actions) != sum(
        isinstance(action, dict) and isinstance(action.get("id"), str) and bool(action["id"])
        for action in lineage.get("corporate_actions", ())
    ):
        return ()
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    grids = {sessions for _, _, sessions in views.values()}
    if len(grids) != 1:
        return ()
    sessions = next(iter(grids))
    symbol_intervals = [item for item in intervals if item.historical_symbol == symbol]
    departures = [
        (edge, interval)
        for edge in edges
        for interval in symbol_intervals
        if edge.edge_type == "name_change"
        and edge.predecessor_symbol == symbol
        and edge.successor_symbol != symbol
        and edge.predecessor_asset_id == edge.successor_asset_id
        and interval.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        and interval.end_date_exclusive == edge.effective_date
    ]
    arrivals = []
    for interval in symbol_intervals:
        matching_rules = [
            rule for rule in interval.applied_rules if rule.name == "action_anchored_current_ticker_occupancy"
        ]
        action_ids = tuple(
            dict.fromkeys(action_id for rule in matching_rules for action_id in rule.supporting_action_ids)
        )
        if (
            interval.start_date_inclusive is None
            or interval.end_date_exclusive is not None
            or len(matching_rules) != 1
            or len(action_ids) != 1
        ):
            continue
        action = actions.get(action_ids[0])
        if action is None or action.get("action_type") != "unit_split":
            continue
        arrivals.append((interval, action_ids[0]))

    candidates = {}
    for (departure, prior), (arrival, arrival_action_id) in (
        (departure_item, arrival_item) for departure_item in departures for arrival_item in arrivals
    ):
        first_boundary = prior.end_date_exclusive
        end_boundary = arrival.start_date_inclusive
        if (
            first_boundary is None
            or end_boundary is None
            or first_boundary >= end_boundary
            or prior.asset_id == arrival.asset_id
            or prior.cusip == arrival.cusip
            or not _complete_corporate_action_query(lineage, first_boundary, end_boundary)
        ):
            continue
        if any(
            item not in (prior, arrival)
            and (item.start_date_inclusive is None or item.start_date_inclusive < end_boundary)
            and (item.end_date_exclusive is None or item.end_date_exclusive > first_boundary)
            for item in symbol_intervals
        ):
            continue
        touching_edges = {
            edge
            for edge in edges
            if first_boundary <= edge.effective_date <= end_boundary
            and symbol in (edge.predecessor_symbol, edge.successor_symbol)
        }
        if touching_edges != {departure}:
            continue
        expected_occupants = {prior.asset_id, arrival.asset_id}
        if _known_label_occupants(lineage, symbol) != expected_occupants:
            continue
        allowed_action_ids = {departure.action_id, arrival_action_id}
        conflict = False
        for action in lineage.get("corporate_actions", ()):
            if not isinstance(action, dict) or not any(
                value == symbol for key, value in action.items() if key == "symbol" or key.endswith("_symbol")
            ):
                continue
            dated = _dated_action_values(action)
            if dated is None:
                conflict = True
                break
            if action.get("id") not in allowed_action_ids and any(
                first_boundary <= when <= end_boundary for when in dated
            ):
                conflict = True
                break
        if conflict:
            continue
        for occupancy in lineage.get("symbol_occupancy_evidence", ()):
            if not isinstance(occupancy, dict) or occupancy.get("symbol") != symbol:
                continue
            evidence = occupancy.get("evidence")
            if not isinstance(evidence, list) or not evidence:
                conflict = True
                break
            for record in evidence:
                try:
                    when = date.fromisoformat(record["process_date"])
                except (KeyError, TypeError, ValueError):
                    conflict = True
                    break
                if first_boundary <= when <= end_boundary and (
                    occupancy.get("asset_id") not in expected_occupants
                    or record.get("action_id") not in allowed_action_ids
                ):
                    conflict = True
                    break
            if conflict:
                break
        if conflict:
            continue
        try:
            expected = tuple(
                date.fromisoformat(value)
                for value in expected_market_session_coverage(
                    first_boundary.isoformat(), end_boundary.isoformat()
                ).session_dates
            )
        except (MarketCalendarError, ValueError):
            continue
        positions = [position for position, session in enumerate(sessions) if session in expected]
        if (
            not expected
            or not positions
            or positions != list(range(positions[0], positions[-1] + 1))
            or tuple(sessions[position] for position in positions) != expected
        ):
            continue
        start, stop = positions[0], positions[-1] + 1
        if start == 0 or stop >= len(sessions) or sessions[stop] != end_boundary:
            continue
        containing_bridges = [
            bridge for bridge in shared_inert_bridges(work_items) if bridge[0] <= start and bridge[1] == stop - 1
        ]
        if len(containing_bridges) != 1:
            continue
        inert_start = containing_bridges[0][0]
        if any(
            Decimal(str(row.get("vw", "0"))) != 0
            for values, _, _ in views.values()
            for row in values[start:stop]
        ):
            continue
        raw = views["raw"][0]
        if Decimal(str(raw[inert_start - 1]["c"])) != _inert(raw[inert_start]) or any(
            not has_traded_observation(values[index : index + 1])
            for values, _, _ in views.values()
            for index in (inert_start - 1, stop)
        ):
            continue
        bridge = VacantTickerBridge(
            expected[0],
            expected[-1],
            departure.action_id,
            arrival_action_id,
        )
        candidates.setdefault((bridge.first, bridge.last), []).append(bridge)
    return tuple(items[0] for _, items in sorted(candidates.items()) if len(items) == 1)


def assess_vacant_ticker_bridges(
    symbol,
    lineage,
    intervals,
    edges,
    work_items,
    *,
    evidence_issues,
    correlation_loader,
    source_rows_loader,
    include_action_anchored_arrivals=False,
):
    """Certify complete synthetic observations during a proven ticker vacancy.

    The result is a non-observation disposition only. It never assigns a row,
    changes a ticker interval, or joins the two identities delimiting the gap.
    """
    if evidence_issues or correlation_loader is None or source_rows_loader is None:
        return ()
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if (
        len(views) != len(work_items)
        or set(views) != set(ALPACA_ADJUSTMENT_MODES)
        or any(source.get("recorded_integrity_failure") is not None for _, source, _ in work_items)
    ):
        return ()
    grids = {sessions for _, _, sessions in views.values()}
    if len(grids) != 1:
        return ()
    sessions = next(iter(grids))
    legacy_evidence_complete = lineage.get("unresolved_cusips") == []
    departures = (
        [
            edge
            for edge in edges
            if edge.edge_type == "name_change"
            and edge.predecessor_symbol == symbol
            and edge.successor_symbol != symbol
            and edge.predecessor_asset_id == edge.successor_asset_id
        ]
        if legacy_evidence_complete
        else []
    )
    arrivals = (
        [
            edge
            for edge in edges
            if edge.edge_type == "name_change"
            and edge.successor_symbol == symbol
            and edge.predecessor_symbol != symbol
            and edge.predecessor_asset_id == edge.successor_asset_id
        ]
        if legacy_evidence_complete
        else []
    )
    candidates = {}
    for departure in departures:
        for arrival in arrivals:
            first_boundary, end_boundary = departure.effective_date, arrival.effective_date
            if (
                first_boundary >= end_boundary
                or departure.predecessor_asset_id == arrival.successor_asset_id
                or departure.predecessor_cusip == arrival.successor_cusip
                or not _complete_corporate_action_query(lineage, first_boundary, end_boundary)
            ):
                continue
            departure_intervals = [
                item
                for item in intervals
                if item.identity_key()
                == (
                    symbol,
                    departure.predecessor_cusip,
                    departure.predecessor_asset_id,
                )
                and item.end_date_exclusive == first_boundary
            ]
            arrival_intervals = [
                item
                for item in intervals
                if item.identity_key() == (symbol, arrival.successor_cusip, arrival.successor_asset_id)
                and item.start_date_inclusive == end_boundary
            ]
            if len(departure_intervals) != 1 or len(arrival_intervals) != 1:
                continue
            if any(
                item.historical_symbol == symbol
                and (item.start_date_inclusive is None or item.start_date_inclusive < end_boundary)
                and (item.end_date_exclusive is None or item.end_date_exclusive > first_boundary)
                for item in intervals
            ):
                continue
            touching_edges = {
                edge
                for edge in edges
                if first_boundary <= edge.effective_date <= end_boundary
                and symbol in (edge.predecessor_symbol, edge.successor_symbol)
            }
            if touching_edges != {departure, arrival}:
                continue
            occupants = _known_label_occupants(lineage, symbol)
            expected_occupants = {
                departure.predecessor_asset_id,
                arrival.successor_asset_id,
            }
            if not expected_occupants <= occupants or not occupants <= expected_occupants:
                continue
            allowed_action_ids = set()
            allowed_value_change_dates = set()
            for action in lineage.get("corporate_actions", []):
                if (
                    action.get("action_type") != "cash_dividend"
                    or (action.get("symbol"), action.get("cusip")) != (symbol, arrival.successor_cusip)
                    or not action.get("id")
                ):
                    continue
                try:
                    when = positive_occupancy_action_date(action, symbol, arrival.successor_cusip)
                except (KeyError, TypeError, ValueError, InvalidOperation):
                    continue
                if when is not None and first_boundary <= when < end_boundary:
                    allowed_action_ids.add(action["id"])
                    allowed_value_change_dates.add(when)
            conflicting_action = False
            for action in lineage.get("corporate_actions", []):
                if not any(value == symbol for key, value in action.items() if key.endswith("symbol")):
                    continue
                dates = _dated_action_values(action)
                if dates is None:
                    conflicting_action = True
                    break
                if action.get("id") not in {departure.action_id, arrival.action_id, *allowed_action_ids} and any(
                    first_boundary <= when < end_boundary for when in dates
                ):
                    conflicting_action = True
                    break
            if conflicting_action:
                continue
            conflicting_occupancy = False
            for occupancy in lineage.get("symbol_occupancy_evidence", []):
                if occupancy.get("symbol") != symbol:
                    continue
                if occupancy.get("asset_id") not in expected_occupants:
                    conflicting_occupancy = True
                    break
                for record in occupancy.get("evidence", []):
                    try:
                        when = date.fromisoformat(record["process_date"])
                    except (KeyError, TypeError, ValueError):
                        conflicting_occupancy = True
                        break
                    if first_boundary <= when < end_boundary and record.get("action_id") not in {
                        departure.action_id,
                        *allowed_action_ids,
                    }:
                        conflicting_occupancy = True
                        break
                if conflicting_occupancy:
                    break
            if conflicting_occupancy:
                continue
            try:
                expected = tuple(
                    date.fromisoformat(value)
                    for value in expected_market_session_coverage(
                        first_boundary.isoformat(), end_boundary.isoformat()
                    ).session_dates
                )
            except (MarketCalendarError, ValueError):
                continue
            if not expected or expected[0] != first_boundary:
                continue
            positions = [position for position, session in enumerate(sessions) if session in expected]
            if (
                not positions
                or positions != list(range(positions[0], positions[-1] + 1))
                or tuple(sessions[position] for position in positions) != expected
            ):
                continue
            start, stop = positions[0], positions[-1] + 1
            if start == 0 or stop >= len(sessions) or sessions[stop] != end_boundary:
                continue
            bridges = shared_inert_bridges(
                work_items,
                allowed_value_change_dates=frozenset(allowed_value_change_dates),
            )
            if (start, stop - 1) not in bridges:
                continue
            if any(
                Decimal(str(row.get("vw", "0"))) != 0
                for values, _, _ in views.values()
                for row in values[start:stop]
            ):
                continue
            raw = views["raw"][0]
            if Decimal(str(raw[start - 1]["c"])) != _inert(raw[start]) or any(
                not has_traded_observation(values[index : index + 1])
                for values, _, _ in views.values()
                for index in (start - 1, stop)
            ):
                continue
            source = views["raw"][1]
            if not _independently_trades_through_vacancy(
                departure.successor_symbol,
                departure.successor_asset_id,
                departure.successor_cusip,
                expected,
                source,
                correlation_loader=correlation_loader,
                source_rows_loader=source_rows_loader,
            ) or not _independently_trades_through_vacancy(
                arrival.predecessor_symbol,
                arrival.predecessor_asset_id,
                arrival.predecessor_cusip,
                expected,
                source,
                correlation_loader=correlation_loader,
                source_rows_loader=source_rows_loader,
            ):
                continue
            bridge = VacantTickerBridge(
                expected[0],
                expected[-1],
                departure.action_id,
                arrival.action_id,
            )
            candidates.setdefault((bridge.first, bridge.last), []).append(bridge)
    certified = [items[0] for items in candidates.values() if len(items) == 1]
    if include_action_anchored_arrivals:
        certified.extend(
            _action_anchored_vacant_ticker_bridges(
                symbol,
                lineage,
                intervals,
                edges,
                work_items,
            )
        )
        unique = {}
        for bridge in certified:
            unique.setdefault((bridge.first, bridge.last), []).append(bridge)
        certified = [items[0] for items in unique.values() if len(set(items)) == 1]
    certified.sort(key=lambda item: (item.first, item.last, item.departure_action_id, item.arrival_action_id))
    if any(left.last >= right.first for left, right in zip(certified, certified[1:])):
        return ()
    return tuple(certified)


def _positive_competing_owner(symbol, intervals, asset_id, first, last):
    return any(
        item.historical_symbol == symbol
        and item.asset_id != asset_id
        and (
            item.start_date_inclusive is not None
            and item.start_date_inclusive <= last
            and (item.end_date_exclusive is None or item.end_date_exclusive > first)
            or item.end_date_exclusive is not None
            and first < item.end_date_exclusive <= last
        )
        for item in intervals
    )


def _dividend_backed_current_reuse(
    symbol, lineage, intervals, candidates, current_id, cusip, bridge, first, last, end
):
    """A dated dividend anchors the current issue within an observed resumption.

    It does not establish inception. The shared inert separator and unrelated
    former departure distinguish this episode from a continuing stock's halt.
    """
    # Even an uncertified continuation vetoes a claim that two episodes are
    # unrelated. This screen does not publish or accept UUID membership.
    edges, issues = build_uuid_lineage_edges(lineage, candidates_only=True)
    rejected = {issue["rejected_action_id"] for issue in issues if "rejected_action_id" in issue}
    actions = {action.get("id"): action for action in lineage.get("corporate_actions", [])}
    departures = set()
    # Trust the ownership boundary, not merely a raw action candidate. A known
    # departure does not require resolving the former issue's successor UUID.
    for when, asset_id, former_cusip in candidates:
        if not bridge[0] <= when <= bridge[1] or asset_id == current_id:
            continue
        for interval in intervals:
            if (
                interval.identity_key() != (symbol, former_cusip, asset_id)
                or interval.end_date_exclusive != when
            ):
                continue
            for rule in interval.applied_rules:
                for action_id in rule.supporting_action_ids:
                    action = actions.get(action_id, {})
                    role = "old" if action.get("action_type") == "name_change" else "acquiree"
                    effective = (
                        action.get("process_date")
                        if role == "old"
                        else action.get("effective_date") or action.get("process_date")
                    )
                    if (
                        action_id not in rejected
                        and effective == when.isoformat()
                        and (action.get(role + "_symbol"), action.get(role + "_cusip")) == (symbol, former_cusip)
                    ):
                        departures.add((when, asset_id))
    if not departures or not _complete_corporate_action_query(lineage, min(d[0] for d in departures), end):
        return False
    component = security_identity_for_alpaca_assets(
        (current_id,), edges, tickers=(symbol,), basis="current_reuse_screen"
    ).member_alpaca_asset_ids
    if any(asset_id in component for _, asset_id in departures):
        return False
    anchored = False
    for action in lineage.get("corporate_actions", []):
        if not any(value == symbol for key, value in action.items() if key.endswith("symbol")):
            continue
        try:
            when = date.fromisoformat(
                action.get("ex_date") or action.get("effective_date") or action["process_date"]
            )
        except (KeyError, TypeError, ValueError):
            return False
        if not first <= when <= last:
            continue
        if action.get("action_type") != "cash_dividend" or action.get("cusip") != cusip:
            return False  # A return, structural change or another issue needs its own dated allocation.
        try:
            anchored |= positive_occupancy_action_date(action, symbol, cusip) == when
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return False
    # A conflicting dated occupant may not have produced an ownership interval.
    for occupant in lineage.get("symbol_occupancy_evidence", []):
        if occupant.get("symbol") != symbol or (occupant.get("asset_id"), occupant.get("cusip")) == (
            current_id,
            cusip,
        ):
            continue
        for record in occupant.get("evidence", []):
            try:
                when = date.fromisoformat(record["process_date"])
            except (KeyError, TypeError, ValueError):
                return False
            if first <= when <= last:
                return False
    return anchored


def assess_bridged_traded_episodes(symbol, lineage, intervals, work_items):
    """Anchor bounded resumed runs, without interpreting inert rows as trades."""
    bridges = shared_inert_bridges(work_items)
    if not bridges:
        return ()
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    rows, source, sessions = views["raw"]
    candidates = terminal_episode_candidates(symbol, lineage)
    current = _current_identity(lineage, symbol)
    episodes = []
    for number, (_, left_end) in enumerate(bridges):
        following = bridges[number + 1] if number + 1 < len(bridges) else None
        start = left_end + 1
        stop = following[0] if following else len(rows)
        first, last = sessions[start], sessions[stop - 1]
        expected = expected_market_session_coverage(first.isoformat(), (last + timedelta(days=1)).isoformat())
        if tuple(day.isoformat() for day in sessions[start:stop]) != expected.session_dates or any(
            not has_informative_trading(values[start:stop]) or has_prolonged_carry_forward(values[start:stop])
            for values, _, _ in views.values()
        ):
            continue
        owners = set()
        current_reuse = False
        if following:
            # A named departure near the first inert row anchors the preceding
            # complete episode. An event anywhere in a long bridge is insufficient.
            for when, asset_id, cusip in candidates:
                left, right = sorted((when, sessions[following[0]]))
                if (
                    sessions[following[0]] <= when <= sessions[following[1]]
                    and (
                        left == right
                        or expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count
                        <= 4
                    )
                    and all(
                        Decimal(str(values[stop - 1]["c"])) == _inert(values[stop])
                        for values, _, _ in views.values()
                    )
                ):
                    owners.add((asset_id, cusip))
        elif current is not None:
            asset_id, cusip, _, _ = current
            endpoint = exact_cusip_lookup_asset(lineage, cusip)
            complete = expected_market_session_coverage(first.isoformat(), source["request_to"])
            if (
                endpoint is None
                or endpoint.get("status") != "active"
                or tuple(day.isoformat() for day in sessions[start:]) != complete.session_dates
            ):
                continue
            if _dividend_backed_current_reuse(
                symbol,
                lineage,
                intervals,
                candidates,
                asset_id,
                cusip,
                (sessions[bridges[number][0]], sessions[left_end]),
                first,
                last,
                date.fromisoformat(source["request_to"][:10]),
            ):
                owners.add((asset_id, cusip))
                current_reuse = True
            for action in lineage.get("corporate_actions", []):
                if (
                    action.get("action_type") != "stock_merger"
                    or (action.get("acquirer_symbol"), action.get("acquirer_cusip")) != (symbol, cusip)
                    or not action.get("id")
                ):
                    continue
                try:
                    when = date.fromisoformat(action["effective_date"])
                    rates = [Decimal(str(action[key])) for key in ("acquirer_rate", "acquiree_rate")]
                except (KeyError, ValueError, InvalidOperation):
                    continue
                if (
                    all(rate.is_finite() and rate > 0 for rate in rates)
                    and sessions[bridges[number][0]] <= when <= sessions[left_end]
                    and expected_market_session_coverage(when.isoformat(), first.isoformat()).session_count <= 4
                ):
                    owners.add((asset_id, cusip))
        if len(owners) != 1:
            continue
        asset_id, cusip = next(iter(owners))
        if _positive_competing_owner(symbol, intervals, asset_id, first, last) or any(
            item.historical_symbol == symbol
            and item.asset_id == asset_id
            and item.start_date_inclusive is not None
            and first < item.start_date_inclusive <= last
            and not (
                current_reuse
                and item.cusip == cusip
                and item.end_date_exclusive is None
                and item.applied_rules
                and all(rule.name == "action_anchored_current_ticker_occupancy" for rule in item.applied_rules)
            )
            for item in intervals
        ):
            continue
        episodes.append(
            TerminalTradedEpisode(
                first,
                last,
                asset_id,
                cusip,
                bounded_by_inert_bridges=True,
                preceding_inert_first=sessions[bridges[number][0]] if current_reuse else None,
                preceding_inert_last=sessions[left_end] if current_reuse else None,
            )
        )
    return tuple(episodes)


def assess_dated_departure_prefix(symbol, lineage, intervals, work_items):
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return None
    first = last = None
    for value, group in groupby(enumerate(rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        if value is not None and len(positions) >= 100:
            first, last = positions[0], positions[-1]
            break
    if first in (None, 0) or last + 1 >= len(rows):
        return None
    if not has_informative_trading(rows[:first]) or not has_informative_trading(rows[last + 1 :]):
        return None
    for values, _ in views.values():
        inert_values = {_inert(row) for row in values[first : last + 1]}
        if None in inert_values or len(inert_values) != 1 or has_prolonged_carry_forward(values[:first]):
            return None
        if Decimal(str(values[first - 1]["c"])) != next(iter(inert_values)):
            return None
    expected = expected_market_session_coverage(sessions[0].isoformat(), sessions[first].isoformat())
    if tuple(session.isoformat() for session in sessions[:first]) != expected.session_dates:
        return None
    start, end, inactive_start = sessions[0], sessions[first - 1], sessions[first]
    actions = {action.get("id"): action for action in lineage.get("corporate_actions", []) if action.get("id")}
    qualified = []
    for interval in intervals:
        when = interval.end_date_exclusive
        if (
            interval.historical_symbol != symbol
            or when is None
            or not interval.contains(start)
            or not interval.contains(end)
        ):
            continue
        left, right = sorted((when, inactive_start))
        if (
            left != right
            and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
        ):
            continue
        endpoint = exact_cusip_lookup_asset(lineage, interval.cusip)
        if endpoint is None or endpoint["id"] != interval.asset_id:
            continue
        action_ids = {action_id for rule in interval.applied_rules for action_id in rule.supporting_action_ids}
        supported = False
        for action_id in action_ids:
            action = actions.get(action_id, {})
            kind = action.get("action_type")
            role = "old" if kind == "name_change" else "acquiree"
            if kind in ("name_change", "cash_merger", "stock_merger") and (
                action.get(role + "_symbol"),
                action.get(role + "_cusip"),
            ) == (symbol, interval.cusip):
                supported = True
        if not supported:
            continue
        if any(
            other.historical_symbol == symbol
            and other.asset_id != interval.asset_id
            and (
                (
                    other.start_date_inclusive is not None
                    and other.start_date_inclusive <= end
                    and (other.end_date_exclusive is None or other.end_date_exclusive > start)
                )
                or (other.end_date_exclusive is not None and start < other.end_date_exclusive <= end)
            )
            for other in intervals
        ):
            continue
        qualified.append(interval)
    if len({item.identity_key() for item in qualified}) != 1:
        return None
    return DatedDeparturePrefix(start, end, qualified[0])


def assess_historical_asof_gap(symbol, lineage, intervals, work_items):
    """Bound an old mapped prefix without inventing the later ticker boundary.

    A mapping as-of selects an entity, not the end of its observations. It can
    nevertheless bound the already supported side of a discontinuity when the
    selected date is the complete prefix endpoint and provider evidence later
    names a distinct successor. Rows after the gap retain their ambiguity.
    """
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    _, _, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, _, grid in views.values()):
        return None
    asof_values = {item[1].get("asof") for item in views.values()}
    if len(asof_values) != 1:
        return None
    try:
        asof = date.fromisoformat(next(iter(asof_values))[:10])
    except (TypeError, ValueError):
        return None
    if asof not in sessions:
        return None
    position = sessions.index(asof)
    if position + 1 >= len(sessions):
        return None
    first, next_observed = sessions[0], sessions[position + 1]
    expected_prefix = expected_market_session_coverage(first.isoformat(), (asof + timedelta(days=1)).isoformat())
    if tuple(session.isoformat() for session in sessions[: position + 1]) != expected_prefix.session_dates:
        return None
    gap_start = asof + timedelta(days=1)
    if gap_start >= next_observed:
        return None
    missing = expected_market_session_coverage(gap_start.isoformat(), next_observed.isoformat())
    if not missing.session_dates:
        return None
    if any(
        not has_informative_trading(values[: position + 1])
        or has_prolonged_carry_forward(values[: position + 1])
        for values, _, _ in views.values()
    ):
        return None

    owners = [
        interval for interval in intervals if interval.historical_symbol == symbol and interval.contains(asof)
    ]
    if len(owners) != 1:
        return None
    owner = owners[0]
    endpoint = exact_cusip_lookup_asset(lineage, owner.cusip)
    if (
        endpoint is None
        or endpoint.get("id") != owner.asset_id
        or endpoint.get("symbol") != symbol
        or endpoint.get("status") != "inactive"
    ):
        return None
    if any(
        interval.historical_symbol == symbol
        and interval.asset_id != owner.asset_id
        and (interval.start_date_inclusive is None or interval.start_date_inclusive <= asof)
        and (interval.end_date_exclusive is None or interval.end_date_exclusive > first)
        for interval in intervals
    ):
        return None

    supporting = {action_id for rule in owner.applied_rules for action_id in rule.supporting_action_ids}
    candidates = []
    for action in lineage.get("corporate_actions", []):
        if (
            action.get("id") not in supporting
            or action.get("action_type") != "name_change"
            or (action.get("old_symbol"), action.get("old_cusip")) != (symbol, owner.cusip)
            or action.get("new_symbol") in (None, symbol)
            or action.get("new_cusip") in (None, owner.cusip)
        ):
            continue
        successor = exact_cusip_lookup_asset(lineage, action["new_cusip"])
        try:
            when = date.fromisoformat(action["process_date"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            successor is not None
            and successor.get("id") not in (None, owner.asset_id)
            and successor.get("symbol") == action["new_symbol"]
            and owner.end_date_exclusive == when
            and next_observed < when
        ):
            candidates.append((action, successor, when))
    if len(candidates) != 1:
        return None
    action, successor, end = candidates[0]
    if not _complete_corporate_action_query(lineage, first, end):
        return None
    return HistoricalAsOfGap(
        first,
        asof,
        next_observed,
        end,
        owner,
        successor["id"],
        action["new_cusip"],
        action["id"],
    )


def _same_day_cash_termination(symbol, cusip, when, lineage, intervals):
    """A cash closing date need not exclude that day's departing-issue trades."""
    supporting = {
        action_id
        for interval in intervals
        for rule in interval.applied_rules
        for action_id in rule.supporting_action_ids
    }
    cash = False
    for action in lineage.get("corporate_actions", []):
        kind = action.get("action_type")
        role = "old" if kind == "name_change" else "acquiree"
        if (
            kind not in ("name_change", "cash_merger", "stock_merger")
            or action.get(role + "_symbol") != symbol
            or action.get("process_date" if kind == "name_change" else "effective_date") != when.isoformat()
        ):
            continue
        if kind != "cash_merger" or action.get(role + "_cusip") != cusip:
            return False
        try:
            rate = Decimal(str(action.get("rate")))
        except InvalidOperation:
            return False
        if not rate.is_finite() or rate <= 0:
            return False
        cash |= action.get("id") in supporting
    return cash


def assess_terminal_traded_episode(symbol, lineage, intervals, work_items):
    candidates = terminal_episode_candidates(symbol, lineage)
    if not candidates:
        return None
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return None
    raw_source = next(source for _, source, _ in work_items if source["adjustment"] == "raw")
    gap = assess_ticker_reuse_gap(
        sessions,
        raw_source,
        lineage,
        intervals,
        adjustment_session_consensus((adjustment, grid) for adjustment, (_, grid) in views.items()),
    )
    prior_end = len(rows)
    if gap is not None:
        if (
            not (gap.complete_current_suffix_eligible or gap.active_current_suffix_eligible)
            or gap.current_asset_status != "active"
        ):
            return None
        prior_end = sessions.index(gap.left_observed_session) + 1
    prior_rows = rows[:prior_end]
    separators = []
    for key, group in groupby(enumerate(prior_rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        first, last = positions[0], positions[-1]
        if key is not None and len(positions) >= 100 and first > 0 and last + 1 < len(prior_rows):
            if _traded_prices(prior_rows[:first]) and len(_traded_prices(prior_rows[last + 1 :])) >= 2:
                separators.append((first, last))
    initial = not separators
    if initial:
        if gap is None or any(
            not has_informative_trading(values[:prior_end]) or has_prolonged_carry_forward(values[:prior_end])
            for values, _ in views.values()
        ):
            return None
        episode_start = 0
    else:
        first, last = separators[-1]
        for values, _ in views.values():
            value = _inert(values[last])
            suffix_first = last
            while suffix_first > first and value is not None and _inert(values[suffix_first - 1]) == value:
                suffix_first -= 1
            if (
                value is None
                or last - suffix_first + 1 < 100
                or has_prolonged_carry_forward(values[last + 1 : prior_end])
            ):
                return None
        episode_start = last + 1
    start, end = sessions[episode_start], sessions[prior_end - 1]
    expected = expected_market_session_coverage(start.isoformat(), (end + timedelta(days=1)).isoformat())
    if tuple(session.isoformat() for session in sessions[episode_start:prior_end]) != expected.session_dates:
        return None
    qualified = {}
    for when, asset_id, cusip in candidates:
        episode_end = end
        # Event processing and the last trading session can differ by a few
        # sessions. That tolerance supports a terminal episode, not a new birth.
        left, right = sorted((when, end))
        if gap is None:
            if when < start or (
                left != right
                and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
            ):
                continue
        else:
            endpoint = exact_cusip_lookup_asset(lineage, cusip)
            owner_intervals = [
                interval
                for interval in intervals
                if interval.identity_key() == (symbol, cusip, asset_id)
                and interval.end_date_exclusive == when
                and interval.contains(start)
            ]
            same_day_cash = (
                initial
                and when == end
                and _same_day_cash_termination(symbol, cusip, when, lineage, owner_intervals)
            )
            if initial and (
                left != right
                and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
                or not any(interval.contains(end) or same_day_cash for interval in owner_intervals)
            ):
                continue
            if same_day_cash and any(
                not _traded_prices(values[prior_end - 1 : prior_end]) for values, _ in views.values()
            ):
                # An untraded event-day observation does not establish ownership
                # of that row, but must not erase the independently bounded prefix.
                if prior_end - episode_start < 2:
                    continue
                episode_end = sessions[prior_end - 2]
            known = (
                {
                    item.get("id")
                    for item in lineage.get("identities", [])
                    if item.get("symbol") == symbol and item.get("id")
                }
                | {
                    item.get("asset_id")
                    for item in lineage.get("symbol_occupancy_evidence", [])
                    if item.get("symbol") == symbol and item.get("asset_id")
                }
                | {
                    item.get("asset_id")
                    for item in lineage.get("input_symbol_occupants", [])
                    if item.get("symbol") == symbol and item.get("asset_id")
                }
            )
            known.update(
                item["asset"].get("id")
                for item in lineage.get("asset_lookups", [])
                if item.get("found") is True
                and isinstance(item.get("asset"), dict)
                and item["asset"].get("symbol") == symbol
                and item["asset"].get("id")
            )
            if (
                not (end < when or same_day_cash)
                or when >= gap.right_observed_session
                or endpoint.get("status") != "inactive"
                or asset_id == gap.current_asset_id
                or known != {asset_id, gap.current_asset_id}
                or any(
                    interval.asset_id == asset_id
                    and interval.historical_symbol == symbol
                    and interval.start_date_inclusive is not None
                    and interval.start_date_inclusive > start
                    for interval in intervals
                )
            ):
                continue
        if any(
            interval.historical_symbol == symbol
            and interval.asset_id != asset_id
            and (interval.start_date_inclusive is None or interval.start_date_inclusive <= end)
            and (interval.end_date_exclusive is None or interval.end_date_exclusive > start)
            for interval in intervals
        ):
            continue
        if any(
            item.get("asset_id") not in (None, asset_id)
            for item in lineage.get("symbol_occupancy_evidence", [])
            if (item.get("symbol"), item.get("cusip")) == (symbol, cusip)
        ):
            continue
        previous_end, previous_date = qualified.get((asset_id, cusip), (episode_end, when))
        qualified[asset_id, cusip] = (max(previous_end, episode_end), min(previous_date, when))
    if len(qualified) != 1:
        return None
    (asset_id, cusip), (episode_end, departure_date) = next(iter(qualified.items()))
    return TerminalTradedEpisode(
        start,
        episode_end,
        asset_id,
        cusip,
        gap is not None and end < departure_date,
        initial_before_reuse_gap=initial,
    )
