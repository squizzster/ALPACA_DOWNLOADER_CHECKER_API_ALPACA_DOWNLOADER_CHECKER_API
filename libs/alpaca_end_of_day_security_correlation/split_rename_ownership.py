"""Dated issue continuity through splits between two same-issue ticker changes."""

from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from .name_change import _non_observation_conflict, _traded_price
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES, UuidLineageEdge

VERSION = 107


@dataclass(frozen=True)
class SplitRenameOccupation:
    arrival: UuidLineageEdge
    splits: tuple[UuidLineageEdge, ...]
    departure: UuidLineageEdge
    literal_bounds: tuple[tuple[str, date | None, date | None, bool], ...]

    @property
    def edges(self):
        return (self.arrival, *self.splits, self.departure)

    def literal_windows(self, symbol):
        return tuple((first, stop) for label, first, stop, _ in self.literal_bounds if label == symbol)

    def contradicts_literal(self, symbol, day):
        return any(
            (first is None or first <= day) and (stop is None or day < stop)
            for first, stop in self.literal_windows(symbol)
        )


def literal_source_conflict(occupation, sources):
    """Only positive rows on an unmapped request can contradict a spelling here.

    The completed raw grid is already in memory. A mapped source often crosses
    both renames, so its span supplies no contrary literal observations.
    """
    for symbol, first, stop, forward in occupation.literal_bounds:
        for download, manifest, sessions in sources.get(symbol, ()):
            if manifest["adjustment"] != "raw" or manifest["asof"] != "-":
                continue
            left = bisect_left(sessions, first) if first is not None else 0
            right = bisect_left(sessions, stop) if stop is not None else len(sessions)
            indexes = range(left, right) if forward else range(right - 1, left - 1, -1)
            if any(_traded_price(download.rows[i]) is not None for i in indexes):
                return True
    return False


def contrary_literal_trades(occupation, lineage):
    """Yield dated contrary prints from completed literal trade observations."""
    for field, search_field in (
        ("historical_trade_tape", "last_trade_search"),
        ("otc_historical_trade_tape", "following_trade_search"),
    ):
        point = lineage.get(field) or {}
        search = point.get(search_field) or {}
        samples = [(point, None, None)]
        if search.get("outcome") in ("observed", "unavailable"):
            for key in ("last_request", "observation"):
                if sample := search.get(key):
                    samples.append((sample, search.get("from_session"), search.get("through_session")))
        for sample, first, last in samples:
            if (sample.get("asof"), sample.get("outcome"), sample.get("http_status")) != ("-", "observed", 200):
                continue
            observed = sample.get("observed_session") or sample["session"]
            if (first and observed < first) or (last and observed > last):
                continue
            day = date.fromisoformat(observed)
            if occupation.contradicts_literal(sample["symbol"], day):
                yield sample["symbol"], day


def literal_trade_conflict(occupation, lineage):
    return next(contrary_literal_trades(occupation, lineage), None) is not None


def _native_edge(value):
    """Use the original native edge or its completed receipt representation."""
    if isinstance(value, UuidLineageEdge):
        return value if value.action_id and not value.source_fact_id else None
    if (
        value.get("edge_type") not in ("name_change", *IDENTITY_PRESERVING_SPLIT_EDGE_TYPES)
        or not value.get("corporate_action_id")
        or value.get("sourced_fact_id")
    ):
        return None
    old, new = value["predecessor"], value["successor"]
    return UuidLineageEdge(
        value["edge_type"],
        value["corporate_action_id"],
        date.fromisoformat(value["effective_date"]),
        old["asset_id"],
        old["symbol"],
        old["cusip"],
        new["asset_id"],
        new["symbol"],
        new["cusip"],
    )


def _closed_rename_pairs(symbol, arrivals, departures):
    """A separate entry after a departure can introduce another occupation."""
    events = sorted((*arrivals[symbol], *departures[symbol]), key=lambda edge: edge.effective_date)
    for i, (arrival, departure) in enumerate(zip(events, events[1:])):
        if arrival.successor_symbol != symbol or departure.predecessor_symbol != symbol:
            continue
        if i and (
            events[i - 1].successor_symbol == symbol or events[i - 1].effective_date >= arrival.effective_date
        ):
            continue
        if i + 2 < len(events) and (
            events[i + 2].predecessor_symbol == symbol
            or events[i + 2].effective_date <= departure.effective_date
        ):
            continue
        yield arrival, departure


def _literal_bounds(path, arrivals, departures):
    """Only dispute this issue's handover, before another dated occupation intervenes."""
    owners = {edge.successor_asset_id for edge in path}
    spans = []
    for edge in (path[0], path[-1]):
        old, new = edge.predecessor_symbol, edge.successor_symbol
        later_entries = [
            item.effective_date
            for item in arrivals[old]
            if item.effective_date > edge.effective_date and item.successor_asset_id not in owners
        ]
        earlier_exits = [
            item.effective_date
            for item in departures[new]
            if item.effective_date < edge.effective_date and item.predecessor_asset_id not in owners
        ]
        if edge == path[0]:
            later_entries.append(path[1].effective_date)  # The next split positively names the new label.
        else:
            earlier_exits.append(path[-2].effective_date)  # This issue started at the last split.
        spans.append((old, edge.effective_date, min(later_entries, default=None), True))
        spans.append((new, max(earlier_exits, default=None), edge.effective_date, False))
    return tuple(spans)


def occupation_conflict(occupation, lineage):
    """Compare the path with other native claims inside its actual dates."""
    arrival, departure = occupation.arrival, occupation.departure
    owners = {(edge.successor_asset_id, edge.successor_cusip) for edge in (arrival, *occupation.splits)}
    actions = {edge.action_id for edge in occupation.edges}
    if _non_observation_conflict(
        lineage, arrival.successor_symbol, arrival.effective_date, departure.effective_date, owners, actions
    ) or _non_observation_conflict(
        lineage,
        departure.successor_symbol,
        departure.effective_date,
        departure.effective_date,
        {(departure.successor_asset_id, departure.successor_cusip)},
        actions,
    ):
        return "split_rename_competing_native_occupation"
    if literal_trade_conflict(occupation, lineage):
        return "split_rename_contrary_literal_trading"
    return None


def split_rename_occupations(lineage, edges, *, conflict_reason=None, issues=None):
    """Select unbranched native split paths between named entrance and departure.

    Accepted split edges already establish the two issue endpoints and date. A
    connected UUID component alone cannot place a split inside this occupation.
    """
    arrivals, departures, splits = defaultdict(list), defaultdict(list), defaultdict(list)
    for value in edges:
        edge = _native_edge(value)
        if edge is None:
            continue
        if edge.edge_type == "name_change":
            arrivals[edge.successor_symbol].append(edge)
            departures[edge.predecessor_symbol].append(edge)
        elif edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES:
            splits[edge.predecessor_symbol].append(edge)

    occupations = []
    pairs = (
        (symbol, arrival, departure)
        for symbol in sorted(splits.keys() & arrivals.keys() & departures.keys())
        for arrival, departure in _closed_rename_pairs(symbol, arrivals, departures)
    )
    for symbol, arrival, departure in pairs:
        if len({arrival.predecessor_symbol, symbol, departure.successor_symbol}) != 3 or any(
            rename.predecessor_cusip != rename.successor_cusip
            or rename.predecessor_asset_id != rename.successor_asset_id
            for rename in (arrival, departure)
        ):
            continue
        between = sorted(
            (
                edge
                for edge in splits[symbol]
                if arrival.effective_date < edge.effective_date < departure.effective_date
            ),
            key=lambda edge: edge.effective_date,
        )
        if not between:
            continue
        path = (arrival, *between, departure)
        if any(
            left.effective_date >= right.effective_date
            or (left.successor_asset_id, left.successor_cusip)
            != (right.predecessor_asset_id, right.predecessor_cusip)
            for left, right in zip(path, path[1:])
        ):
            continue
        owners = {(edge.successor_asset_id, edge.successor_cusip) for edge in path[:-1]}
        if len(owners) != len(path) - 1:
            # A recurring issue would require allocating separate episodes.
            continue
        occupation = SplitRenameOccupation(
            arrival, tuple(between), departure, _literal_bounds(path, arrivals, departures)
        )
        reason = (conflict_reason or occupation_conflict)(occupation, lineage)
        if reason:
            if issues is not None:
                issues.append(
                    {
                        "severity": "warning",
                        "reason_code": reason,
                        "symbol": symbol,
                        "corporate_action_ids": [edge.action_id for edge in path],
                        "detail": "The split path cannot establish exclusive ticker dates against competing provider evidence.",
                    }
                )
            continue
        occupations.append(occupation)
    return tuple(occupations)
