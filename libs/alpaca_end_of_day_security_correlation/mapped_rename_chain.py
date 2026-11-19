"""Assign provider-mapped rename history from independently owned original-label observations."""

from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, InvalidOperation

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS

from .action_identity import exact_cusip_lookup_asset
from .errors import SecurityCorrelationError
from .name_change import _complete_action_query_covers, name_change_date
from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval
from .terminal_episode import has_informative_trading, has_prolonged_carry_forward
from .ticker_reuse_gap import DATED_TERMINAL_SUFFIX_RULE_NAME
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES

SPLIT_ADJUSTED_RENAME_PREFIX_VERSION = 92
SPLIT_ADJUSTED_RENAME_TAPE = "split_adjusted_rename_tape_matches_dated_owner"
BOUNDED_RENAME_PREFIX_VERSION = 93
TERMINAL_RENAME_PREFIX = "split_adjusted_rename_matches_dated_terminal_episode"
CLOSED_ENTRY_RENAME_PREFIX = "split_adjusted_rename_from_closed_predecessor_entry"


@dataclass(frozen=True)
class MappedContinuation:
    sessions: frozenset[date]
    interval: TickerOwnershipInterval
    donor_symbol: str
    action_id: str
    split_adjusted_rename: bool = False
    scope_reason: str | None = None

    def contains(self, session: date) -> bool:
        return session in self.sessions


@dataclass(frozen=True)
class PredecessorEntry:
    effective_date: date
    predecessor_asset_id: str
    successor_asset_id: str
    successor_symbol: str
    successor_cusip: str


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


def _rename_route(symbol, lineage, edges, *, bounded):
    """Walk accepted dated renames back from the queried label, rejecting forks and reuse."""
    incoming = defaultdict(list)
    outgoing = defaultdict(list)
    for edge in edges:
        if edge.edge_type == "name_change" and (not bounded or edge.predecessor_symbol != edge.successor_symbol):
            incoming[edge.successor_symbol].append(edge)
            outgoing[edge.predecessor_symbol].append(edge)
    if outgoing[symbol]:
        return (), None
    route = []
    labels = {symbol}
    while incoming[symbol]:
        candidates = incoming[symbol]
        if len(candidates) != 1:
            return (), None
        edge = candidates[0]
        if (
            edge.predecessor_symbol in labels
            or outgoing[edge.predecessor_symbol] != [edge]
            or edge.predecessor_asset_id != edge.successor_asset_id
            or edge.predecessor_cusip != edge.successor_cusip
            or route
            and edge.effective_date >= route[-1].effective_date
        ):
            return (), None
        route.append(edge)
        symbol = edge.predecessor_symbol
        labels.add(symbol)
    accepted = {edge.action_id for edge in route}
    actions = lineage.get("corporate_actions", ())
    unmatched = [
        action
        for action in actions
        if action.get("action_type") == "name_change"
        and (action.get("old_symbol") in labels or action.get("new_symbol") in labels)
        and action.get("id") not in accepted
    ]
    if not unmatched:
        return tuple(reversed(route)), None
    if not bounded or not route or len(unmatched) != 1:
        return (), None
    entry = _predecessor_entry(unmatched[0], route[-1], lineage, actions)
    return (tuple(reversed(route)), entry) if entry else ((), None)


def _predecessor_entry(action, departure, lineage, actions):
    """Date a separate same-label entry without joining the earlier owner."""
    label = departure.predecessor_symbol
    if action.get("old_symbol") != label or action.get("new_symbol") != label:
        return None
    effective, issue = name_change_date(action, actions)
    prior_cusip, entering_cusip = action.get("old_cusip"), action.get("new_cusip")
    if issue or effective >= departure.effective_date or prior_cusip == entering_cusip:
        return None
    prior = exact_cusip_lookup_asset(lineage, prior_cusip)
    entering = exact_cusip_lookup_asset(lineage, entering_cusip)
    if not prior or not entering or prior["id"] == entering["id"]:
        return None
    return PredecessorEntry(effective, prior["id"], entering["id"], label, entering_cusip)


def _connected_handoffs(route, splits):
    """A label's entry and departure must trace the same accepted share lineage."""
    for incoming, outgoing in zip(route, route[1:]):
        endpoint = incoming.successor_asset_id, incoming.successor_cusip
        for edge in splits:
            if (
                edge.predecessor_symbol == incoming.successor_symbol
                and incoming.effective_date < edge.effective_date < outgoing.effective_date
                and endpoint == (edge.predecessor_asset_id, edge.predecessor_cusip)
            ):
                endpoint = edge.successor_asset_id, edge.successor_cusip
        if endpoint != (outgoing.predecessor_asset_id, outgoing.predecessor_cusip):
            return False
    return True


def _donor_interval(segment, symbol, required_members):
    if (
        segment.get("historical_symbol") != symbol
        or segment.get("correlation_method")
        not in ("requested_symbol_dated_uuid_interval", "asof_selected_uuid_lineage_interval")
        or not required_members.issubset(segment["security_identity"]["member_alpaca_asset_ids"])
    ):
        return None
    interval = segment.get("ticker_ownership_interval") or {}
    security = segment["canonical_security"]
    if (
        interval.get("historical_symbol") != symbol
        or interval.get("asset_id") != security["alpaca_asset_id"]
        or interval.get("cusip") != security["cusip"]
        or not interval.get("ownership_interval_rules")
        or interval.get("supporting_sourced_fact_ids")
    ):
        return None
    return TickerOwnershipInterval(
        symbol,
        interval["cusip"],
        interval["asset_id"],
        date.fromisoformat(interval["start_date_inclusive"]) if interval["start_date_inclusive"] else None,
        date.fromisoformat(interval["end_date_exclusive"]) if interval["end_date_exclusive"] else None,
        tuple(
            AppliedOwnershipIntervalRule(
                rule["order"], rule["name"], tuple(rule["supporting_corporate_action_ids"])
            )
            for rule in interval["ownership_interval_rules"]
        ),
    )


def _terminal_donor_interval(segment, interval, required_members):
    security = segment["canonical_security"]
    if (
        segment.get("correlation_method") == DATED_TERMINAL_SUFFIX_RULE_NAME
        and segment.get("historical_symbol") == interval.historical_symbol
        and security["alpaca_asset_id"] == interval.asset_id
        and security["cusip"] == interval.cusip
        and required_members.issubset(segment["security_identity"]["member_alpaca_asset_ids"])
        and not segment.get("supporting_sourced_fact_ids")
    ):
        return interval
    return None


def _terminal_scope(report, raw_file, edge, intervals, stop, required_members):
    """Use the donor's completed terminal run and the matching rename interval."""
    matching = [
        interval
        for interval in intervals
        if interval.identity_key()
        == (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        and interval.end_date_exclusive == edge.effective_date
        and any(edge.action_id in rule.supporting_action_ids for rule in interval.applied_rules)
        and not any(rule.supporting_fact_ids for rule in interval.applied_rules)
    ]
    if len(matching) != 1:
        return None
    interval = matching[0]
    runs = [
        segment
        for segment in report.correlated["segments"]
        if segment["data_file"] == raw_file and _terminal_donor_interval(segment, interval, required_members)
    ]
    if (
        not runs
        or runs[-1]["row_end"] != stop
        or any(left["row_end"] + 1 != right["row_start"] for left, right in zip(runs, runs[1:]))
    ):
        return None
    first = date.fromisoformat(runs[0]["first_session_date"])
    if not interval.contains(first) or any(
        other.historical_symbol == interval.historical_symbol
        and other.identity_key() != interval.identity_key()
        and other.start_date_inclusive is not None
        and other.start_date_inclusive < edge.effective_date
        and (other.end_date_exclusive is None or first < other.end_date_exclusive)
        for other in intervals
    ):
        return None
    return runs[0]["row_start"] - 1, replace(interval, start_date_inclusive=first)


def _donor_owners(symbol, report, mode_sources, lower, upper, days, required_members, *, terminal=None):
    """Consume each accepted all-view ownership run over the matched raw row positions."""
    by_file = {s["data_file"]: (mode, s) for mode, s in mode_sources.items()}
    scope = None
    positions = {mode: [None] * len(days) for mode in mode_sources}
    for segment in report.correlated["segments"]:
        if segment["data_file"] not in by_file:
            continue
        left = max(lower, segment["row_start"] - 1)
        right = min(upper, segment["row_end"])
        if left >= right:
            continue
        mode, descriptor = by_file[segment["data_file"]]
        binding = tuple(segment.get(field) for field in ("asof", "request_from", "request_to"))
        if (
            None in binding
            or segment.get("requested_symbol") != symbol
            or scope is not None
            and scope != binding
        ):
            return ()
        scope = binding
        interval = (
            _donor_interval(segment, symbol, required_members)
            if terminal is None
            else _terminal_donor_interval(segment, terminal, required_members)
        )
        if (
            not interval
            or segment["download_sha256"] != descriptor["download_sha256"]
            or not interval.contains(days[left - lower])
            or not interval.contains(days[right - lower - 1])
        ):
            return ()
        value = segment["canonical_security"]["security_id"], interval
        owners = positions[mode]
        if any(owners[left - lower : right - lower]):
            return ()
        owners[left - lower : right - lower] = [value] * (right - left)
    raw = positions["raw"]
    return raw if all(raw) and all(items == raw for items in positions.values()) else ()


def _same_transaction(left, right):
    try:
        return left["t"] == right["t"] and all(
            Decimal(str(left[k])) == Decimal(str(right[k])) for k in FIELDS[1:]
        )
    except (InvalidOperation, KeyError):
        return False


def split_adjusted_prefix(
    symbol, lineage, edges, sources, correlation_loader, rows_loader, sessions_loader, *, bounded_intervals=None
):
    """Borrow dated owners when an entire traded prefix matches on a single contemporaneous basis."""
    if not any(e.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES for e in edges):
        return ()
    route, entry = _rename_route(symbol, lineage, edges, bounded=bounded_intervals is not None)
    if not route:
        return ()
    rows, context, dates = sources["raw"]
    last_edge = route[-1]
    try:
        asof = date.fromisoformat(context["asof"][:10])
    except (KeyError, TypeError, ValueError):
        return ()
    prefix_end = bisect_left(dates, last_edge.effective_date)
    lower = bisect_left(dates, entry.effective_date) if entry else 0
    binding = ("requested_symbol", "asof", "feed", "timeframe", "request_from", "request_to")
    if (
        any(any(source[field] != context[field] for field in binding) for _, source, _ in sources.values())
        or not 0 < prefix_end < len(dates)
        or entry
        and (lower >= prefix_end or dates[lower] != entry.effective_date)
        or dates[prefix_end] != last_edge.effective_date
        or asof < last_edge.effective_date
        or not _complete_action_query_covers(lineage, symbol, dates[lower])
        or not _complete_action_query_covers(lineage, symbol, asof)
    ):
        return ()
    labels = {edge.predecessor_symbol for edge in route} | {symbol}
    splits = [
        e
        for e in edges
        if e.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
        and e.predecessor_symbol in labels
        and dates[lower] <= e.effective_date <= asof
    ]
    splits.sort(key=lambda edge: edge.effective_date)
    if (
        not splits
        or not _connected_handoffs((entry, *route) if entry else route, splits)
        or any(e.predecessor_symbol != e.successor_symbol for e in splits)
    ):
        return ()
    # Multiple possible split successors/predecessors cannot identify a share class.
    if len({(e.predecessor_symbol, e.predecessor_asset_id) for e in splits}) != len(splits) or len(
        {(e.successor_symbol, e.successor_asset_id) for e in splits}
    ) != len(splits):
        return ()
    members = _known_label_asset_ids(lineage, symbol)
    members.update(edge.successor_asset_id for edge in route)
    members.update(e.predecessor_asset_id for e in splits)
    members.update(e.successor_asset_id for e in splits)
    if entry:
        if entry.predecessor_asset_id in members:
            return ()
        members.add(entry.successor_asset_id)
    # Collect the whole dependency set before reading donors. An unavailable
    # first-pass report cannot send a retry through a previously matched tape.
    donors = {}
    for edge in route:
        if edge.effective_date <= dates[lower]:
            continue
        try:
            donors[edge.predecessor_symbol] = correlation_loader(edge.predecessor_symbol)
        except SecurityCorrelationError:
            donors[edge.predecessor_symbol] = None
    if not all(donors.values()):
        return ()
    matched = defaultdict(set)
    identities = set()
    scope_reason = CLOSED_ENTRY_RENAME_PREFIX if entry else None
    for edge in route:
        upper = bisect_left(dates, edge.effective_date)
        if lower == upper:
            continue
        donor = edge.predecessor_symbol
        report = donors[donor]
        terminal = None
        try:
            grid = sessions_loader(donor)
            if grid is None:
                return ()
            donor_days, _ = grid
            stop = bisect_left(donor_days, edge.effective_date)
            descriptors = {value["adjustment"]: value for value in report.sources_by_filename.values()}
            if (
                set(descriptors) != set(ADJUSTMENTS)
                or len(descriptors) != len(report.sources_by_filename)
                or any(
                    value["feed"] != context["feed"] or value["timeframe"] != context["timeframe"]
                    for value in descriptors.values()
                )
            ):
                return ()
            scoped = (
                _terminal_scope(report, descriptors["raw"]["data_file"], edge, bounded_intervals, stop, members)
                if bounded_intervals and not entry and len(route) == 1
                else None
            )
            if scoped is not None:
                start, terminal = scoped
                lower = bisect_left(dates, terminal.start_date_inclusive)
                scope_reason = TERMINAL_RENAME_PREFIX
            else:
                start = bisect_left(donor_days, dates[lower])
            days, prefix = dates[lower:upper], rows[lower:upper]
            if (
                donor_days[start:stop] != days
                or not has_informative_trading(prefix)
                or has_prolonged_carry_forward(prefix)
            ):
                return ()
            owners = _donor_owners(donor, report, descriptors, start, stop, days, members, terminal=terminal)
            if not owners:
                return ()
            raw, generation = rows_loader(donor, descriptors["raw"]["data_file"])
        except SecurityCorrelationError:
            return ()
        if generation != descriptors["raw"]["download_sha256"] or len(raw) != len(donor_days):
            return ()
        for day, (public_id, interval), source_row, donor_row in zip(
            days, owners, prefix, raw[start:stop], strict=True
        ):
            if not _same_transaction(source_row, donor_row):
                return ()
            identities.add(public_id)
            matched[interval, donor, edge.action_id].add(day)
        lower = upper
    if len(identities) != 1:
        return ()
    return tuple(
        MappedContinuation(frozenset(days), *key, split_adjusted_rename=True, scope_reason=scope_reason)
        for key, days in matched.items()
    )
