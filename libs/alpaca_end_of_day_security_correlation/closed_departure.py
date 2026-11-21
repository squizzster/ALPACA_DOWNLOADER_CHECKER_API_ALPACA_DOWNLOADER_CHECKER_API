"""Bound current pricing ownership when a separate claimant is historical."""

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass, replace
from datetime import date
from decimal import Decimal

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import (
    expected_market_session_coverage,
    market_sessions_between,
)

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .dated_current_anchor import current_anchor_bounds
from .name_change import (
    _action_dates,
    _bridge_structural_conflict,
    _complete_action_query_covers,
    _known_owners,
    _positive_decimal,
    _traded_price,
    _unknown_occupancy_conflict,
    action_labels,
)
from .observed_current_session import observed_session
from .resumed_current_source import resumed_start
from .terminal_episode import (
    has_informative_trading,
    has_prolonged_carry_forward,
    last_prolonged_carry_forward_end,
)
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES

CORRELATION_VERSION = 80
TERMINAL_DEPARTURE_VERSION = 96
PRIOR_CLAIM_VERSION = 97
SPLIT_SUFFIX_VERSION = 98
DATED_CURRENT_ANCHOR_VERSION = 99
RESUMED_CURRENT_SOURCE_VERSION = 99
OBSERVED_CURRENT_SESSION_VERSION = 100
SPLIT_CONTINUITY_VERSION = 102
METHOD = "bounded_current_source_after_closed_departure"
PRIOR_CLAIM_METHOD = "bounded_current_source_after_historical_claim"
SPLIT_SUFFIX_METHOD = "bounded_current_source_after_observed_split"
SPLIT_CONTINUITY_METHOD = "continuous_pre_split_common_stock_history"
OBSERVED_CURRENT_SESSION_METHOD = "current_source_session_at_exact_catalogue_observation"
DATED_CURRENT_ANCHOR_METHOD = "bounded_current_source_after_own_action"


@dataclass(frozen=True)
class BoundedSourceOwnership:
    symbol: str
    asset_id: str
    first: str
    last: str

    def contains(self, session):
        return self.first <= session.isoformat() <= self.last

    def to_report(self):
        return {"version": self.version, **asdict(self)}

    def method_for_session(self, _session):
        return self.method


@dataclass(frozen=True)
class CurrentSourceOwnership(BoundedSourceOwnership):
    version = 1
    method = METHOD
    departed_asset_id: str
    departure_action_id: str
    departure_date: str


@dataclass(frozen=True)
class PriorClaimSourceOwnership(BoundedSourceOwnership):
    version = 2
    method = PRIOR_CLAIM_METHOD
    prior_asset_id: str
    latest_prior_activity_date: str


@dataclass(frozen=True)
class SplitSuffixOwnership(BoundedSourceOwnership):
    version = 3
    method = SPLIT_SUFFIX_METHOD
    prior_asset_id: str
    split_action_id: str


@dataclass(frozen=True)
class SplitContinuityOwnership(SplitSuffixOwnership):
    version = 6
    split_date: str

    def method_for_session(self, session):
        return SPLIT_CONTINUITY_METHOD if session.isoformat() < self.split_date else self.method


@dataclass(frozen=True)
class DatedCurrentAnchorOwnership(BoundedSourceOwnership):
    version = 4
    method = DATED_CURRENT_ANCHOR_METHOD
    cusip: str
    anchor_action_id: str


@dataclass(frozen=True)
class ObservedCurrentSessionOwnership(BoundedSourceOwnership):
    version = 5
    method = OBSERVED_CURRENT_SESSION_METHOD
    observed_at: str


def current_asset(lineage, symbol):
    asset = exact_symbol_lookup_asset(lineage, symbol)
    if asset is None or not (
        asset.get("symbol") == symbol
        and asset.get("class") == "us_equity"
        and asset.get("status") == "active"
        and asset.get("tradable") is True
        and asset.get("exchange") not in (None, "", "OTC")
    ):
        return None
    identities = [item for item in lineage.get("identities", ()) if item.get("id") == asset["id"]]
    if len(identities) != 1 or any(
        identities[0].get(key) != asset.get(key)
        for key in ("symbol", "class", "exchange", "name", "status", "tradable")
    ):
        return None
    return asset


def restore_current_source(accepted):
    # Receipt ingress already accepted the envelope. Carry the exact proof and
    # original first session forward without opening the old source or actions.
    proof = {
        1: CurrentSourceOwnership,
        2: PriorClaimSourceOwnership,
        3: SplitSuffixOwnership,
        4: DatedCurrentAnchorOwnership,
        5: ObservedCurrentSessionOwnership,
        6: SplitContinuityOwnership,
    }[accepted["version"]]
    return proof(**{key: accepted[key] for key in proof.__dataclass_fields__})


def continue_current_source(accepted, lineage, last):
    """Extend a producer-approved source under its accepted append binding."""
    previous = restore_current_source(accepted)
    if last is None or last <= previous.last:
        return previous
    asset = current_asset(lineage, previous.symbol)
    if asset is None or asset["id"] != previous.asset_id:
        return previous
    return replace(previous, last=last)


def _terminal_branch(lineage, intervals, edges, symbol, current):
    """Use completed edges and exact terminal evidence to close former claimants."""
    actions = lineage.get("corporate_actions", ())
    terminals = [
        (interval.identity_key(), interval.end_date_exclusive, rule.supporting_action_ids[0])
        for interval in intervals
        if interval.historical_symbol == symbol and interval.end_date_exclusive is not None
        for rule in interval.applied_rules
        if rule.name == "cash_merger_acquiree_terminal_boundary" and len(rule.supporting_action_ids) == 1
    ]
    for action in actions:
        if action.get("action_type") != "worthless_removal":
            continue
        label, cusip, action_id = (action.get(key) for key in ("symbol", "cusip", "id"))
        if not all(isinstance(value, str) and value for value in (label, cusip, action_id)):
            continue
        if label != symbol and not any(
            edge.successor_symbol == label and edge.predecessor_symbol == symbol for edge in edges
        ):
            continue
        dates = _action_dates(action)
        lookup = exact_cusip_lookup_asset(lineage, cusip)
        if (
            len(dates) != 1
            or lookup is None
            or lookup.get("symbol") != label
            or lookup.get("status") != "inactive"
        ):
            return None
        terminals.append(((label, cusip, lookup["id"]), next(iter(dates)), action_id))
    if len(terminals) != 1:
        return None
    endpoint, terminal, terminal_action_id = terminals[0]
    branch, used = {endpoint}, {terminal_action_id}
    node, before = endpoint, terminal
    while True:
        predecessors = [
            edge
            for edge in edges
            if (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id) == node
            and edge.effective_date < before
        ]
        if not predecessors:
            break
        if len(predecessors) != 1:
            return None
        edge = predecessors[0]
        if edge.edge_type not in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES and not (
            edge.edge_type == "name_change"
            and edge.predecessor_asset_id == edge.successor_asset_id
            and edge.predecessor_cusip == edge.successor_cusip
        ):
            return None
        node = edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id
        if node in branch:
            return None
        branch.add(node)
        used.add(edge.action_id)
        before = edge.effective_date
    labels = {label for label, _, _ in branch}
    owners = {identifier for _, _, identifier in branch}
    retired = {item.get("id") for item in lineage.get("identities", ()) if item.get("status") == "inactive"}
    if symbol not in labels or not owners <= retired or current in owners:
        return None
    for label in labels:
        expected = {identifier for name, _, identifier in branch if name == label}
        if label == symbol:
            expected.add(current)
        if _known_owners(lineage, label) != expected:
            return None
    if any(
        edge.action_id not in used
        and (
            edge.predecessor_symbol in labels
            or edge.successor_symbol in labels
            or edge.predecessor_asset_id in owners | {current}
            or edge.successor_asset_id in owners | {current}
        )
        for edge in edges
    ):
        return None
    if any(
        action.get("id") not in used
        and action_labels(action).intersection(labels)
        and not (
            action.get("action_type") == "cash_dividend"
            and any((label, cusip) == (action.get("symbol"), action.get("cusip")) for label, cusip, _ in branch)
            and (dates := _action_dates(action))
            and max(dates) <= terminal
        )
        for action in actions
    ):
        return None
    return endpoint[2], terminal, terminal_action_id, labels


def _assess_extinct_source(
    symbol, lineage, intervals, edges, source, sessions, current, complete_sources, selected_sources
):
    branch = _terminal_branch(lineage, intervals, edges, symbol, current)
    if branch is None:
        return None
    if selected_sources is None or set(complete_sources) != set(selected_sources):
        return None
    # A competing or mapped peer with bars in the selected span requires the
    # ordinary independent-source proof. A completed empty peer contributes none.
    for name, work in complete_sources.items():
        if name == symbol:
            continue
        if {context["adjustment"] for _, context, _ in work} != set(ADJUSTMENTS) or any(
            context.get("recorded_integrity_failure") is not None
            or bisect_left(dates, sessions[0]) < bisect_right(dates, sessions[-1])
            for _, context, dates in work
        ):
            return None
    departed, terminal, terminal_action_id, labels = branch
    end = date.fromisoformat(source["request_to"][:10])
    if (
        not date.fromisoformat(source["request_from"][:10]) <= terminal < sessions[0]
        or any(
            not _complete_action_query_covers(lineage, label, when)
            for label in labels
            for when in (terminal, end)
        )
        or _unknown_occupancy_conflict(lineage, labels)
    ):
        return None
    expected = expected_market_session_coverage(sessions[0].isoformat(), source["request_to"])
    if tuple(day.isoformat() for day in sessions) != expected.session_dates:
        return None
    return CurrentSourceOwnership(
        symbol,
        current,
        sessions[0].isoformat(),
        sessions[-1].isoformat(),
        departed,
        terminal_action_id,
        terminal.isoformat(),
    )


def _assess_prior_claim_source(symbol, lineage, source, sessions, current, *, resumed_work=None):
    """Allocate the later observed source when every rival claim predates it."""
    actions = lineage.get("corporate_actions", ())
    if any(action.get("action_type") != "cash_dividend" or action.get("symbol") != symbol for action in actions):
        return None
    cusips = {action.get("cusip") for action in actions if action.get("cusip")}
    if len(cusips) != 1:
        return None
    cusip = next(iter(cusips))
    prior = exact_cusip_lookup_asset(lineage, cusip)
    if (
        prior is None
        or prior.get("symbol") != symbol
        or prior.get("status") != "inactive"
        or prior.get("tradable") is not False
        or prior["id"] == current
        or _known_owners(lineage, symbol) != {prior["id"], current}
        or {asset.get("id") for asset in lineage.get("identities", ())} != {prior["id"], current}
        or _unknown_occupancy_conflict(lineage, {symbol})
    ):
        return None
    start, end = (date.fromisoformat(source[key][:10]) for key in ("request_from", "request_to"))
    if not all(_complete_action_query_covers(lineage, symbol, day) for day in (start, end)):
        return None
    exact_ex_dates, latest = set(), start
    for action in actions:
        try:
            ex_date = date.fromisoformat(action["ex_date"])
            last_event = max(
                (
                    ex_date,
                    *(
                        date.fromisoformat(action[key])
                        for key in ("process_date", "effective_date", "payable_date", "record_date")
                        if action.get(key) is not None
                    ),
                )
            )
        except (KeyError, TypeError, ValueError):
            return None
        if action.get("cusip") == cusip:
            if ex_date < start:
                return None
            exact_ex_dates.add(ex_date)
        latest = max(latest, last_event)
    if len(exact_ex_dates) < 2:
        return None
    first = 0 if resumed_work is None else resumed_start(resumed_work, sessions, min(exact_ex_dates), latest)
    if first is None or latest >= sessions[first]:
        return None
    expected = expected_market_session_coverage(sessions[first].isoformat(), source["request_to"])
    if tuple(day.isoformat() for day in sessions[first:]) != expected.session_dates:
        return None
    # Last positive historical activity is evidence about the source scope.
    # It does not define when the old issue ceased or when the current one began.
    return PriorClaimSourceOwnership(
        symbol, current, sessions[first].isoformat(), sessions[-1].isoformat(), prior["id"], latest.isoformat()
    )


def _split_observation(raw, split, position, rate):
    before, after = raw[position - 1 : position + 1]
    adjusted_before, adjusted_after = split[position - 1 : position + 1]
    raw_before, raw_after, split_before, split_after = map(
        _traded_price, (before, after, adjusted_before, adjusted_after)
    )
    if not all((raw_before, raw_after, split_before, split_after)) or raw_after != split_after:
        return False
    return (
        all(
            min(a, b) > 0 and abs(a - b * rate) <= rate * Decimal("0.01")
            for a, b in zip(raw_before, split_before, strict=True)
        )
        and abs(Decimal(str(adjusted_before["v"])) - Decimal(str(before["v"])) * rate) <= 1
        and Decimal(str(adjusted_after["v"])) == Decimal(str(after["v"]))
    )


def _assess_split_suffix(symbol, lineage, source, sessions, current, work_items, informative):
    """Resolve the observed later tape while leaving the vendor-ID cutover undecided."""
    actions = lineage.get("corporate_actions", ())
    splits = [action for action in actions if action.get("action_type") == "forward_split"]
    if not splits or any(
        action.get("symbol") != symbol or action.get("action_type") not in {"forward_split", "cash_dividend"}
        for action in actions
    ):
        return None
    cusip = splits[0].get("cusip")
    if not cusip or any(
        action.get("cusip") != cusip or action.get("old_cusip") or action.get("new_cusip") for action in splits
    ):
        return None
    prior = exact_cusip_lookup_asset(lineage, cusip)
    if (
        prior is None
        or prior.get("status") != "inactive"
        or prior.get("tradable") is not False
        or any(not current.get(key) or current[key] != prior.get(key) for key in ("symbol", "name", "exchange"))
        or _known_owners(lineage, symbol) != {prior["id"], current["id"]}
        or {item.get("id") for item in lineage.get("identities", ())} != {prior["id"], current["id"]}
        or prior["id"] == current["id"]
    ):
        return None

    dated = {}
    for action in splits:
        when = action.get("ex_date")
        old, new = _positive_decimal(action.get("old_rate")), _positive_decimal(action.get("new_rate"))
        if (
            not isinstance(when, str)
            or not action.get("id")
            or action.get("effective_date") not in (None, when)
            or when in dated
            or old is None
            or new is None
            or new <= old
        ):
            return None
        dated[when] = action["id"], new / old
    try:
        day = date.fromisoformat(max(dated))
        start, end = (date.fromisoformat(source[key][:10]) for key in ("request_from", "request_to"))
        rival_dates = [
            date.fromisoformat(action["ex_date"])
            for action in actions
            if action.get("action_type") == "cash_dividend"
        ]
    except (KeyError, TypeError, ValueError):
        return None
    if (
        any(other >= day for other in rival_dates)
        or any(action.get("cusip") not in (None, "", cusip) for action in actions)
        or not all(_complete_action_query_covers(lineage, symbol, when) for when in (start, end))
        or _unknown_occupancy_conflict(lineage, {symbol}, day, end)
    ):
        return None

    position = bisect_left(sessions, day)
    if not 0 < position < len(sessions) - 1 or sessions[position] != day:
        return None
    raw, split = (
        next(download.rows for download, item, _ in work_items if item["adjustment"] == view)
        for view in ("raw", "split")
    )
    action_id, rate = dated[day.isoformat()]
    if not _split_observation(raw, split, position, rate):
        return None
    if not informative and (
        not has_informative_trading(raw[position:]) or has_prolonged_carry_forward(raw[position:])
    ):
        return None
    expected = expected_market_session_coverage(sessions[position - 1].isoformat(), source["request_to"])
    if tuple(session.isoformat() for session in sessions[position - 1 :]) != expected.session_dates:
        return None
    return SplitSuffixOwnership(
        symbol, current["id"], day.isoformat(), sessions[-1].isoformat(), prior["id"], action_id
    )


def extend_split_ownership(suffix, lineage, work_items, *, informative=False):
    """Resolve only the preceding episode using an already corroborated split."""
    splits = [
        action for action in lineage.get("corporate_actions", ()) if action.get("action_type") == "forward_split"
    ]
    if len(splits) != 1 or splits[0].get("id") != suffix.split_action_id:
        return suffix
    cusip = splits[0].get("cusip")
    prior = exact_cusip_lookup_asset(lineage, cusip)
    mapping = lineage.get("mapping_endpoint_observations", {}).get(cusip, {}).get(suffix.symbol)
    if (
        prior is None
        or prior["id"] != suffix.prior_asset_id
        or prior.get("class") != "us_equity"
        or mapping not in (None, suffix.prior_asset_id, suffix.asset_id)
    ):
        return suffix

    rows, sessions = next(
        (download.rows, dates) for download, source, dates in work_items if source["adjustment"] == "raw"
    )
    split = date.fromisoformat(suffix.first)
    stop = bisect_left(sessions, split)
    if not 0 < stop < len(sessions) or sessions[stop] != split:
        return suffix

    # Bound the inference to the uninterrupted episode touching the split. The
    # completed suffix owns the seam and later grid. This selects earlier rows only.
    expected = market_sessions_between(sessions[0].isoformat(), sessions[stop - 1].isoformat())
    first = stop
    for actual, wanted in zip(reversed(sessions[:stop]), reversed(expected)):
        if actual.isoformat() != wanted:
            break
        first -= 1
    if not informative:
        first += last_prolonged_carry_forward_end(rows[first:stop])
    if first == stop or _unknown_occupancy_conflict(lineage, {suffix.symbol}, sessions[first], split):
        return suffix
    return SplitContinuityOwnership(
        suffix.symbol,
        suffix.asset_id,
        sessions[first].isoformat(),
        suffix.last,
        suffix.prior_asset_id,
        suffix.split_action_id,
        suffix.first,
    )


def assess_current_source(
    symbol,
    lineage,
    intervals,
    edges,
    work_items,
    consensus,
    *,
    complete,
    informative,
    evidence_issues,
    complete_sources,
    source_consensus,
    allow_terminal_departure=False,
    allow_prior_claim=False,
    allow_split_suffix=False,
    allow_split_continuity=False,
    allow_resumed_current_source=False,
    allow_dated_current_anchor=False,
    allow_observed_current_session=False,
    selected_sources=None,
):
    """Certify exact observed rows, without dating a new security's birth.

    Pricing supplies the accepted effective mapping and complete selected source.
    Chronology supplies the accepted same-issue transitions and any previously
    established terminal boundary. A complete informative current source must
    lie wholly after the departure, without another possible occupant.
    """
    if not complete or lineage.get("unresolved_cusips") != []:
        return None
    if (
        not consensus.all_authenticated_adjustments_agree
        or consensus.authenticated_adjustment_count != len(ADJUSTMENTS)
        or {source["adjustment"] for _, source, _ in work_items} != set(ADJUSTMENTS)
    ):
        return None
    _, source, sessions = work_items[0]
    if not sessions or any(item[1].get("recorded_integrity_failure") is not None for item in work_items):
        return None
    context_fields = ("requested_symbol", "asof", "request_from", "request_to", "feed", "timeframe")
    if any(any(other.get(key) != source.get(key) for key in context_fields) for _, other, _ in work_items):
        return None
    # An ordinary moving mapping is part of the accepted source contract; a
    # historical/pinned mapping or cropped tail cannot inherit today's lookup.
    if source["requested_symbol"] != symbol or source["asof"] != source["request_to"][:10]:
        return None
    asset = current_asset(lineage, symbol)
    if asset is None:
        return None

    def latest_observed():
        if not allow_observed_current_session or selected_sources is None:
            return None
        last = sessions[-1]
        observed = observed_session(symbol, lineage, source, last, asset["id"], work_items)
        if observed is None:
            return None
        return ObservedCurrentSessionOwnership(symbol, asset["id"], last.isoformat(), last.isoformat(), observed)

    if allow_dated_current_anchor and selected_sources is not None:
        bounds = current_anchor_bounds(
            symbol,
            lineage,
            source,
            sessions,
            asset,
            edges,
            work_items,
            informative,
            evidence_issues,
            complete_sources,
        )
        if bounds is not None:
            return DatedCurrentAnchorOwnership(symbol, asset["id"], *bounds)
    if evidence_issues:
        return latest_observed()
    singleton = (
        selected_sources is not None and set(selected_sources) == {symbol} and not intervals and not edges
    )
    if allow_split_suffix and singleton:
        suffix = _assess_split_suffix(symbol, lineage, source, sessions, asset, work_items, informative)
        if suffix is not None:
            return (
                extend_split_ownership(suffix, lineage, work_items, informative=informative)
                if allow_split_continuity
                else suffix
            )
    if allow_prior_claim and singleton:
        if informative or allow_resumed_current_source:
            prior_claim = _assess_prior_claim_source(
                symbol,
                lineage,
                source,
                sessions,
                asset["id"],
                resumed_work=None if informative else work_items,
            )
            if prior_claim is not None:
                return prior_claim
        return latest_observed()
    if not informative:
        return latest_observed()
    if allow_terminal_departure:
        terminal = _assess_extinct_source(
            symbol,
            lineage,
            intervals,
            edges,
            source,
            sessions,
            asset["id"],
            complete_sources,
            selected_sources,
        )
        if terminal is not None:
            return terminal
    return (
        _assess_closed_rename(
            symbol,
            lineage,
            intervals,
            edges,
            source,
            sessions,
            asset,
            work_items,
            complete_sources,
            source_consensus,
        )
        or latest_observed()
    )


def _assess_closed_rename(
    symbol, lineage, intervals, edges, source, sessions, asset, work_items, complete_sources, source_consensus
):
    own = [item for item in intervals if item.historical_symbol == symbol]
    if len(own) != 1:
        return None
    prior = own[0]
    departure = prior.end_date_exclusive
    if departure is None or prior.asset_id == asset["id"] or sessions[0] <= departure:
        return None
    if date.fromisoformat(source["request_from"][:10]) > departure:
        return None
    departures = [
        edge
        for edge in edges
        if edge.edge_type == "name_change"
        and edge.predecessor_symbol == symbol
        and edge.successor_symbol != symbol
        and edge.predecessor_asset_id == edge.successor_asset_id == prior.asset_id
        and edge.predecessor_cusip == edge.successor_cusip == prior.cusip
        and edge.effective_date == departure
    ]
    if len(departures) != 1:
        return None
    edge = departures[0]
    if any(asset["id"] in (item.predecessor_asset_id, item.successor_asset_id) for item in edges):
        return None
    if _known_owners(lineage, symbol) != {prior.asset_id, asset["id"]}:
        return None
    successor = exact_symbol_lookup_asset(lineage, edge.successor_symbol)
    if successor is None or (successor.get("id"), successor.get("symbol")) != (
        prior.asset_id,
        edge.successor_symbol,
    ):
        return None
    if _known_owners(lineage, edge.successor_symbol) != {prior.asset_id}:
        return None
    successor_intervals = [item for item in intervals if item.historical_symbol == edge.successor_symbol]
    if len(successor_intervals) != 1 or not (
        successor_intervals[0].asset_id == prior.asset_id
        and successor_intervals[0].contains(sessions[0])
        and successor_intervals[0].contains(sessions[-1])
    ):
        return None
    end = date.fromisoformat(source["request_to"][:10])
    labels = {symbol, edge.successor_symbol}
    # This certificate closes exactly this two-identity dependency shape. Its
    # current issue has no lineage edge into the departed issue, so a later peer
    # reconciliation cannot need its closed rows as an ownership donor.
    if set(complete_sources) != labels or {
        item.get("id") for item in lineage.get("identities", ()) if item.get("id")
    } != {prior.asset_id, asset["id"]}:
        return None
    if any(
        not _complete_action_query_covers(lineage, label, when) for label in labels for when in (departure, end)
    ):
        return None
    if _bridge_structural_conflict(lineage, labels, departure, end, edge.action_id):
        return None
    if _unknown_occupancy_conflict(lineage, labels, departure, end):
        return None
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        for occupant in lineage.get(field, ()):
            if occupant.get("symbol") != symbol:
                continue
            for record in occupant.get("evidence", ()):
                if record.get("action_id") == edge.action_id:
                    continue
                dates = _action_dates(record)
                if not dates or (
                    occupant.get("asset_id") != asset["id"] and any(departure <= day <= end for day in dates)
                ):
                    return None
    # This grid is a new ownership prerequisite, not price authentication. The
    # group cache retains its completed result across views and both phases.
    expected = expected_market_session_coverage(sessions[0].isoformat(), source["request_to"])
    if tuple(day.isoformat() for day in sessions) != expected.session_dates:
        return None
    counterpart = complete_sources.get(edge.successor_symbol, ())
    if {item[1]["adjustment"] for item in counterpart} != set(ADJUSTMENTS) or not source_consensus(
        counterpart
    ).all_authenticated_adjustments_agree:
        return None
    for _, context, _ in counterpart:
        if (context["feed"], context["timeframe"]) != (source["feed"], source["timeframe"]):
            return None
        try:
            if not successor_intervals[0].contains(date.fromisoformat(context["asof"])):
                return None
        except (TypeError, ValueError):
            return None
    raw = next(download.rows for download, context, _ in work_items if context["adjustment"] == "raw")
    donor, _, donor_sessions = next(item for item in counterpart if item[1]["adjustment"] == "raw")
    first, stop = bisect_left(donor_sessions, sessions[0]), bisect_right(donor_sessions, sessions[-1])
    if donor_sessions[first:stop] != sessions:
        return None
    donor_rows = donor.rows[first:stop]
    # An exact successor copy does not independently observe the current issue.
    # This is a veto only: numerical differences never identify the new UUID.
    # Inputs are already assembled and accepted; never fetch/reopen a counterpart.
    if all(
        all(
            Decimal(str(left[field])) == Decimal(str(right[field]))
            for field in ("o", "h", "l", "c", "v", "n", "vw")
        )
        for left, right in zip(raw, donor_rows)
    ):
        return None
    return CurrentSourceOwnership(
        symbol,
        asset["id"],
        sessions[0].isoformat(),
        sessions[-1].isoformat(),
        prior.asset_id,
        edge.action_id,
        departure.isoformat(),
    )
