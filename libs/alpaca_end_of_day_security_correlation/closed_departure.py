"""Bound a new current source after an independently established label departure."""

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .action_identity import exact_symbol_lookup_asset
from .name_change import (
    _action_dates,
    _bridge_structural_conflict,
    _complete_action_query_covers,
    _known_owners,
    _unknown_occupancy_conflict,
)

CORRELATION_VERSION = 80
METHOD = "bounded_current_source_after_closed_departure"


@dataclass(frozen=True)
class CurrentSourceOwnership:
    symbol: str
    asset_id: str
    first: str
    last: str
    departed_asset_id: str
    departure_action_id: str
    departure_date: str

    def contains(self, session):
        return self.first <= session.isoformat() <= self.last

    def to_report(self):
        return {"version": 1, **asdict(self)}

    @classmethod
    def from_report(cls, value):
        # Receipt ingress already accepted the envelope. This is projection,
        # never permission to authenticate the original source/evidence again.
        return cls(**{key: value[key] for key in cls.__dataclass_fields__})


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


def continue_current_source(accepted, lineage, last):
    """Extend only a producer-approved prefix/grid with unchanged identity facts.

    The lineage selector supplies this authority after pricing ancestry, request
    continuity, complete append and group evidence compatibility have succeeded.
    It survives a peer's unresolved-history fallback. No historical proof runs.
    Descriptive status changes may stop NEW ownership without revoking old rows.
    """
    previous = CurrentSourceOwnership.from_report(accepted)
    asset = current_asset(lineage, previous.symbol)
    if asset is None or asset["id"] != previous.asset_id or last is None or last <= previous.last:
        return previous
    return CurrentSourceOwnership(**{**asdict(previous), "last": last})


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
):
    """Certify exact observed rows, without dating a new security's birth.

    Pricing supplies the accepted effective mapping and complete selected source;
    chronology supplies an accepted same-UUID/CUSIP departure. These are consumed,
    not recalculated. The new fact is that one complete informative current source
    lies wholly after that closed departure, without another possible occupant.
    """
    if not complete or not informative or evidence_issues or lineage.get("unresolved_cusips") != []:
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
