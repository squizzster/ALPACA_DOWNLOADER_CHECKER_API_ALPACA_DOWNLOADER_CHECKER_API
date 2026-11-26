"""Restore the already accepted source positions across a processing-only rename."""

from dataclasses import dataclass
from datetime import date

from libs.alpaca_end_of_day_security_correlation.name_change import (
    closed_successor_rename,
    closed_successor_rename_conflict,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.common.us_equity_market_session_calendar import market_sessions_between, next_market_session_after

from . import storage
from .accepted_owner_recovery import COLD, _restore_prefix, approved_identity, restore_positions
from .assignment_handoff import selected
from .contract import LineageError
from .duplicate_assignments import UnsupportedOverlap, bind_retained_labels
from .prepared_recovery import PreparedCandidate
from .source_work import assignment_documents

RULE = "retained_processing_date_successor_ownership"


@dataclass(frozen=True)
class SelectedSeam:
    public_id: str
    checkpoint: dict
    ranges: dict
    predecessor: str
    successor: str
    process: str
    extents: tuple
    expected_views: dict


def _lost_extents(group, offline, pid):
    losses = sorted(offline["offline_ownership"]["outcome"]["lost_scopes"], key=lambda item: item["row_start"])
    if not losses:
        raise LineageError("processing-date restoration requires one lost successor prefix")
    successor = losses[0]["source"]
    extents = []
    for loss in losses:
        if (
            successor not in group.pricing
            or loss["source"] != successor
            or loss["public_id"] != pid
            or loss["boundary"] != COLD
            or loss["previous_pricing_generation"] != group.pricing[successor]
            or loss["pricing_generation"] != group.pricing[successor]
            or loss["row_start"] != (extents[-1][1] + 1 if extents else 1)
            or loss["row_end"] < loss["row_start"]
            or extents
            and next_market_session_after(extents[-1][3]) != loss["segment_from"]
        ):
            raise LineageError("loss is outside the unchanged successor source generation or first positions")
        extents.append((loss["row_start"], loss["row_end"], loss["segment_from"], loss["segment_to"]))
    return successor, tuple(extents)


def _expected_grid(group, prior, pid, labels, successor, stop):
    """Project the sole permitted difference from the already accepted full group."""
    history = prior["history_validation"]
    if prior["references"]["pricing"] != group.pricing or history["identities"][pid] != {
        view: {"version": 2, "sources": sorted(labels)} for view in ADJUSTMENTS
    }:
        raise LineageError("the selected pricing generations or numerical contributors changed")
    size = 0
    expected_views = {}
    for symbol, source in history["inputs"].items():
        accepted = source["views"]
        selected = accepted["raw"]
        rows = selected["rows"]
        participating = symbol in labels
        owner = [pid, [symbol]] if participating else [None, None]
        if (
            accepted != dict.fromkeys(ADJUSTMENTS, selected)
            or selected["ownership"] != ([{"owner": owner, "rows": rows}] if rows else [])
            or (participating and (not rows or selected["last_session"] != labels[symbol]["last_session"]))
        ):
            raise LineageError("accepted sources do not have disjoint complete all-view owner and label runs")
        if participating:
            size += rows
        expected = selected
        if symbol == successor:
            if not 0 < stop < rows:
                raise LineageError("successor loss does not leave an accepted continuing owner")
            expected = {
                **selected,
                "ownership": [
                    {"owner": [None, None], "rows": stop},
                    {"owner": owner, "rows": rows - stop},
                ],
            }
        expected_views[symbol] = dict.fromkeys(ADJUSTMENTS, expected)
    if size != history["prefix_records"][0]["coverage"]["session_count"]:
        raise LineageError("accepted source positions do not exhaust the numerical checkpoint")
    return expected_views


def _request_covers(receipt, predecessor_last, process):
    request = receipt.get("pricing_request") or {}
    start = request.get("history_start") or request.get("start")
    end = request.get("end_exclusive")
    return (
        request.get("feed") == "sip"
        and request.get("timeframe") == "1Day"
        and bool(request.get("asof"))
        and isinstance(start, str)
        and isinstance(end, str)
        and start[:10] <= predecessor_last < process < end[:10]
    )


def _native_route(previous, pid, predecessor, successor, predecessor_last, process):
    first, processed = date.fromisoformat(predecessor_last), date.fromisoformat(process)
    route = closed_successor_rename(
        provider_evidence(previous[successor]["evidence"]), predecessor, successor, first, processed
    )
    if route is None or route.terminal_symbol not in previous:
        raise LineageError("missing native same-issue processing-only arrival and later departure")
    for symbol, receipt in previous.items():
        memberships = receipt["identity_memberships"]
        if symbol in (predecessor, successor) and len(memberships) != 1:
            raise LineageError("the selected rename lacks a single continuing identity")
        if any(
            identity["security_id"] != pid or identity["member_alpaca_asset_ids"] != [route.asset_id]
            for identity in memberships
        ):
            raise LineageError("a competing issue participates in the selected group's assignments")
        if symbol != successor and closed_successor_rename_conflict(
            provider_evidence(receipt["evidence"]), predecessor, successor, route, first, processed
        ):
            raise LineageError("another selected source has competing native evidence for the rename")
    return route


def _peer_assignments(root, previous, labels, pid, first, last):
    """Inspect only possible rivals in the lost interval, keeping each opened projection for serving."""
    documents = {symbol: {} for symbol in previous}
    for symbol in previous.keys() - labels.keys():
        receipt = previous[symbol]
        through = receipt["serving_ownership"]["views"]["raw"]["last_session"]
        if through is None or through < first:
            continue
        anchor = receipt["anchor_month"]
        kinds = set()
        if first[:4] < anchor[:4]:
            kinds.add(COLD)
        if first[:7] < anchor and last[:4] >= anchor[:4]:
            kinds.add("current_year")
        if last[:7] >= anchor:
            kinds.add("current_month")
        for kind in sorted(kinds):
            handoff = selected(receipt, kind) or {}
            choices = handoff.get("observation_choices")
            if choices:
                uncertain = False
                for partition in choices["partitions"]:
                    for run in partition["runs"]:
                        if run["from"] > last or run["to"] < first:
                            continue
                        if run["state"] == "superseded" and run["owner"] == pid:
                            continue
                        if run["state"] != "unassigned":
                            raise LineageError("a peer has a competing observation choice on the lost dates")
                        uncertain = True
                if not uncertain:
                    continue
            # Choice runs describe numerical selection. Only the assignment
            # document records the candidate identities of an unassigned row.
            body = (
                storage.read_assignment_projection(root, receipt, kind, handoff)
                if handoff
                else storage.read_boundaries(root, receipt, kinds={kind})[kind]
            )
            documents[symbol][kind] = body
            visited = set()
            for view in body["adjustments"]:
                segments = view["segments"]
                if id(segments) in visited:
                    continue
                visited.add(id(segments))
                for seg in segments:
                    if seg["from"] > last or seg["to"] < first:
                        continue
                    if seg["branch"] == "uncorrelated" and not seg.get("candidate_securities"):
                        continue
                    if (
                        seg.get("observation_selection") == "superseded"
                        and (seg.get("security_identity") or {}).get("security_id") == pid
                    ):
                        continue
                    raise LineageError("an overlapping peer has a competing candidate on the lost dates")
    return documents


def select(group, offline):
    """Complete the serving-checkpoint screen before selecting any sources or lineage receipts."""
    pid, prior = approved_identity(group, offline)
    record = prior["history_validation"]["prefix_records"][0]
    ranges = sorted(record["ticker_ranges"], key=lambda item: item["first_session"])
    labels = {item["ticker"]: item for item in ranges}
    successor, extents = _lost_extents(group, offline, pid)
    if (
        not labels.keys() <= group.pricing.keys()
        or len(ranges) != len(labels)
        or successor not in labels
        or ranges[0]["ticker"] == successor
        or any(left["last_session"] >= right["first_session"] for left, right in zip(ranges, ranges[1:]))
        or record["price_reasons"]
        or record["missing_sessions"]
    ):
        raise LineageError("restoration requires disjoint nonrecurring sources on a clean accepted session grid")
    new_range = labels[successor]
    old_range = ranges[ranges.index(new_range) - 1]
    predecessor = old_range["ticker"]
    first, last, stop = extents[0][2], extents[-1][3], extents[-1][1]
    previous_last = old_range["last_session"]
    process = next_market_session_after(last)
    if (
        record["first_session"] != ranges[0]["first_session"]
        or record["last_session"] != record["validated_through"]
        or record["last_session"] != ranges[-1]["last_session"]
        or new_range["first_session"] != first
        or next_market_session_after(previous_last) != first
        or process is None
        or not first <= last < process <= new_range["last_session"]
        or len(market_sessions_between(first, last)) != stop
    ):
        raise LineageError("the accepted source ranges and lost sessions do not adjoin")
    return SelectedSeam(
        pid,
        prior,
        labels,
        predecessor,
        successor,
        process,
        extents,
        _expected_grid(group, prior, pid, labels, successor, stop),
    )


def prepare(scope, previous, root):
    pid, prior, labels = scope.public_id, scope.checkpoint, scope.ranges
    predecessor, successor, process = scope.predecessor, scope.successor, scope.process
    extents = scope.extents
    first, last = extents[0][2], extents[-1][3]
    previous_last = labels[predecessor]["last_session"]
    if not all(_request_covers(previous[name], previous_last, process) for name in (predecessor, successor)):
        raise LineageError("the source requests do not cover the observed predecessor and successor")
    for symbol, receipt in previous.items():
        if (receipt.get("serving_ownership") or {}).get("views") != scope.expected_views[symbol]:
            raise LineageError("current owner, label or numerical source differs outside the lost prefix")
    route = _native_route(previous, pid, predecessor, successor, previous_last, process)
    if labels[successor]["last_session"] >= route.departure_date.isoformat():
        raise LineageError("the successor's selected history crosses the later native departure")
    assignments = _peer_assignments(root, previous, labels, pid, first, last)
    received = previous[successor]
    boundary = assignment_documents(root, received, {}, kinds={COLD})[COLD]
    ids = [route.arrival_action_id, route.departure_action_id]
    interval = {
        "historical_symbol": successor,
        "cusip": route.cusip,
        "asset_id": route.asset_id,
        "start_date_inclusive": first,
        "end_date_exclusive": process,
        "supporting_corporate_action_ids": ids,
        "ownership_interval_rules": [{"name": RULE, "order": 1, "supporting_corporate_action_ids": ids}],
    }
    document = _restore_prefix(boundary, pid, successor, extents, interval=interval, rule=RULE)
    receipt = restore_positions(received, document, pid, extents, prior)
    receipt["retained_owner_restoration"].update(
        rule=RULE,
        predecessor=predecessor,
        successor=successor,
        process_date=process,
        supporting_corporate_action_ids=ids,
    )
    receipts = {symbol: dict(item) for symbol, item in previous.items()}
    receipts[successor] = receipt
    try:
        bind_retained_labels(receipts, {pid})
    except UnsupportedOverlap as error:
        raise LineageError(str(error)) from error
    boundaries = {symbol: {COLD: document} if symbol == successor else {} for symbol in previous}
    assignments[successor][COLD] = document
    return PreparedCandidate(receipts, boundaries, previous, receipt["correlation_policy"], assignments)
