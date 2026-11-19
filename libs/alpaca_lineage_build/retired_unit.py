"""Bind a numerical correction to the completed terminal ownership of an earlier unit."""

from uuid import UUID

from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.retired_unit import factor
from libs.alpaca_pricing_download.shifted_split import session_window

from .contract import RecoveryRequired
from .dated_price_publication import action_window
from .occupation_storage import FIELD
from .resume import continuing_request


def require_rebuilt_pricing(previous, receipt, source, intervals):
    """Bind the closed prices to this member's held pricing producer handoff."""
    generation = previous["source_sha256"]
    if receipt["source_sha256"] == generation:
        return
    if source is not None and continuing_request(source, previous):
        # The full assignment producer already checked this unit's own price
        # ancestry through current_asof when it applied the terminal occupation.
        through = []
        for interval in intervals:
            occupation = receipt.get(FIELD, {}).get(interval["public_id"], {})
            if (
                occupation.get("symbol") != receipt["symbol"]
                or occupation["current_asof"] < interval["witness_end"]
            ):
                through.append(interval["witness_end"])
        if not through:
            return
        for change in source.get("history_changes", ()):
            if change["source_sha256"] == generation:
                first_changed = change["changed_from"]
                if first_changed is None or first_changed > max(through):
                    return
                break
    raise RecoveryRequired(
        "retired unit historical pricing changed or lacks ancestry; selected pricing recovery required"
    )


def _other_actions(evidence, symbol, first, review, terminal_id):
    # The accepted terminal occupation already establishes the unit separation
    # and the merger of both components. Do not perform this decision again.
    return [
        a
        for a in action_window(evidence, first, review, exclude_id=terminal_id, symbol=symbol)
        if first <= (a.get("ex_date") or a.get("effective_date") or a.get("process_date") or "") <= review
    ]


def inputs(snapshot, security, review, ranges):
    pid = security.public_id
    receipts = snapshot._selected_receipts
    # The group publication installs this handoff on every selected member.
    occupation = next(iter(receipts.values())).get(FIELD, {}).get(pid)
    if not occupation or occupation.get("retained_side") != "prior" or not occupation.get("terminal_action"):
        raise UnsupportedInterval("the selected owner has no completed terminal unit occupation")
    first, last = occupation["prior_first_trade"], occupation["last_prior_trade"]
    symbol = occupation["symbol"]
    terminal = occupation["terminal_action"]
    separation = terminal["effective_date"]
    if (
        ranges != [{"ticker": symbol, "first_session": first, "last_session": last}]
        or not last <= separation < review < occupation["first"]
    ):
        raise UnsupportedInterval(
            "reported share factor is outside the closed interval between the two occupations"
        )
    observed = {}
    for receipt in receipts.values():
        for action in _other_actions(receipt["evidence"], symbol, first, review, terminal["id"]):
            key = action.get("id")
            if not key or key in observed and observed[key] != action:
                raise UnsupportedInterval("conflicting provider unit action observations")
            observed[key] = action
    rate = factor(list(observed.values()), symbol, review)
    plan = dict(
        first=first,
        last=last,
        review=review,
        cut=session_window(last)[2],
        symbol=symbol,
        occupation_token=occupation["token"],
        terminal_action_id=terminal["id"],
        factor=str(rate),
    )
    return {"plan": plan, "views": snapshot.bounded_acquisition_views(pid, first, last)}


def applies(previous, current, interval):
    scope = interval["retired_unit_scope"]
    if current.get(FIELD, {}).get(interval["public_id"], {}).get("token") != scope["occupation_token"]:
        return False
    if previous.get("evidence") is current.get("evidence"):
        return True

    def material(receipt):
        return sorted(
            _other_actions(
                receipt["evidence"],
                scope["symbol"],
                interval["first"],
                interval["witness_end"],
                scope["terminal_action_id"],
            ),
            key=lambda a: a["id"],
        )

    return material(previous) == material(current)


def valid_scope(interval):
    if interval.get("repair_kind") != "retired_unit":
        return "retired_unit_scope" not in interval
    scope = interval["retired_unit_scope"]
    return (
        set(scope) == {"symbol", "occupation_token", "terminal_action_id"}
        and scope["symbol"] in interval["members"]
        and UUID(hex=scope["occupation_token"]).hex == scope["occupation_token"]
        and isinstance(scope["terminal_action_id"], str)
        and bool(scope["terminal_action_id"])
        and interval["cut"] <= interval["witness_end"]
    )
