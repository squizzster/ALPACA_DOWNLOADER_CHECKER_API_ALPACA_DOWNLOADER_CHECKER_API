"""Select the last reported SIP trade across an observed cash retirement boundary."""

from copy import copy
from datetime import date
from decimal import Decimal, InvalidOperation

from libs.alpaca_data_api_solver import NEW_YORK, instant, valid_cusip
from libs.alpaca_end_of_day_security_correlation.action_identity import (
    exact_cusip_lookup_asset,
    exact_symbol_lookup_asset,
)

from .trade_tape import _last_complete_day, _same_generation, _same_pricing_mapping

FIELD = "terminal_cash_merger_trade"


def _dated(value):
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _listed(own):
    return (
        own
        and own.get("class") == "us_equity"
        and own.get("status") == "inactive"
        and isinstance(own.get("exchange"), str)
        and own["exchange"].strip()
        and own["exchange"].strip().upper() != "OTC"
    )


def _competing_occupant(evidence, cusip, first, last):
    rivals = [item for item in evidence.get("input_symbol_occupants", ()) if item.get("cusip") != cusip]
    if not rivals:
        return False
    dated = {
        (a.get("action_type"), a.get("id")): a.get("effective_date") or a.get("ex_date") or a.get("process_date")
        for a in evidence["corporate_actions"]
    }
    for rival in rivals:
        dates = [
            dated.get((item.get("action_type"), item.get("action_id"))) or item.get("process_date")
            for item in rival.get("evidence", ())
        ]
        # A different issue seen only in a disjoint epoch does not compete
        # with the native target. Undated or bracketing observations do.
        if not dates or not all(_dated(day) for day in dates) or (min(dates) <= last and max(dates) >= first):
            return True
    return False


def _terms(evidence, own, action, first, day):
    cusip = action.get("acquiree_cusip")
    if (
        action.get("action_type") != "cash_merger"
        or not isinstance(action.get("id"), str)
        or not action["id"]
        or not valid_cusip(cusip)
        or own.get("cusip") not in (None, "", cusip)
        or _competing_occupant(evidence, cusip, first, day)
    ):
        return None
    try:
        rate = Decimal(str(action.get("rate")))
    except InvalidOperation:
        return None
    if not rate.is_finite() or rate <= 0:
        return None
    endpoint = exact_cusip_lookup_asset(evidence, cusip)
    if not endpoint or endpoint["id"] != own["id"]:
        return None
    return {
        "action_id": action["id"],
        "role": "acquiree",
        "symbol": action["acquiree_symbol"],
        "cusip": cusip,
        "effective_date": day,
        **({"process_date": action["process_date"]} if action.get("process_date") else {}),
        "rate": action["rate"],
    }


def _acquiree_mergers(evidence, symbol):
    return [
        a
        for a in evidence.get("corporate_actions", ())
        if a.get("action_type") in {"cash_merger", "stock_and_cash_merger", "stock_merger"}
        and a.get("acquiree_symbol") == symbol
    ]


def carry_terminal_cash_trade(view, evidence, window=None, *, previous_generation=None):
    """Preserve the original dated observation only while its selected bindings apply."""
    prior = evidence.get(FIELD)
    if not prior:
        return evidence
    symbol, owner = view.symbol, prior["observed_symbol_asset_id"]
    request = view.source["latest_request"]
    if window is not None and window.previous:
        previous_generation = window.previous.get("source_sha256")
    applies = (
        symbol == prior["symbol"]
        and _same_pricing_mapping(prior, request, owner)
        and (
            view.source_sha256 == prior["pricing_generation"]
            or _same_generation(
                view, previous_generation or prior["pricing_generation"], prior["through_session"]
            )
        )
        and _last_complete_day(request["end_exclusive"]).isoformat() >= prior["through_session"]
        and instant(view.source["history_start"]) <= instant(prior["session"])
    )
    if applies and window is not None:
        own = (window.consumed_symbols.get(symbol) or {}).get("asset") or {}
        terms = prior["cash_merger"]
        applies = (
            _listed(own)
            and (own.get("symbol"), own.get("id")) == (symbol, owner)
            and own.get("cusip") in (None, "", terms["cusip"])
        )
        if applies and terms["cusip"] in window.client.assets:
            observed = window.client.observed_uuid_lookup(terms["cusip"]) or {}
            applies = (observed.get("asset") or {}).get("id") == owner
        if applies and (
            window.start is None or (terms.get("process_date") or terms["effective_date"]) >= window.start
        ):
            # In a replaced native action window the provider can withdraw or
            # amend the original event. Consume the held update, including absence.
            original = window.completed_actions.get(("cash_merger", terms["action_id"]))
            applies = original is not None and original in evidence["corporate_actions"]
        if applies and window.changed_cusips:
            occupants = {item["cusip"] for item in evidence.get("input_symbol_occupants", ())}
            if window.changed_cusips & occupants:
                endings = _acquiree_mergers(evidence, symbol)
                applies = not any(
                    a.get("effective_date") == terms["effective_date"] and a.get("id") != terms["action_id"]
                    for a in endings
                ) and not _competing_occupant(
                    evidence, terms["cusip"], prior["session"], prior["through_session"]
                )
    if applies:
        return evidence
    updated = copy(evidence)
    del updated[FIELD]
    return updated


def record_terminal_cash_trade(view, evidence, window):
    """Consume the held raw tail up to its most recent positive daily session."""
    if (
        window.previous is not None
        or FIELD in evidence
        or getattr(view, "accepted_report", None) is not None
        or getattr(view, "materialization_scope", None) != "complete_history"
        or not getattr(view, "source_sha256", None)
    ):
        return
    request = view.source["latest_request"]
    asof = request.get("asof")
    if (request.get("feed"), request.get("timeframe")) != ("sip", "1Day") or not _dated(asof):
        return
    own = exact_symbol_lookup_asset(evidence, view.symbol)
    if not _listed(own):
        return
    endings = _acquiree_mergers(evidence, view.symbol)
    if not any(a["action_type"] == "cash_merger" for a in endings):
        return
    raw = view.views["raw"]
    if len(raw) < 2:
        return
    last = raw[-1]
    if Decimal(last[5]) != 0 or Decimal(last[6]) != 0:
        return
    day = instant(last[0]).astimezone(NEW_YORK).date().isoformat()
    if asof < day or day > _last_complete_day(request["end_exclusive"]).isoformat():
        return
    candidates = [a for a in endings if a.get("effective_date") == day]
    if len(candidates) != 1 or candidates[0]["action_type"] != "cash_merger":
        return
    for offset in range(2, len(raw) + 1):
        before = raw[-offset]
        volume, count = Decimal(before[5]), Decimal(before[6])
        if volume > 0 and count > 0:
            break
        if volume != 0 or count != 0:
            return
    else:
        return
    first = instant(before[0]).astimezone(NEW_YORK).date().isoformat()
    if instant(view.source["history_start"]) > instant(first):
        return
    terms = _terms(evidence, own, candidates[0], first, day)
    if not terms:
        return
    sample = window.client.get_trade_tape_sample(
        view.symbol, first, asof=asof, mapping_asset_id=own["id"], feed="sip", through_session=day, sort="desc"
    )
    if sample is None:
        return
    evidence[FIELD] = {
        **sample,
        "observed_symbol_asset_id": own["id"],
        "pricing_generation": view.source_sha256,
        "pricing_route_binding": {
            "observed_symbol_asset_id": own["id"],
            "end_exclusive": request["end_exclusive"],
            "current_label": asof == request["end_exclusive"][:10],
        },
        "cash_merger": terms,
    }
