"""Positive dated occupancy evidence, without inferring a listing inception."""

from datetime import date
from decimal import Decimal, InvalidOperation

from .action_identity import exact_cusip_lookup_asset


def _action_anchor(action, symbol, cusip):
    if not action.get("id"):
        return None
    if action.get("action_type") == "cash_dividend":
        if (action.get("symbol"), action.get("cusip")) != (symbol, cusip):
            return None
        effective = date.fromisoformat(action["ex_date"])
        rates = [Decimal(str(action["rate"]))]
    elif action.get("action_type") == "unit_split":
        effective = date.fromisoformat(action["effective_date"])
        rates = [Decimal(str(action[role + "_rate"])) for role in ("old", "new", "alternate")]
        endpoints = [
            (action.get(role + "_symbol"), action.get(role + "_cusip")) for role in ("old", "new", "alternate")
        ]
        if (
            any(not isinstance(value, str) or not value for pair in endpoints for value in pair)
            or len({pair[0] for pair in endpoints}) != 3
            or len({pair[1] for pair in endpoints}) != 3
            or (symbol, cusip) not in endpoints[1:]
        ):
            return None
    else:
        return None
    return effective if all(rate.is_finite() and rate > 0 for rate in rates) else None


def dated_occupancy_anchors(symbol, lineage, intervals):
    """Anchor a bounded episode after a dividend or component-separation event.

    Only the requested-label fallback consumes this evidence. Earlier ownership
    and as-of decisions retain their intervals, including supported older rows.
    Units need not disappear when their components become separately tradable.
    """
    anchors = {}
    for interval in intervals:
        if (
            interval.historical_symbol != symbol
            or interval.start_date_inclusive is not None
            or interval.end_date_exclusive is None
            or sum(item.identity_key() == interval.identity_key() for item in intervals) != 1
        ):
            continue
        for action in lineage.get("corporate_actions", []):
            try:
                effective = _action_anchor(action, symbol, interval.cusip)
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            if effective is None or effective >= interval.end_date_exclusive:
                continue
            if any(
                other != interval
                and other.historical_symbol == symbol
                and (other.end_date_exclusive is None or other.end_date_exclusive > effective)
                and (
                    other.start_date_inclusive is None
                    or other.start_date_inclusive < interval.end_date_exclusive
                )
                for other in intervals
            ):
                continue
            asset = exact_cusip_lookup_asset(lineage, interval.cusip)
            if (
                asset is None
                or asset["id"] != interval.asset_id
                or asset.get("symbol") != symbol
                or asset.get("exchange") in (None, "", "OTC")
                or any(
                    item.get("asset_id") not in (None, interval.asset_id)
                    for item in lineage.get("symbol_occupancy_evidence", [])
                    if (item.get("symbol"), item.get("cusip")) == (symbol, interval.cusip)
                )
            ):
                continue
            anchors[interval.interval_key()] = min(anchors.get(interval.interval_key(), effective), effective)
    return anchors
