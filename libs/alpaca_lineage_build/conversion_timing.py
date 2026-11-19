"""Select the dated price interval identified by a same-label security conversion."""

from libs.alpaca_pricing_download.conversion_timing import plan
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.shifted_split import session_window

from .duplicate_split import _actions


def inputs(snapshot, security, review, ranges):
    selected = [item for item in ranges if item["first_session"] <= review <= item["last_session"]]
    if len(selected) != 1:
        raise UnsupportedInterval("conversion is not inside one selected ticker range")
    item = selected[0]
    symbol, first, through = item["ticker"], item["first_session"], item["last_session"]
    earliest = session_window(review)[0]
    observed = {}
    for receipt in snapshot._selected_receipts.values():
        for action in _actions(receipt["evidence"], symbol, earliest, through):
            key = action.get("id")
            if not key or key in observed and observed[key] != action:
                raise UnsupportedInterval("conflicting provider corporate-action observations")
            observed[key] = action
    info = plan(observed.values(), symbol, first, through, review)
    views = snapshot.bounded_acquisition_views(security.public_id, info["previous"], info["final"])
    return {"plan": info, "views": views}
