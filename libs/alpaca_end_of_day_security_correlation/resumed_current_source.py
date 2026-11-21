"""Bound the current traded episode after an older claimant and an inert separator."""

from .name_change import _traded_price
from .terminal_episode import has_informative_trading, has_prolonged_carry_forward, shared_inert_bridges


def resumed_start(work, sessions, oldest_claim, latest_claim):
    """Use the already selected source and its prepared all-view separator."""
    bridges = shared_inert_bridges(work)
    if len(bridges) != 1:
        return None
    first, last = bridges[0]
    if not sessions[0] <= oldest_claim <= latest_claim < sessions[first]:
        return None
    start = last + 1
    for download, _, _ in work:
        rows = download.rows
        resumed = rows[start:]
        if (
            _traded_price(rows[first - 1]) is None
            or _traded_price(resumed[0]) is None
            or not has_informative_trading(resumed)
            or has_prolonged_carry_forward(resumed)
        ):
            return None
    return start
