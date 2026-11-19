"""Limit a newly selected eight-view history to its supported split-adjustment basis."""

from bisect import bisect_left
from collections import Counter, defaultdict
from decimal import Decimal

from libs.alpaca_end_of_day_security_correlation.mapped_rename_chain import TERMINAL_RENAME_PREFIX
from libs.alpaca_pricing_download.contract import ADJUSTMENTS

INCOMPATIBLE_SPLIT_BASIS = "incompatible_split_adjustment_basis"


def _common_split_scale(samples):
    # A correction to one price or one session cannot establish a basis.
    # Distinct traded sessions must exhibit the same multiplicative change in
    # all four split-adjusted prices. Allow the provider's quoted precision.
    scales = []
    for winner, donor in samples:
        if Decimal(donor[6]) <= 0 or Decimal(winner[6]) <= 0:
            return False
        for current, old in zip(winner[1:5], donor[1:5], strict=True):
            numerator, denominator = Decimal(current), Decimal(old)
            if min(numerator, denominator) <= 0:
                return False
            scales.append(numerator / denominator)
    return (min(scales) > Decimal("1.01") or max(scales) < 1 / Decimal("1.01")) and max(scales) <= min(
        scales
    ) * Decimal("1.001")


def bounded_prefixes(scopes, candidates, labels, direct_market, routes, instant, *, complete_selection):
    """Admit a complete bounded source switch, including its earlier boundary."""
    if not scopes:
        return set()
    counts = Counter(pid for pid, *_ in scopes)
    calendars = defaultdict(list)
    for key in candidates:
        if key[0] in counts:
            calendars[key[0]].append(key)
    excluded = set()
    for (pid, winner, donor, reason), keys in scopes.items():
        newer = routes[pid].get(winner)
        older = routes[pid].get(donor)
        earlier = [key for key in calendars[pid] if key[1] < keys[0][1]]
        following = min((key for key in calendars[pid] if key[1] > keys[-1][1]), default=None)
        qualified = (
            complete_selection
            and counts[pid] == 1
            and newer is not None
            and older is not None
            and instant(newer[0]) > instant(older[0])
            and following is not None
            and set(candidates[following]) == {winner}
            and labels[following] == {winner}
            and len(candidates[following][winner]) == len(ADJUSTMENTS)
            and all(
                not direct_market[key]
                and labels[key] == {donor}
                and set(candidates[key]) == {winner, donor}
                and all(len(bundle) == len(ADJUSTMENTS) for bundle in candidates[key].values())
                for key in keys
            )
        )
        if qualified and earlier:
            qualified = (
                reason == TERMINAL_RENAME_PREFIX
                and len(keys) >= 2
                and all(
                    set(candidates[key]) == {donor}
                    and len(candidates[key][donor]) == len(ADJUSTMENTS)
                    and labels[key] == {donor}
                    for key in earlier
                )
                and all(min(map(Decimal, candidates[key][donor]["raw"][5:7])) > 0 for key in keys[:2])
                and _common_split_scale(
                    (candidates[key][winner]["split"], candidates[key][donor]["split"]) for key in keys[:2]
                )
            )
        if qualified:
            excluded.update(earlier)
        else:
            # The mapped rows still carry their resolved ownership. Keep the
            # incumbent numerical observations if the switch would reopen an
            # earlier boundary or compete with another accepted authority.
            for key in keys:
                candidates[key].pop(winner, None)
    return excluded


def incompatible_prefixes(starts, candidates, choices, labels):
    """Return fresh, sole-source sessions immediately preceding a proven change of basis."""
    qualified = [
        (pid, winner, donor, opening)
        for (pid, winner, donor), opening in starts.items()
        if len(opening) == 2 and all(choices[key] == winner for key in opening)
    ]
    if not qualified:
        return set()
    limits = {}
    for pid, _, _, opening in qualified:
        first = opening[0][1]
        limits[pid] = max(limits.get(pid, first), first)
    calendars = defaultdict(list)
    for pid, instant in candidates:
        if pid in limits and instant < limits[pid]:
            calendars[pid].append(instant)
    for instants in calendars.values():
        instants.sort()
    excluded = set()
    for pid, winner, donor, opening in qualified:
        instants = calendars[pid]
        index = bisect_left(instants, opening[0][1])
        prefix = []
        for position in range(index - 1, -1, -1):
            key = pid, instants[position]
            sources = candidates[key]
            if choices[key] != donor or set(sources) != {donor} or labels[key] != {donor}:
                break
            if sources[donor].keys() != set(ADJUSTMENTS):
                break
            prefix.append(key)
        if not prefix:
            continue
        samples = []
        for key in opening:
            sources = candidates[key]
            if set(sources) != {winner, donor} or any("split" not in bundle for bundle in sources.values()):
                break
            samples.append((sources[winner]["split"], sources[donor]["split"]))
        if len(samples) == 2 and _common_split_scale(samples):
            excluded.update(prefix)
    return excluded
