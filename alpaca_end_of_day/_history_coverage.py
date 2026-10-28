"""Extend a completed source-coverage order without reopening its observations."""


def extend_coverage(coverage, previous=None):
    # The serving checkpoint already owns the historical subset/overlap facts.
    # Extend them from newly selected observations only; DO NOT reopen retained
    # history to reconstruct the old coverage sets.
    previous = previous or {"subsets": {}, "overlaps": []}
    old = previous["subsets"]
    names = sorted(set(old) | set(coverage))

    def subset(left, right):
        was_subset = left not in old or (right in old and right in old[left])
        return was_subset and coverage.get(left, set()) <= coverage.get(right, set())

    overlaps = {tuple(pair) for pair in previous["overlaps"]}
    overlaps.update(
        (left, right)
        for i, left in enumerate(names)
        for right in names[i + 1 :]
        if coverage.get(left, set()) & coverage.get(right, set())
    )
    return {
        "subsets": {left: [right for right in names if subset(left, right)] for left in names},
        "overlaps": [list(pair) for pair in sorted(overlaps)],
    }


def strict_subset(order, left, right):
    return right in order["subsets"][left] and left not in order["subsets"][right]


def preserves_selection(previous, current):
    return all(
        strict_subset(previous, left, right) == strict_subset(current, left, right)
        and strict_subset(previous, right, left) == strict_subset(current, right, left)
        for left, right in previous["overlaps"]
    )


class SelectedCoverage:
    """The shared resolver needs only strict subset comparisons of coverage."""

    def __init__(self, order, source):
        self.order, self.source = order, source

    def __lt__(self, other):
        return strict_subset(self.order, self.source, other.source)
