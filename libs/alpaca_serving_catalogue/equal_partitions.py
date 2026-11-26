"""Transport an accepted checkpoint to an exactly equal selected pricing partition."""

from alpaca_end_of_day._history_coverage import coverage_sources, extend_coverage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import policy


def _sections(view, offsets, public_id):
    """Effective partition-relative selections, including physical holes and labels."""
    from .reuse import _identity_prefix

    runs = _identity_prefix(view["ownership"], view["rows"], public_id)
    kinds = ("completed_years", "current_year", "current_month")
    result = {}
    for kind, limit in zip(kinds, (offsets["current_year"], offsets["current_month"], view["rows"])):
        start = offsets[kind]
        sections = []
        for first, end, labels in runs:
            left, right = max(start, first) - start, min(limit, end) - start
            if left < right:
                sections.append((left, right, labels))
        if sections:
            result[kind] = sections
    return result


def _positions(parts):
    selected = []
    for left, right, _ in parts:
        if selected and selected[-1][1] == left:
            selected[-1] = (selected[-1][0], right)
        else:
            selected.append((left, right))
    return selected


def _matched_prefix(record, old, new, offsets, donor, receiver, group):
    from .reuse import project_dated_labels

    pid = record["public_id"]
    kinds = set()
    for mode in ADJUSTMENTS:
        before = _sections(old["views"][mode], offsets[donor][mode], pid)
        after = _sections(new["views"][mode], offsets[receiver][mode], pid)
        if (
            not before
            or before.keys() != after.keys()
            or any(_positions(parts) != _positions(after[kind]) for kind, parts in before.items())
        ):
            return None
        kinds.update(before)
        if mode == "raw":
            raw_before, raw_after = before, after
    if not all(group.equal_pricing_partition(donor, receiver, kind) for kind in sorted(kinds)):
        return None
    projected = record
    if raw_before != raw_after:
        dates = record.get("session_dates")
        if dates is None:
            # The complete raw grid is identical on both sources. An unmarked
            # legacy checkpoint may lift those dates once, independent of prices.
            dates = group.serving_session_dates(pid)
            if (
                not dates
                or len(dates) != record["coverage"]["session_count"]
                or dates[0] != record["first_session"]
                or dates[-1] != record["last_session"]
            ):
                raise ValueError("equal partition legacy date projection differs from its accepted checkpoint")
        episodes, cursor = [], 0
        for parts in raw_after.values():
            for left, right, labels in parts:
                if not labels:
                    return None
                end = cursor + right - left
                episodes.extend(
                    {
                        "symbol": label,
                        "start_inclusive": dates[cursor],
                        "end_exclusive": dates[end] if end < len(dates) else None,
                    }
                    for label in labels
                )
                cursor = end
        if cursor != len(dates):
            raise ValueError("equal partition date projection differs from its selected raw positions")
        projected = project_dated_labels(record, episodes, dates)
    return projected


def select(group, previous, inputs, offsets, completed):
    """Select only an actual whole-donor replacement after ordinary checkpoint reuse."""
    if not hasattr(group, "equal_pricing_partition"):
        return {}
    result = {}
    for prior in previous:
        if prior["policy"] != policy():
            continue
        history = prior["history_validation"]
        for record in history["prefix_records"]:
            pid = record["public_id"]
            if pid in completed or pid in result or record.get("dated_price_token"):
                continue
            contributors = {
                name for view in history["identities"][pid].values() for name in coverage_sources(view)
            }
            if len(contributors) != 1:
                continue
            donor = next(iter(contributors))
            old = history["inputs"][donor]
            if donor not in inputs or inputs[donor]["pricing_generation"] != old["pricing_generation"]:
                continue
            current = {
                name
                for name, source in inputs.items()
                if any(run["owner"][0] == pid for view in source["views"].values() for run in view["ownership"])
            }
            if len(current) != 1 or donor in current:
                continue
            receiver = next(iter(current))
            projected = _matched_prefix(record, old, inputs[receiver], offsets, donor, receiver, group)
            if projected is None:
                continue
            # Every selected position has the same timestamp and all eight prices.
            # Eligibility and metadata still use the new assignments and evidence.
            result[pid] = {
                "record": projected,
                "coverage": {mode: extend_coverage({receiver}) for mode in ADJUSTMENTS},
                "rows": {receiver: {mode: view["rows"] for mode, view in inputs[receiver]["views"].items()}},
                "offsets": offsets,
                "last_session": record["validated_through"],
                "refresh_gap_sources": bool(record["missing_sessions"])
                and (
                    inputs.keys() != history["inputs"].keys()
                    or any(
                        source["pricing_generation"] != inputs[name]["pricing_generation"]
                        for name, source in history["inputs"].items()
                    )
                ),
                "previous_publication": prior["decision_generation"],
            }
    return result
