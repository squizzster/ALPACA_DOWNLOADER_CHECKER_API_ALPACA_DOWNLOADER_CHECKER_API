"""Consume equal-source decisions without repeating numerical validation."""

from .contract import policy


def _applicable_dates(group, source, generation, dates):
    """A real generation change invalidates only its affected dated suffix."""
    ordered = sorted(dates)
    previous = {"pricing_generation": generation}
    if group.preserves_pricing_prefix(source, previous, through=ordered[-1]):
        return set(ordered)
    if len(ordered) == 1 or not group.preserves_pricing_prefix(source, previous, through=ordered[0]):
        return set()
    left, right = 0, len(ordered) - 1
    while right - left > 1:
        middle = (left + right) // 2
        if group.preserves_pricing_prefix(source, previous, through=ordered[middle]):
            left = middle
        else:
            right = middle
    return set(ordered[:right])


def accepted_dates(group, previous, inputs, *, public_id=None, sessions=None):
    if sessions is not None and not sessions:
        return {}
    if not hasattr(group, "numerical_source_equivalences"):
        return {}
    handoffs = group.numerical_source_equivalences()
    frames, seen = [], set()
    for value in handoffs.values():
        for frame in value["decisions"]:
            identities = {
                pid: selected
                for pid, days in frame["identities"].items()
                if (public_id is None or pid == public_id)
                and (
                    selected := {
                        day: value for day, value in days.items() if sessions is None or day in sessions
                    }
                )
            }
            if not identities:
                continue
            key = tuple(sorted(frame["previous_receipt_generations"].items()))
            if key not in seen:
                frames.append({**frame, "identities": identities})
                seen.add(key)
    accepted = {}
    for frame in frames:
        matching = [
            prior
            for prior in previous
            if prior["policy"] == policy()
            and all(
                frame["previous_receipt_generations"].get(source) == token
                for source, token in prior["references"]["lineage"].items()
            )
            and all(
                frame["previous_pricing_generations"].get(source) == token
                for source, token in prior["references"]["pricing"].items()
            )
        ]
        if not matching:
            continue
        records = {}
        for prior in matching:
            history = prior["history_validation"]
            for record in history["prefix_records"]:
                if record["public_id"] in records:
                    raise ValueError("source equality has competing completed serving donors")
                records[record["public_id"]] = record, history
        scopes, dates_by_id = {}, {}
        for days in frame["identities"].values():
            for day, decision in days.items():
                source = decision["selected_source"]
                scopes.setdefault((source, frame["pricing_generations"][source]), set()).add(day)
                for donor in decision["donors"]:
                    source = donor["source"]
                    for field in ("pricing_generations", "previous_pricing_generations"):
                        scopes.setdefault((source, frame[field][source]), set()).add(day)
        applicable = {
            key: _applicable_dates(group, key[0], key[1], dates)
            for key, dates in scopes.items()
            if key[0] in inputs
        }
        for pid, days in frame["identities"].items():
            for day, decision in days.items():
                winner = decision["selected_source"]
                if day not in applicable.get((winner, frame["pricing_generations"][winner]), ()):
                    continue
                for donor in decision["donors"]:
                    found = records.get(donor["public_id"])
                    if found is None:
                        continue
                    record, _history = found
                    source = donor["source"]
                    # The first selection proved exact equality to this donor.
                    # Its accepted dates and clean numerical outcome apply to
                    # every advertised view; ownership is not that guarantee.
                    dates = record.get("session_dates")
                    if dates is None:
                        raise ValueError("equal-source donor needs its owning completed date projection")
                    if donor["public_id"] not in dates_by_id:
                        dates_by_id[donor["public_id"]] = set(dates)
                    if day not in dates_by_id[donor["public_id"]]:
                        continue
                    if record["price_reasons"]:
                        raise ValueError(
                            "bounded serving projection pending: equal-source donor needs its owning dated negative-outcome projection"
                        )
                    if all(
                        day in applicable.get((source, frame[field][source]), ())
                        for field in ("pricing_generations", "previous_pricing_generations")
                    ):
                        accepted.setdefault(pid, set()).add(day)
                        break
    return accepted
