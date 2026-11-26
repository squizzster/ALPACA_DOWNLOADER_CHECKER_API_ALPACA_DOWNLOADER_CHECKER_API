"""Carry actual selected numerical outcomes from one canonical comparison to serving."""

from bisect import bisect_left
from collections import defaultdict
from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import document_digest
from .source_choice_handoff import produce


def _histories(dimensions, instants, numerical_instants, *, participants=None):
    histories, incomplete = {}, set()
    for key, semantic in dimensions["semantic"].items():
        source, view, stamp = key
        pid = (semantic.get("security_identity") or {}).get("security_id")
        if not pid or semantic.get("branch") != "correlated":
            continue
        observation = dimensions["observation"][key]
        state = observation.get("observation_selection")
        if state not in {"selected", "superseded"}:
            incomplete.add(pid)
            continue
        if stamp not in instants:
            instant = numerical_instants.get(stamp) or timestamp(stamp)
            instants[stamp] = instant, instant.astimezone(NEW_YORK).date().isoformat()
        instant, day = instants[stamp]
        if participants is not None:
            participants[pid, source, instant].add(view)
        if state != "selected":
            continue
        labels = observation.get("observation_tickers") or [semantic.get("historical_symbol")]
        selected = histories.setdefault(pid, {}).setdefault(day, {})
        if view in selected or not all(labels):
            incomplete.add(pid)
            continue
        selected[view] = source, tuple(sorted(labels)), instant
    return {pid: days for pid, days in histories.items() if pid not in incomplete}


def _complete(days):
    # Serving's numerical revision describes one whole winning bundle per day.
    # Mixed winners, missing adjustments and differing label scopes use the
    # ordinary selected serving owner, which retains their own capabilities.
    return all(set(views) == set(ADJUSTMENTS) and len(set(views.values())) == 1 for views in days.values())


def _changed_episodes(ordered, before, after):
    episodes, active = [], None
    for day in ordered:
        labels = after[day]["raw"][1]
        if labels == before[day]["raw"][1]:
            active = None
            continue
        if active is None or active[0] != labels:
            current = [{"symbol": symbol, "start_inclusive": day} for symbol in labels]
            episodes.extend(current)
            active = labels, current
        end = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        for episode in active[1]:
            episode["end_exclusive"] = end
    return episodes


def _held_bundle(row_indexes, source, day):
    bundles, physical = {}, {}
    for view in ADJUSTMENTS:
        rows, dates = row_indexes[source][0][view]
        if id(rows) not in physical:
            # The selected normalized row already establishes this position.
            physical[id(rows)] = rows[bisect_left(dates, day)]
        bundles[view] = {"source": source, "row": physical[id(rows)]}
    return bundles


def capture(
    baseline, candidate, equal_bundles, row_indexes, group, *, numerical_instants=None, label_output=None
):
    if len(group.pricing) < 2:
        return None, None
    instants, participants = {}, defaultdict(set)
    numerical_instants = {} if numerical_instants is None else numerical_instants
    before = _histories(baseline, instants, numerical_instants)
    after = _histories(candidate, instants, numerical_instants, participants=participants)
    sessions, reuse, episodes, membership, sources, bundles = {}, {}, {}, {}, {}, {}
    for pid in sorted(before.keys() & after):
        old, new = before[pid], after[pid]
        if old.keys() != new.keys() or not _complete(old) or not _complete(new):
            continue
        ordered = sorted(new)
        changed = [day for day in ordered if old[day]["raw"][0] != new[day]["raw"][0]]
        contributors = sorted({new[day]["raw"][0] for day in ordered})
        if not changed:
            if label_output is not None and len(contributors) > 1:
                relabelled = _changed_episodes(ordered, old, new)
                if relabelled:
                    for source in contributors:
                        label_output.setdefault(source, {})[pid] = relabelled
            continue
        sessions[pid] = changed
        episodes[pid] = _changed_episodes(ordered, old, new)
        membership[pid] = dict.fromkeys(ADJUSTMENTS, contributors)
        sources[pid] = {day: new[day]["raw"][0] for day in changed}
        reuse[pid], bundles[pid] = [], {}
        for day in changed:
            prior_source, _, prior_instant = old[day]["raw"]
            winner, _, instant = new[day]["raw"]
            if (
                prior_instant == instant
                and equal_bundles.get((pid, instant)) is True
                and participants[pid, prior_source, instant] == set(ADJUSTMENTS)
            ):
                reuse[pid].append(day)
            else:
                bundles[pid][day] = _held_bundle(row_indexes, winner, day)
    if not sessions:
        return None, None
    labels_unchanged = not any(episodes.values())
    revision = {
        "version": 2 if labels_unchanged else 1,
        "previous_receipt_generations": group.receipts,
        "pricing_generations": group.pricing,
        "sessions": sessions,
        "identity_episodes": episodes,
        **({"labels_unchanged": True} if labels_unchanged else {}),
        "selection_unchanged_outside_scope": True,
        "retained_session_grid": True,
        "numeric_reuse_sessions": reuse,
        "source_membership": membership,
        "selected_sources": sources,
    }
    revision["id"] = document_digest(revision)
    return revision, produce(bundles, revision, required_only=True) if any(bundles.values()) else None
