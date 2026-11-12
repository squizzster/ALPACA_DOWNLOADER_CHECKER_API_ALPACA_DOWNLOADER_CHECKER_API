"""Reconcile only newly documented temporary-market observation contests."""

from bisect import bisect_left, bisect_right
from collections import defaultdict
from copy import deepcopy
from types import SimpleNamespace

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError
from .observations import select_observations


def select_temporary(
    reports,
    inputs,
    source_names,
    plan,
    previous,
    source_provider,
    *,
    equivalence_output=None,
    baseline_reports=None,
):
    periods = defaultdict(list)
    for _identifier, pid, episodes in plan.bound:
        periods[pid].extend(episode for episode in episodes if episode["market"] == "when_issued")
    if not periods:
        return reports, {}, {}
    potential = defaultdict(set)
    for key, report in reports.items():
        name, kind = source_names[key], key[1]
        for segment in report["correlated"]["segments"]:
            if kind == "current_year" and segment["from"][:7] >= previous[name]["anchor_month"]:
                continue
            pid = segment["security_identity"]["security_id"]
            for number, episode in enumerate(periods[pid]):
                if episode["start_inclusive"] <= segment["to"] and segment["from"] < episode["end_exclusive"]:
                    potential[pid, number].add(name)
    selected_periods = {key for key, names in potential.items() if len(names) > 1}
    if not selected_periods:
        return reports, {}, {}
    records, dates, instants, present, direct = [], {}, {}, defaultdict(set), defaultdict(set)
    baseline = {}
    if baseline_reports is not None:
        for key, report in baseline_reports.items():
            for segment in report["correlated"]["segments"]:
                baseline.setdefault((key, segment["adjustment"]), []).append(segment)

    def accepted_state(key, adjustment, index, segment):
        if baseline_reports is None:
            return segment.get("observation_selection")
        old = next(
            piece for piece in baseline[key, adjustment] if piece["row_start"] - 1 <= index < piece["row_end"]
        )
        return old.get("observation_selection")

    def instant(value):
        if value not in instants:
            instants[value] = timestamp(value)
        return instants[value]

    def day(row):
        value = row[0]
        if value not in dates:
            dates[value] = instant(value).astimezone(NEW_YORK).date().isoformat()
        return dates[value]

    for key, report in reports.items():
        name, kind = source_names[key], key[1]
        for segment in report["correlated"]["segments"]:
            if kind == "current_year" and segment["from"][:7] >= previous[name]["anchor_month"]:
                continue
            pid, adjustment = segment["security_identity"]["security_id"], segment["adjustment"]
            episodes = [
                episode
                for number, episode in enumerate(periods[pid])
                if (pid, number) in selected_periods
                and episode["start_inclusive"] <= segment["to"]
                and segment["from"] < episode["end_exclusive"]
            ]
            if not episodes:
                continue
            rows = inputs[key].views[adjustment]
            positions = set()
            for episode in episodes:
                left = bisect_left(
                    rows, episode["start_inclusive"], lo=segment["row_start"] - 1, hi=segment["row_end"], key=day
                )
                right = bisect_left(rows, episode["end_exclusive"], lo=left, hi=segment["row_end"], key=day)
                positions.update(range(left, right))
            for index in sorted(positions):
                row = rows[index]
                target = pid, instant(row[0])
                present[target].add(name)
                authority = segment.get("observation_authority") or {}
                if authority.get("direct_market_source") is True:
                    direct[target].add(name)
                records.append((name, key, adjustment, index, pid, row, segment))
    contests = {target for target, names in present.items() if len(names) > 1 and direct[target]}
    if not contests:
        return reports, {}, {}
    selected_rows, selected_reports, origins = {}, {}, {}
    aliases = {}
    for name, key, adjustment, index, pid, row, segment in records:
        if (pid, instant(row[0])) not in contests:
            continue
        rows = selected_rows.setdefault(name, {view: [] for view in ADJUSTMENTS})[adjustment]
        rows.append(row)
        local = len(rows)
        selected_reports.setdefault(
            name,
            {
                "correlated": {"segments": [], "row_count": 0},
                "uncorrelated": {"segments": [], "row_count": 0},
                "retained_observation_routes": previous[name].get("observation_routes", {}),
            },
        )
        origins[name, adjustment, local] = (key, index, segment)
    selected_inputs = {}
    for name, views in selected_rows.items():
        for adjustment, rows in views.items():
            ordered = sorted(range(len(rows)), key=lambda number: instant(rows[number][0]))
            before = {number: origins[name, adjustment, number + 1] for number in range(len(rows))}
            views[adjustment] = [rows[number] for number in ordered]
            for number, old_number in enumerate(ordered, 1):
                origins[name, adjustment, number] = before[old_number]
        fragments = []
        for adjustment, rows in views.items():
            for number, row in enumerate(rows, 1):
                _key, _index, segment = origins[name, adjustment, number]
                fragments.append(
                    {
                        **segment,
                        "row_start": number,
                        "row_end": number,
                        "row_count": 1,
                        "first_timestamp": row[0],
                        "last_timestamp": row[0],
                        "from": day(row),
                        "to": day(row),
                    }
                )
        selected_reports[name]["correlated"]["segments"] = fragments
        # Preserve known physical equality among newly assembled bounded views.
        for adjustment, rows in views.items():
            fingerprint = tuple(
                (id(inputs[key].views[adjustment]), index)
                for number in range(1, len(rows) + 1)
                for key, index, _segment in (origins[name, adjustment, number],)
            )
            if fingerprint not in aliases:
                aliases[fingerprint] = tuple(rows)
            views[adjustment] = aliases[fingerprint]
        selected_inputs[name] = SimpleNamespace(
            source=source_provider(name), source_sha256=previous[name]["source_sha256"], views=views
        )
        selected_reports[name]["correlated"]["row_count"] = sum(len(rows) for rows in views.values())
    equivalence = {}
    chosen, corrections = select_observations(
        selected_inputs, selected_reports, instant_keys=instants, equality_outcomes=equivalence
    )
    if corrections:
        raise LineageError("temporary-market selection requires explicit owning mapping reconciliation")
    changes, scopes, bundles, decisions = {}, defaultdict(set), defaultdict(dict), {}
    for name, report in chosen.items():
        for segment in report["correlated"]["segments"]:
            adjustment = segment["adjustment"]
            for position in range(segment["row_start"], segment["row_end"] + 1):
                key, index, original = origins[name, adjustment, position]
                row = selected_inputs[name].views[adjustment][position - 1]
                pid = segment["security_identity"]["security_id"]
                fields = {field: segment[field] for field in ("observation_selection", "observation_tickers")}
                changes.setdefault(key, {}).setdefault(adjustment, {})[index] = fields
                decisions[name, adjustment, instant(row[0])] = fields
                if accepted_state(key, adjustment, index, original) != fields["observation_selection"]:
                    scopes[pid].add(day(row))
                if fields["observation_selection"] == "selected":
                    bundles[pid].setdefault(day(row), {})[adjustment] = {"source": name, "row": row}
    # The year snapshot's physical hot overlap consumes the same completed
    # decisions; it is never another source in the numerical contest.
    for key, report in reports.items():
        name, kind = source_names[key], key[1]
        if kind != "current_year":
            continue
        for segment in report["correlated"]["segments"]:
            if segment["from"][:7] < previous[name]["anchor_month"]:
                continue
            adjustment = segment["adjustment"]
            selected = {
                when: fields
                for (source, view, when), fields in decisions.items()
                if source == name
                and view == adjustment
                and segment["from"] <= when.astimezone(NEW_YORK).date().isoformat() <= segment["to"]
            }
            if not selected:
                continue
            rows = inputs[key].views[adjustment]
            left = bisect_left(
                rows,
                min(selected),
                lo=segment["row_start"] - 1,
                hi=segment["row_end"],
                key=lambda row: instant(row[0]),
            )
            right = bisect_right(
                rows, max(selected), lo=left, hi=segment["row_end"], key=lambda row: instant(row[0])
            )
            for index in range(left, right):
                if instant(rows[index][0]) in selected:
                    changes.setdefault(key, {}).setdefault(adjustment, {})[index] = selected[
                        instant(rows[index][0])
                    ]
    projected = deepcopy(reports)
    for key, by_view in changes.items():
        segments = []
        for segment in reports[key]["correlated"]["segments"]:
            selected = by_view.get(segment["adjustment"], {})
            indexes = sorted(
                index for index in selected if segment["row_start"] - 1 <= index < segment["row_end"]
            )
            if not indexes:
                segments.append(segment)
                continue
            rows = inputs[key].views[segment["adjustment"]]
            cuts = sorted(
                {segment["row_start"] - 1, segment["row_end"]}
                | {bound for index in indexes for bound in (index, index + 1)}
            )
            for first, stop in zip(cuts, cuts[1:]):
                fields = selected.get(first, {}) if stop == first + 1 else {}
                segments.append(
                    {
                        **segment,
                        **fields,
                        "row_start": first + 1,
                        "row_end": stop,
                        "row_count": stop - first,
                        "first_timestamp": rows[first][0],
                        "last_timestamp": rows[stop - 1][0],
                        "from": day(rows[first]),
                        "to": day(rows[stop - 1]),
                    }
                )
        projected[key]["correlated"]["segments"] = segments
    old_selected = defaultdict(set)
    for _name, key, adjustment, index, pid, row, segment in records:
        if accepted_state(key, adjustment, index, segment) == "selected":
            old_selected[pid, day(row)].add(adjustment)
    scopes = {pid: sorted(days) for pid, days in scopes.items()}
    if equivalence_output is not None:
        equivalence_output.update(
            {
                pid: sorted(
                    {
                        when.astimezone(NEW_YORK).date().isoformat()
                        for (owner, when), equal in equivalence.items()
                        if owner == pid and equal and when.astimezone(NEW_YORK).date().isoformat() in days
                    }
                )
                for pid, days in scopes.items()
            }
        )
    bundles = {
        pid: {day: values for day, values in days.items() if day in scopes[pid]}
        for pid, days in bundles.items()
        if pid in scopes
    }
    if (
        bundles.keys() != scopes.keys()
        or any(set(bundles[pid]) != set(days) for pid, days in scopes.items())
        or any(set(values) != set(ADJUSTMENTS) for days in bundles.values() for values in days.values())
        or any(old_selected[pid, day] != set(ADJUSTMENTS) for pid, days in scopes.items() for day in days)
    ):
        raise LineageError("temporary-market decision lacks a complete selected eight-view bundle")
    return projected, scopes, bundles
