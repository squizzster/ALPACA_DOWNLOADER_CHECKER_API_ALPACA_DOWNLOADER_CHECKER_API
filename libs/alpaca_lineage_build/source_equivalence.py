"""Carry a completed equal-bundle decision independently of row ownership.

Selection owns numerical equality. This module projects that result onto an
explicit previously selected donor; it never compares prices. Receipt and
pricing bindings survive later metadata publication and disjoint new contests.
"""

from bisect import bisect_left
from collections import defaultdict

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .assignment_handoff import _FrozenDict
from .contract import LineageError


class AcceptedSourceEquivalence(_FrozenDict):
    """A schema-accepted immutable projection; later consumers borrow it."""

    __setattr__ = __delattr__ = _FrozenDict._immutable


class AcceptedEqualityFrame(_FrozenDict):
    """An accepted frame keeps its original immutable generation bindings."""

    __setattr__ = __delattr__ = _FrozenDict._immutable


def _freeze(value):
    if isinstance(value, _FrozenDict):
        return value
    if isinstance(value, dict):
        return _FrozenDict({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    return value


def project(inputs, reports, positions, retained, equivalence, instants, *, track_opened=True):
    if not equivalence and not track_opened:
        return {"opened": set(), "identities": {}}
    opened, winners = set(), defaultdict(dict)
    donors = defaultdict(lambda: defaultdict(set))
    for source, report in reports.items():
        choices = retained.get(source, (None,) * 4)
        choices = choices[3] if len(choices) > 3 else None
        for segment in report["correlated"]["segments"]:
            view = segment["adjustment"]
            pid = segment["security_identity"]["security_id"]
            runs = choices.runs[view] if choices is not None else ()
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                row = inputs[source].views[view][index]
                if row[0] not in instants:
                    instants[row[0]] = timestamp(row[0])
                instant = instants[row[0]]
                key = pid, instant
                if track_opened:
                    opened.add((pid, instant.astimezone(NEW_YORK).date().isoformat()))
                if equivalence.get(key) is not True:
                    continue
                if segment.get("observation_selection") == "selected":
                    winners[key][view] = source
                if not runs:
                    continue
                original = positions[source][view][index] if source in positions else index + 1
                number = bisect_left(runs, original, key=lambda run: run.row_end)
                if number == len(runs):
                    continue
                run = runs[number]
                if run.row_start <= original <= run.row_end and run.state == "selected":
                    donors[key][run.owner, source].add(view)
    identities = {}
    for (pid, instant), views in winners.items():
        if set(views) != set(ADJUSTMENTS) or len(set(views.values())) != 1:
            continue
        approved = [
            {"public_id": old, "source": source}
            for (old, source), donated in donors[pid, instant].items()
            if donated == set(ADJUSTMENTS)
        ]
        if approved:
            day = instant.astimezone(NEW_YORK).date().isoformat()
            identities.setdefault(pid, {})[day] = {
                "selected_source": views["raw"],
                "donors": sorted(approved, key=lambda item: (item["public_id"], item["source"])),
            }
    return {"opened": opened, "identities": identities}


def _carried_frames(previous_receipts, opened, identity_changes):
    """Retain untouched completed scopes under their original bindings."""
    frames, seen = [], set()
    for previous in previous_receipts:
        previous = previous or {}
        validate(previous)
        for frame in previous.get("numerical_source_equivalence", {}).get("decisions", ()):
            # Group receipts share this producer object. Its original binding,
            # rather than a rehash of its contents, names the completed frame.
            key = tuple(sorted(frame["previous_receipt_generations"].items()))
            if key in seen:
                continue
            seen.add(key)
            identities = {}
            for pid, days in frame["identities"].items():
                target = identity_changes.get(pid, {}).get("security_id", pid)
                kept = {day: value for day, value in days.items() if (target, day) not in opened}
                if kept:
                    identities[target] = kept
            if identities:
                carried = {**frame, "identities": _freeze(identities)}
                frames.append(
                    AcceptedEqualityFrame(carried) if isinstance(frame, AcceptedEqualityFrame) else carried
                )
    return frames


def _completed(frames):
    if not frames:
        return None
    envelope = {
        "numerical_source_equivalence_version": 1,
        "numerical_source_equivalence": {"version": 1, "decisions": frames},
    }
    validate(envelope)
    return envelope["numerical_source_equivalence"]


def retire(previous, scopes):
    opened = {(pid, day) for pid, days in scopes.items() for day in days}
    return _completed(_carried_frames(previous.values(), opened, {}))


def build(controls, selection, identity_changes):
    frames = _carried_frames(
        (control.previous for control in controls.values()), selection["opened"], identity_changes
    )
    if selection["identities"]:
        previous = {name: control for name, control in controls.items() if control.previous is not None}
        if any(control.previous_receipt_generation is None for control in previous.values()):
            raise LineageError("source equality lacks its previous receipt binding; owning recovery required")
        frames.append(
            {
                "previous_receipt_generations": {
                    name: control.previous_receipt_generation for name, control in previous.items()
                },
                "pricing_generations": {name: control.source_sha256 for name, control in controls.items()},
                "previous_pricing_generations": {
                    name: control.previous["source_sha256"] for name, control in previous.items()
                },
                "identities": selection["identities"],
            }
        )
    return _completed(frames)


def carry_prefix(previous, through):
    """Ordinary suffix publication preserves the completed closed prefix."""
    validate(previous)
    value = previous.get("numerical_source_equivalence")
    if value is None or through is None:
        return None
    frames = []
    for frame in value["decisions"]:
        identities = {
            pid: kept
            for pid, days in frame["identities"].items()
            if (kept := {day: decision for day, decision in days.items() if day <= through})
        }
        if identities:
            frames.append(AcceptedEqualityFrame({**frame, "identities": _freeze(identities)}))
    return AcceptedSourceEquivalence(version=1, decisions=tuple(frames)) if frames else None


def validate(receipt):
    """Accept the newly produced capability at the owning receipt ingress."""
    if not ({"numerical_source_equivalence", "numerical_source_equivalence_version"} & receipt.keys()):
        return
    value = receipt.get("numerical_source_equivalence")
    if (
        isinstance(value, AcceptedSourceEquivalence)
        and type(receipt.get("numerical_source_equivalence_version")) is int
        and receipt["numerical_source_equivalence_version"] == 1
    ):
        return
    valid = (
        type(receipt.get("numerical_source_equivalence_version")) is int
        and receipt["numerical_source_equivalence_version"] == 1
        and isinstance(value, dict)
        and set(value) == {"version", "decisions"}
        and type(value.get("version")) is int
        and value["version"] == 1
        and isinstance(value.get("decisions"), list)
        and bool(value["decisions"])
    )
    if valid:
        from .correlation_policy_contract import valid_sha256

        for frame in value["decisions"]:
            if isinstance(frame, AcceptedEqualityFrame):
                continue
            if (
                not isinstance(frame, dict)
                or set(frame)
                != {
                    "previous_receipt_generations",
                    "previous_pricing_generations",
                    "pricing_generations",
                    "identities",
                }
                or not isinstance(frame["previous_receipt_generations"], dict)
                or not isinstance(frame["pricing_generations"], dict)
                or not isinstance(frame["previous_pricing_generations"], dict)
                or frame["previous_receipt_generations"].keys() != frame["previous_pricing_generations"].keys()
                or not frame["previous_receipt_generations"].keys() <= frame["pricing_generations"].keys()
                or not frame["pricing_generations"]
                or any(
                    not valid_sha256(token)
                    for field in (
                        "previous_receipt_generations",
                        "previous_pricing_generations",
                        "pricing_generations",
                    )
                    for token in frame[field].values()
                )
                or not isinstance(frame["identities"], dict)
                or not frame["identities"]
            ):
                valid = False
                break
            for pid, days in frame["identities"].items():
                if not valid_sha256(pid) or not isinstance(days, dict) or not days:
                    valid = False
                    break
                for day, decision in days.items():
                    if (
                        not isinstance(day, str)
                        or len(day) != 10
                        or not isinstance(decision, dict)
                        or set(decision) != {"selected_source", "donors"}
                        or decision["selected_source"] not in frame["pricing_generations"]
                        or not isinstance(decision["donors"], list)
                        or not decision["donors"]
                        or any(
                            not isinstance(donor, dict)
                            or set(donor) != {"source", "public_id"}
                            or donor["source"] not in frame["previous_pricing_generations"]
                            or not valid_sha256(donor["public_id"])
                            for donor in decision["donors"]
                        )
                    ):
                        valid = False
                        break
    if not valid:
        raise LineageError("incomplete numerical source equivalence; finish owning lineage recovery")
    receipt["numerical_source_equivalence"] = AcceptedSourceEquivalence(
        version=1,
        decisions=tuple(
            frame if isinstance(frame, AcceptedEqualityFrame) else AcceptedEqualityFrame(_freeze(frame))
            for frame in value["decisions"]
        ),
    )
