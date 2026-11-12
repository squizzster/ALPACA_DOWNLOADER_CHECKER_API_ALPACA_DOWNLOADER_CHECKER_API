"""Project applicable accepted numerical decisions without selecting them again."""

from bisect import bisect_left
from dataclasses import dataclass
from types import MappingProxyType

from libs.alpaca_pricing_download.contract import ADJUSTMENTS


@dataclass(frozen=True)
class ChoiceRun:
    row_start: int
    row_end: int
    owner: str | None
    state: str | None
    tickers: tuple
    first: str
    last: str
    first_timestamp: str | None = None
    last_timestamp: str | None = None


@dataclass(frozen=True)
class RetainedChoices:
    source_generation: str
    previous_receipt_generation: str
    runs: object


def membership_choices(previous, *, source_generation, receipt_generation, legacy_boundary):
    """Project accepted choices for metadata-only membership work, without bars.

    The source-work owner already established pricing and metadata applicability.
    Accepted counts select positions; they cannot turn serving ownership into a
    numerical decision. Legacy assignments supply only their explicit flags.
    """
    offsets = dict.fromkeys(ADJUSTMENTS, 0)
    accepted_offsets = (previous.get("serving_ownership") or {}).get("offsets", {})
    result = {name: [] for name in ADJUSTMENTS}
    if all(item["rows"] == 0 for item in previous["assignment_summary"].values()):
        return RetainedChoices(
            source_generation, receipt_generation, MappingProxyType({name: () for name in ADJUSTMENTS})
        )
    for kind, parts in _accepted_parts(previous, legacy_boundary, previous["boundaries"]):
        for name, runs in parts.items():
            offset = accepted_offsets.get(name, {}).get(kind, offsets[name])
            last = 0
            for run in runs:
                if kind == "current_year" and run["from"][:7] >= previous["anchor_month"]:
                    continue
                last = run["row_end"]
                result[name].append(
                    ChoiceRun(
                        offset + run["row_start"],
                        offset + run["row_end"],
                        run["owner"],
                        run["state"],
                        tuple(run["tickers"]),
                        run["from"],
                        run["to"],
                        run["first_timestamp"],
                        run["last_timestamp"],
                    )
                )
            offsets[name] = offset + last
    return RetainedChoices(
        source_generation, receipt_generation, MappingProxyType({k: tuple(v) for k, v in result.items()})
    )


def _accepted_parts(previous, legacy_boundary, required):
    """Use explicit choices, or assemble selected legacy metadata once."""
    for kind in ("completed_years", "current_year", "current_month"):
        if kind not in required:
            continue
        handoff = previous.get("assignment_handoffs", {}).get(kind, {})
        choices = handoff.get("observation_choices")
        if choices is not None:
            yield kind, {name: choices["partitions"][choices["views"][name]]["runs"] for name in ADJUSTMENTS}
        else:
            document = legacy_boundary(kind)
            yield (
                kind,
                {
                    item["adjustment"]: tuple(
                        {
                            **segment,
                            "owner": (segment.get("security_identity") or {}).get("security_id"),
                            "state": segment.get("observation_selection")
                            if segment["branch"] == "correlated"
                            else "unassigned",
                            "tickers": tuple(
                                segment.get("observation_tickers")
                                or ([segment["historical_symbol"]] if segment.get("historical_symbol") else [])
                            ),
                        }
                        for segment in item["segments"]
                    )
                    for item in document["adjustments"]
                },
            )


def project_choices(
    previous,
    view,
    *,
    receipt_generation,
    legacy_boundary,
    invalidated=None,
    original_view=None,
):
    """Inputs already carry completed pricing-prefix and metadata applicability.

    Boundary-local, full-source and materialized-suffix positions are distinct.
    Only selected correction dates need exact position lookup; no history grid,
    numerical value or upstream applicability decision is checked here.
    """
    original = original_view or view
    invalidated = invalidated or {}
    origins = {
        name: view.previous_assignment_summary[name]["rows"]
        if view.materialization_scope == "retained_suffix"
        else 0
        for name in ADJUSTMENTS
    }
    summaries = previous.get("assignment_summary", {})
    old_limits = {name: summaries.get(name, {}).get("rows") for name in ADJUSTMENTS}
    materialized_old = {
        name
        for name in ADJUSTMENTS
        if len(view.views[name]) and (old_limits[name] is None or origins[name] < old_limits[name])
    }
    if not materialized_old:
        return RetainedChoices(
            view.source_sha256, receipt_generation, MappingProxyType(dict.fromkeys(ADJUSTMENTS, ()))
        )
    accepted_offsets = (previous.get("serving_ownership") or {}).get("offsets", {})
    kinds = ("completed_years", "current_year", "current_month")
    required = set()
    for number, kind in enumerate(kinds):
        for name in materialized_old:
            positions = accepted_offsets.get(name, {})
            if kind not in positions or old_limits[name] is None:
                required.add(kind)
                break
            first = positions[kind] + 1
            last = positions[kinds[number + 1]] if number < 2 else old_limits[name]
            if first <= origins[name] + len(view.views[name]) and origins[name] < last:
                required.add(kind)
                break
    original_origins = {
        name: original.previous_assignment_summary[name]["rows"]
        if original.materialization_scope == "retained_suffix"
        else 0
        for name in ADJUSTMENTS
    }
    excluded, lookups = {}, {}
    for name in ADJUSTMENTS:
        dates = (original.reprojected_sessions or {}).get(name)
        if dates is not None:
            excluded[name] = {
                original_origins[name] + number: invalidated[day]
                for number, day in enumerate(dates, 1)
                if day in invalidated
            }
        else:
            excluded[name] = {}
            for day, identities in invalidated.items():
                rows = original.views[name]
                key = id(rows), day
                if key not in lookups:
                    index = bisect_left(rows, day, key=lambda row: row[0][:10])
                    lookups[key] = index if index < len(rows) and rows[index][0][:10] == day else None
                index = lookups[key]
                if index is not None:
                    excluded[name][original_origins[name] + index + 1] = identities

    offsets = dict.fromkeys(ADJUSTMENTS, 0)
    result = {name: [] for name in ADJUSTMENTS}
    for kind, parts in _accepted_parts(previous, legacy_boundary, required):
        handoff = previous.get("assignment_handoffs", {}).get(kind, {})
        for name in ADJUSTMENTS:
            offset = accepted_offsets.get(name, {}).get(kind, offsets[name])
            last_position = 0
            for run in parts[name]:
                if kind == "current_year" and run["from"][:7] >= previous["anchor_month"]:
                    continue
                last_position = run["row_end"]
                if run["branch"] != "correlated" or run["owner"] is None or run["state"] is None:
                    continue
                first = max(offset + run["row_start"], origins[name] + 1)
                last = min(offset + run["row_end"], origins[name] + len(view.views[name]))
                if first > last:
                    continue
                cuts = sorted(
                    index
                    for index, identities in excluded[name].items()
                    if first <= index <= last and run["owner"] in identities
                )
                for stop in (*cuts, last + 1):
                    if first < stop:
                        result[name].append(
                            ChoiceRun(
                                first - origins[name],
                                stop - 1 - origins[name],
                                run["owner"],
                                run["state"],
                                tuple(run["tickers"]),
                                run["from"],
                                run["to"],
                            )
                        )
                    first = stop + 1
            offsets[name] = offset + handoff.get("ownership", {}).get(name, {}).get("rows", last_position)
    return RetainedChoices(
        view.source_sha256,
        receipt_generation,
        MappingProxyType({name: tuple(runs) for name, runs in result.items()}),
    )
