"""Complete selected numerical claims without replaying their accepted owners."""

from dataclasses import dataclass

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import LineageError
from .observations import select_observations


@dataclass(frozen=True)
class NumericalInput:
    source: dict
    source_sha256: str
    views: dict
    instant_keys: dict


@dataclass(frozen=True)
class NumericalCompletion:
    reports: dict
    changes: dict
    scopes: dict
    selected_sources: dict
    equality_outcomes: dict
    selected_bundles: dict


def complete(inputs, reports, *, positions, dates):
    """Select one bundle for only the already admitted counterpart positions.

    Reports carry accepted owners, labels and complete own-label routes. Local
    positions identify the materialized subset; ``positions`` maps them back to
    the selected publication. The caller retains this completed value through
    output construction and publication instead of invoking selection again.
    """
    instants, equality = {}, {}
    for view in inputs.values():
        instants.update(getattr(view, "instant_keys", {}))
    # Authored market/adjustment assertions may remain in retained provenance.
    # Reopening a numerical contest does not make them admissible fresh inputs.
    # Keep accepted owners, labels and routes, without mutating the old report.
    working = {
        name: {
            **report,
            "correlated": {
                **report["correlated"],
                "segments": [
                    {key: value for key, value in segment.items() if key != "observation_authority"}
                    if "observation_authority" in segment
                    else segment
                    for segment in report["correlated"]["segments"]
                ],
            },
        }
        for name, report in reports.items()
    }
    # A clipped old contest must retain a later independently observed occupant.
    # Its completed own-label route is sufficient for the existing mapping guard;
    # no unselected historical rows need to be materialized to rediscover it.
    occupants = {
        name: {
            segment["security_identity"]["security_id"]
            for segment in report["correlated"]["segments"]
            if segment["adjustment"] == "raw"
        }
        | set(report.get("retained_observation_routes", ()))
        for name, report in working.items()
    }
    selected, corrections = select_observations(
        inputs, working, instant_keys=instants, equality_outcomes=equality, source_occupants=occupants
    )
    if corrections:
        raise LineageError(
            f"numerical completion requires owning pricing mapping correction for {sorted(corrections)}"
        )

    changes, scopes, sources, bundles, days = {}, {}, {}, {}, {}
    for name, report in selected.items():
        changes[name] = {adjustment: {} for adjustment in ADJUSTMENTS}
        for segment in report["correlated"]["segments"]:
            state = segment["observation_selection"]
            if state not in ("selected", "superseded"):
                raise LineageError("numerical completion requires one complete selected source bundle")
            adjustment = segment["adjustment"]
            identity = segment["security_identity"]["security_id"]
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                original = positions[name][adjustment][index]
                day, stamp = dates[name][adjustment][original]
                days[stamp] = day
                scopes.setdefault(identity, set()).add(day)
                changes[name][adjustment][original] = {
                    "observation_selection": state,
                    "observation_tickers": segment["observation_tickers"],
                    "timestamp": stamp,
                    "day": day,
                }
                if state != "selected":
                    continue
                selected_sources = sources.setdefault(identity, {})
                if selected_sources.setdefault(day, name) != name:
                    raise LineageError("numerical completion selected different sources across views")
                bundle = bundles.setdefault(identity, {}).setdefault(day, {})
                if adjustment in bundle:
                    raise LineageError("numerical completion selected competing observations")
                bundle[adjustment] = {"source": name, "row": inputs[name].views[adjustment][index]}

    outcomes = {
        identity: dict.fromkeys(sorted(selected_days), False) for identity, selected_days in scopes.items()
    }
    instant_days = {instant: days[stamp] for stamp, instant in instants.items() if stamp in days}
    for (identity, instant), equal in equality.items():
        outcomes[identity][instant_days[instant]] = equal
    for identity, selected_days in scopes.items():
        for day in selected_days:
            if set(bundles.get(identity, {}).get(day, ())) != set(ADJUSTMENTS):
                raise LineageError("numerical completion requires one complete selected source bundle")
    return NumericalCompletion(
        selected,
        changes,
        {identity: tuple(sorted(selected_days)) for identity, selected_days in scopes.items()},
        sources,
        outcomes,
        bundles,
    )
