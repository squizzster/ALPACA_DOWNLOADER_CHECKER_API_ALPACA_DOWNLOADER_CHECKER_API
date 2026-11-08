"""Project accepted membership and row decisions into a selected continuation."""

from bisect import bisect_right
from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError

HANDOFF_VERSION = 1


def validate_handoff(previous):
    """A declared completed handoff cannot fall back to legacy assembly."""
    if not previous or "continuation_handoff_version" not in previous:
        return
    if (
        type(previous["continuation_handoff_version"]) is not int
        or previous["continuation_handoff_version"] != HANDOFF_VERSION
        or not isinstance(previous.get("identity_memberships"), list)
        or not isinstance(previous.get("identity_intervals"), list)
        or not isinstance(previous.get("observation_routes"), dict)
    ):
        raise LineageError("incomplete lineage continuation handoff; finish owning lineage recovery")


def identity_intervals(previous, boundaries):
    """Accepted contribution ranges include every view and superseded sources."""
    if "identity_intervals" in previous:
        return previous["identity_intervals"]
    return [
        {
            "from": segment["from"],
            "to": segment["to"],
            "security_id": segment["security_identity"]["security_id"],
        }
        for kind, document in boundaries.items()
        for entry in document["adjustments"]
        for segment in entry["segments"]
        if segment["branch"] == "correlated"
        and (kind != "current_year" or segment["from"][:7] < previous["anchor_month"])
    ]


def identity_memberships(previous, boundaries):
    """Consume compact membership, or project already selected legacy assignments."""
    accepted = {}
    for symbol, prior in previous.items():
        identities = prior.get("identity_memberships")
        if identities is None:
            identities = [
                segment["security_identity"]
                for document in boundaries.get(symbol, {}).values()
                for entry in document["adjustments"]
                for segment in entry["segments"]
                if segment["branch"] == "correlated"
            ]
        for identity in identities:
            for member in identity["member_alpaca_asset_ids"]:
                existing = accepted.setdefault(member, identity)
                if existing["security_id"] != identity["security_id"]:
                    raise LineageError(
                        "accepted UUID membership is ambiguous; explicit lineage recovery required"
                    )
    return accepted


def retained_report(previous, boundaries, view, *, unchanged_before):
    """Freeze selected unchanged rows; numerical selection may reconsider them.

    These are accepted ownership decisions, not another correlation execution.
    Positions and request provenance describe the newly assembled suffix only.
    """
    from .transform import retained_decision_report

    report = retained_decision_report(previous, policy=None)
    report["correlation_policy"] = previous["correlation_policy"]
    report["source_row_count"] = sum(len(rows) for rows in view.views.values())
    report["source_downloads"] = []
    report["ticker_ownership_intervals"] = []
    report["retained_observation_routes"] = deepcopy(previous.get("observation_routes", {}))
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        descriptor = next(item for item in view.local["downloads"] if item["adjustment"] == adjustment)
        source = {
            "data_file": descriptor["file"],
            "download_manifest": None,
            "download_sha256": descriptor["sha256"],
            "adjustment": adjustment,
            "asof": descriptor["asof"],
            "request_from": descriptor["request_from"],
            "request_to": descriptor["request_to"],
            "feed": descriptor["feed"],
            "timeframe": descriptor["timeframe"],
        }
        report["source_downloads"].append(source)
        accepted = [
            segment
            for kind, document in boundaries.items()
            for entry in document["adjustments"]
            if entry["adjustment"] == adjustment
            for segment in entry["segments"]
            if kind != "current_year" or segment["from"][:7] < previous["anchor_month"]
        ]
        accepted.sort(key=lambda segment: segment["from"])
        starts = [segment["from"] for segment in accepted]
        last, run = None, None
        for number, row in enumerate(rows, 1):
            day = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            if day >= unchanged_before[:10]:
                continue
            position = bisect_right(starts, day) - 1
            segment = accepted[position] if position >= 0 else None
            if segment is None or day > segment["to"] or segment["branch"] != "correlated":
                raise LineageError("selected continuation is missing an accepted row decision")
            if last is segment:
                run.update(row_end=number, row_count=run["row_count"] + 1, last_timestamp=row[0], to=day)
            else:
                run = {
                    **segment,
                    **source,
                    "row_start": number,
                    "row_end": number,
                    "row_count": 1,
                    "first_timestamp": row[0],
                    "last_timestamp": row[0],
                    "from": day,
                    "to": day,
                }
                report["correlated"]["segments"].append(run)
                last = segment
            report["correlated"]["row_count"] += 1
            interval = segment.get("ticker_ownership_interval")
            if interval and interval not in report["ticker_ownership_intervals"]:
                report["ticker_ownership_intervals"].append(interval)
    return report
