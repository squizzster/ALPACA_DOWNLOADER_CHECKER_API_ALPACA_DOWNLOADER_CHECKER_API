"""Project accepted membership and row decisions into a selected continuation."""

from bisect import bisect_right
from copy import deepcopy
from datetime import date

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError

HANDOFF_VERSION = 1


def _valid_rename_certificate(certificate):
    required = {
        "action_id",
        "old_symbol",
        "new_symbol",
        "old_cusip",
        "new_cusip",
        "old_id",
        "new_id",
        "boundary",
        "legal_date",
        "labels",
    }
    if not isinstance(certificate, dict) or set(certificate) != required:
        return False
    if any(not isinstance(certificate[key], str) or not certificate[key] for key in required - {"labels"}):
        return False
    labels = certificate["labels"]
    if not isinstance(labels, list) or not labels or len(labels) > 100:
        return False
    try:
        days = [certificate["boundary"], certificate["legal_date"]]
        previous, names = None, set()
        for label in labels:
            if (
                not isinstance(label, list)
                or len(label) != 3
                or any(not isinstance(item, str) or not item for item in label)
            ):
                return False
            name, first, last = label
            if (
                first >= last
                or previous is not None
                and first != previous
                or name in names
                or name == certificate["new_symbol"]
            ):
                return False
            previous = last
            names.add(name)
            days.extend((first, last))
        return (
            all(date.fromisoformat(day).isoformat() == day for day in days)
            and labels[-1][0] == certificate["old_symbol"]
            and previous == certificate["boundary"]
            and certificate["legal_date"] <= certificate["boundary"]
            and all(
                len(certificate[key]) == 9 and certificate[key].isascii() and certificate[key].isalnum()
                for key in ("old_cusip", "new_cusip")
            )
        )
    except (TypeError, ValueError):
        return False


def _valid_rename_handoffs(previous):
    """Validate each newly read envelope once, not its completed upstream proof."""
    if "sourced_rename_ownership" not in previous or "sourced_rename_membership" not in previous:
        return False
    membership, source = previous["sourced_rename_membership"], previous["sourced_rename_ownership"]
    certificates = {}
    if membership is not None:
        if (
            not isinstance(membership, dict)
            or set(membership) != {"version", "certificates", "issues"}
            or type(membership["version"]) is not int
            or membership["version"] != 1
        ):
            return False
        if not isinstance(membership["certificates"], list) or not isinstance(membership["issues"], list):
            return False
        for item in membership["certificates"]:
            if not _valid_rename_certificate(item) or item["action_id"] in certificates:
                return False
            certificates[item["action_id"]] = item
        if any(
            not isinstance(item, dict)
            or set(item) != {"reason_code", "corporate_action_id"}
            or any(not isinstance(value, str) or not value for value in item.values())
            for item in membership["issues"]
        ):
            return False
    if source is None:
        return True
    if (
        not isinstance(source, dict)
        or set(source) != {"version", "symbol", "first", "last", "action_id"}
        or type(source["version"]) is not int
        or source["version"] != 1
        or source["symbol"] != previous.get("symbol")
    ):
        return False
    if not isinstance(source["action_id"], str) or source["action_id"] not in certificates:
        return False
    certificate = certificates[source["action_id"]]
    try:
        return (
            all(date.fromisoformat(source[key]).isoformat() == source[key] for key in ("first", "last"))
            and certificate["new_symbol"] == source["symbol"]
            and certificate["boundary"] <= source["last"]
            and certificate["labels"][0][1] <= source["first"] <= source["last"]
        )
    except (TypeError, ValueError):
        return False


def validate_handoff(previous):
    """A declared completed handoff cannot fall back to legacy assembly."""
    if previous and previous.get("schema_version", 0) >= 36 and (not _valid_rename_handoffs(previous)):
        raise LineageError("incomplete sourced-rename ownership handoff; finish owning lineage recovery")
    if previous and previous.get("schema_version", 0) >= 35:
        certificate = previous.get("current_source_ownership")
        dates_valid = certificate is None
        if isinstance(certificate, dict):
            try:
                dates_valid = all(
                    date.fromisoformat(certificate[key]).isoformat() == certificate[key]
                    for key in ("first", "last", "departure_date")
                )
            except (KeyError, TypeError, ValueError):
                dates_valid = False
        if (
            "current_source_ownership" not in previous
            or certificate is not None
            and not (
                isinstance(certificate, dict)
                and set(certificate)
                == {
                    "version",
                    "symbol",
                    "asset_id",
                    "first",
                    "last",
                    "departed_asset_id",
                    "departure_action_id",
                    "departure_date",
                }
                and type(certificate.get("version")) is int
                and certificate.get("version") == 1
                and certificate.get("symbol") == previous.get("symbol")
                and all(
                    isinstance(certificate.get(key), str) and certificate[key]
                    for key in (
                        "asset_id",
                        "first",
                        "last",
                        "departed_asset_id",
                        "departure_action_id",
                        "departure_date",
                    )
                )
                and dates_valid
                and certificate["asset_id"] != certificate["departed_asset_id"]
                and certificate["departure_date"] < certificate["first"] <= certificate["last"]
            )
        ):
            raise LineageError("incomplete current-source ownership handoff; finish owning lineage recovery")
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
