"""Resolve only unaccepted all-view contradictions using newer exact observations."""

from datetime import datetime, timedelta, timezone

from libs.common.debug_trace import event

from .contract import NEW_YORK, DownloadError, json_bytes, timestamp
from .correction import ProviderObservationConflict
from .endpoints import same_row
from .responses import completed_bars


def _read(root, binding):
    from .storage import read_document, regular_path

    try:
        receipt = read_document(
            regular_path(root, "correction.observations.json", published=True), published=True
        )
    except DownloadError as error:
        if isinstance(error.__cause__, FileNotFoundError):
            return {"schema_version": 1, "binding": binding, "resolutions": []}
        raise
    if receipt.get("schema_version") != 1 or not isinstance(receipt.get("resolutions"), list):
        raise DownloadError("invalid correction observation recovery; owning recovery required")
    if receipt.get("binding") != binding:
        return {"schema_version": 1, "binding": binding, "resolutions": []}
    if any(
        not isinstance(item, dict)
        or not {"previous_observation", "current_observation", "request", "observed_at"} <= item.keys()
        or any(
            not isinstance(item[key], list) or len(item[key]) != 8
            for key in ("previous_observation", "current_observation")
        )
        for item in receipt["resolutions"]
    ):
        raise DownloadError("incomplete correction observation recovery; owning recovery required")
    return receipt


def _write(root, receipt):
    import os

    from .storage import regular_path, write_file

    staged = regular_path(root, ".correction.observations.writing")
    staged.unlink(missing_ok=True)
    write_file(staged, json_bytes(receipt))
    os.replace(staged, regular_path(root, "correction.observations.json"))


def resolve(root, conflict, *, symbol, generation, correction_id, request, client):
    """Keep the completed replacement decision; resolve its stale values only.

    Called by the locked publisher only after an unequal full replacement has
    accounted for every original witness. Raw, missing, ambiguous or mutually
    incompatible witnesses never reach this boundary. Each successful newer
    observation is committed before another request or price publication.
    """
    conflicts = getattr(conflict, "context_conflicts", ())
    if not conflicts or getattr(conflict, "confirmed_observations", None) is None:
        raise conflict
    binding = {
        "store": str(root.parents[2].absolute()),
        "symbol": symbol,
        "source_sha256": generation,
        "correction_id": correction_id,
        "request": {
            key: request[key] for key in ("start", "end_exclusive", "asof", "feed", "currency", "timeframe")
        },
    }
    receipt = _read(root, binding)
    for disagreement in conflicts:
        evidence = disagreement.evidence
        if evidence["adjustment"] != "all" or evidence.get("session_rows") != 1:
            raise disagreement
        expected, acquired = evidence["selected_observation"], evidence["acquired_observation"]
        for item in receipt["resolutions"]:
            if same_row(expected, item["previous_observation"]):
                expected = item["current_observation"]
        if same_row(expected, acquired):
            continue
        day = timestamp(expected[0]).astimezone(NEW_YORK).date()
        start = datetime.combine(day, datetime.min.time(), NEW_YORK).astimezone(timezone.utc)
        end = min(
            datetime.combine(day + timedelta(days=1), datetime.min.time(), NEW_YORK).astimezone(timezone.utc),
            timestamp(request["end_exclusive"]),
        )
        fetch = getattr(client, "fetch_current_observation", client.fetch)
        response = completed_bars(
            fetch(symbol, "all", start, end, asof=request["asof"]), symbol, "all", start, end, request["asof"]
        )
        if len(response.rows) != 1 or not same_row(response.rows[0], acquired):
            error = ProviderObservationConflict(symbol, "all", expected, acquired)
            error.evidence.update(
                reconciliation_observations=[list(row) for row in response.rows],
                reconciliation="current exact-session response does not uniquely support acquired output",
            )
            raise error
        receipt["resolutions"].append(
            {
                "previous_observation": list(expected),
                "current_observation": list(response.rows[0]),
                "request": {
                    "symbol": symbol,
                    "adjustment": "all",
                    "start": start.isoformat(),
                    "end_exclusive": end.isoformat(),
                    "asof": request["asof"],
                },
                "observed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _write(root, receipt)
        event(
            "correction_observation_superseded",
            symbol=symbol,
            source_generation=generation,
            session=expected[0],
            previous_observation=expected,
            current_observation=response.rows[0],
        )
    return receipt
