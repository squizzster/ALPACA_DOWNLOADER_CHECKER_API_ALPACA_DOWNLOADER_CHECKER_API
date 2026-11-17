"""The selected mapping owner's bounded journal and outside-lock execution."""

from datetime import datetime

from libs.alpaca_pricing_download.responses import CompletedBars, completed_bars
from libs.alpaca_pricing_download.storage import read_document

from . import storage
from .contract import LineageError, RecoveryRequired
from .numerical_mapping import VERSION, witness_window


def _witness_document(value):
    return {
        "symbol": value.symbol,
        "adjustment": value.adjustment,
        "start": value.start.isoformat(),
        "end": value.end.isoformat(),
        "asof": value.asof,
        "rows": list(value.rows),
        "singleton": value.singleton,
    }


def _restore_witness(value):
    # The stage owner already normalized these completed provider rows. Restore
    # their declared envelope, never transform/hash/compare them a second time.
    if not isinstance(value, dict) or set(value) != {
        "symbol",
        "adjustment",
        "start",
        "end",
        "asof",
        "rows",
        "singleton",
    }:
        raise LineageError("invalid completed mapping witness; owning recovery required")
    return CompletedBars(
        value["symbol"],
        value["adjustment"],
        datetime.fromisoformat(value["start"]),
        datetime.fromisoformat(value["end"]),
        value["asof"],
        tuple(tuple(row) for row in value["rows"]),
        value["singleton"],
    )


def execute(
    store, root, symbol, request, *, client, download, context, cache, required_sessions, grid_provider=None
):
    """Acquire one new mapping, carrying every completed observation forward.

    Caller holds the lineage worker/operation gate and NO pricing locks. Only
    this selected source's journal is opened. A completed negative is processing,
    not a pricing rejection, ownership verdict or a new serving policy.
    """
    need = request["numerical_mapping"]
    identifier = need["candidate_id"]
    stages = cache.setdefault("stages", {})
    if identifier not in stages:
        path = storage.path_at(root, f"{symbol}/.numerical-mapping.json")
        value = read_document(path) if path.exists() else None
        if value is not None and (
            not isinstance(value, dict)
            or type(value.get("version")) is not int
            or value.get("version") != VERSION
            or not isinstance(value.get("candidate_id"), str)
        ):
            raise LineageError("unsupported numerical mapping journal; owning recovery required")
        if value is None or value["candidate_id"] != identifier:
            value = {"version": VERSION, "candidate_id": identifier, "binding": need["binding"]}
        stages[identifier] = {"path": path, "value": value}
    stage = stages[identifier]
    value, path = stage["value"], stage["path"]
    if value["binding"] != need["binding"]:
        raise LineageError("numerical mapping journal binding changed")

    def persist():
        storage.atomic_json(path, value, compare=False)

    def finish(outcome, generation, *, missing_sessions=()):
        scope = (
            {"kind": "one_session_all_witness", "session": need["binding"]["witness_session"]}
            if outcome == "provider_mapping_unavailable"
            else {
                "kind": "mapped_history_required_sessions",
                "start": request["from"],
                "end_exclusive": request["end_exclusive"],
                "missing_sessions": list(missing_sessions),
            }
        )
        assessment = {
            "candidate_id": identifier,
            "binding": need["binding"],
            "outcome": outcome,
            "witness": value["witness"],
            "result_generation": generation,
            "scope": scope,
        }
        value["assessment"] = assessment
        persist()
        cache.setdefault("completed", {})[identifier] = assessment
        return {"mapping_assessment": assessment, "source_sha256": generation, "changed": False}

    if "assessment" in value:
        assessment = value["assessment"]
        cache.setdefault("completed", {})[identifier] = assessment
        return {
            "mapping_assessment": assessment,
            "source_sha256": assessment["result_generation"],
            "changed": False,
        }
    start, end = witness_window(need)
    if "witness" not in value:
        persist()  # An interruption cannot forget the selected request context.
        witness = completed_bars(
            client.fetch(symbol, "all", start, end, asof=request["mapping_asof"]),
            symbol,
            "all",
            start,
            end,
            request["mapping_asof"],
        )
        value["witness"] = _witness_document(witness)
        persist()  # Complete this observation before any independent action.
        stage["witness"] = witness
    elif "witness" in stage:
        witness = stage["witness"]
    else:
        witness = _restore_witness(value["witness"])
        stage["witness"] = witness
    if not witness.rows:
        return finish("provider_mapping_unavailable", request["selected_source_sha256"])
    if not required_sessions:
        required_sessions = cache.get("required_sessions", {}).get(identifier)
    if not required_sessions and "acquisition" in value:
        required_sessions = value["acquisition"].get("required_sessions")
    if not required_sessions and "required_sessions" in value:
        required_sessions = tuple(value["required_sessions"])
    if not required_sessions and grid_provider is not None:
        required_sessions = grid_provider()
        cache.setdefault("required_sessions", {})[identifier] = required_sessions
    if not required_sessions:
        raise RecoveryRequired("missing accepted mapping contribution grid; owning recovery required")
    if "required_sessions" not in value:
        value["required_sessions"] = list(required_sessions)
        persist()
    from libs.alpaca_pricing_download.mapping import MappingAcquisition, MappingCoverageIncomplete

    if "acquisition" not in stage:
        stage["acquisition"] = (
            MappingAcquisition.from_document(value["acquisition"])
            if "acquisition" in value
            else MappingAcquisition(
                store,
                symbol,
                request["selected_source_sha256"],
                datetime.fromisoformat(request["from"] + "T00:00:00+00:00"),
                datetime.fromisoformat(request["end_exclusive"] + "T00:00:00+00:00"),
                request["mapping_asof"],
                identifier,
                witness,
                required_sessions=required_sessions,
            )
        )
    acquisition = stage["acquisition"]

    def progress(current):
        value["acquisition"] = current.document()
        persist()

    if "acquisition" not in value:
        progress(acquisition)
    try:
        result = download(
            store,
            symbol,
            request["end_exclusive"],
            client=client,
            from_date=request["from"],
            refresh_all=True,
            mapping_asof=request["mapping_asof"],
            selected_source_sha256=request["selected_source_sha256"],
            history_correction_id=identifier,
            context=context,
            mapping_acquisition=acquisition,
            on_mapping_progress=progress,
        )
    except MappingCoverageIncomplete as error:
        return finish(
            "provider_coverage_insufficient",
            request["selected_source_sha256"],
            missing_sessions=error.missing_sessions,
        )
    completed = finish("mapped_history_acquired", result["source_sha256"])
    return {**result, **completed, "changed": result.get("changed", True)}
