"""The selected mapping owner's bounded journal and outside-lock execution."""

from datetime import datetime

from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.mapping import restore_observation
from libs.alpaca_pricing_download.responses import completed_bars
from libs.alpaca_pricing_download.storage import read_document

from . import storage
from .contract import RecoveryRequired
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
    try:
        witness = restore_observation(value)
        if witness.adjustment != "all" or len(witness.rows) > 1:
            raise ValueError("expected one fully adjusted witness session")
        return witness
    except (DownloadError, TypeError, ValueError, OverflowError) as error:
        raise RecoveryRequired("invalid completed mapping witness; owning recovery required") from error


def _load_stage(path, request):
    from libs.alpaca_pricing_download.mapping import MappingAcquisition

    need = request["numerical_mapping"]
    fresh = {"version": VERSION, "candidate_id": need["candidate_id"], "binding": need["binding"]}
    if not path.exists():
        return {"path": path, "value": fresh}
    try:
        value = read_document(path)
        if type(value["version"]) is not int or value["version"] != VERSION:
            raise ValueError("unsupported numerical mapping journal version")
        if not isinstance(value["candidate_id"], str) or not isinstance(value["binding"], dict):
            raise ValueError("invalid mapping journal binding")
        if value["candidate_id"] != need["candidate_id"]:
            return {"path": path, "value": fresh}
        if value["binding"] != need["binding"]:
            raise ValueError("numerical mapping journal binding changed")
        stage = {"path": path, "value": value}
        if "assessment" in value:
            # Only the completed disposition is needed on acknowledgement.
            assessment = value["assessment"]
            if (
                assessment["candidate_id"] != need["candidate_id"]
                or assessment["binding"] != need["binding"]
                or assessment["outcome"]
                not in {
                    "mapped_history_acquired",
                    "provider_mapping_unavailable",
                    "provider_coverage_insufficient",
                }
                or not isinstance(assessment["scope"], dict)
                or not isinstance(assessment["witness"], dict)
                or not isinstance(assessment["result_generation"], str)
            ):
                raise ValueError("invalid completed mapping assessment")
        elif "acquisition" in value:
            acquisition = MappingAcquisition.from_document(value["acquisition"])
            if acquisition.candidate_id != need["candidate_id"]:
                raise ValueError("invalid mapped acquisition journal binding")
            acquisition.require_binding(
                need["binding"]["store"],
                need["binding"]["symbol"],
                request["selected_source_sha256"],
                datetime.fromisoformat(request["from"] + "T00:00:00+00:00"),
                datetime.fromisoformat(request["end_exclusive"] + "T00:00:00+00:00"),
                request["mapping_asof"],
            )
            witness = acquisition.fragments[0]
            if not witness.rows or (witness.start, witness.end) != witness_window(need):
                raise ValueError("mapped acquisition lacks its selected positive witness")
            # The acquisition carries its accepted witness into the eventual assessment.
            value["witness"] = value["acquisition"]["fragments"][0]
            stage.update(acquisition=acquisition, witness=witness)
        elif "witness" in value:
            witness = _restore_witness(value["witness"])
            if (witness.symbol, witness.asof, witness.start, witness.end) != (
                need["binding"]["symbol"],
                need["binding"]["asof"],
                *witness_window(need),
            ):
                raise ValueError("completed mapping witness has a different scope")
            stage["witness"] = witness
            if "required_sessions" in value and (
                not isinstance(value["required_sessions"], list)
                or not value["required_sessions"]
                or any(not isinstance(day, str) for day in value["required_sessions"])
            ):
                raise ValueError("invalid mapping contribution grid")
        elif "required_sessions" in value:
            raise ValueError("missing completed mapping witness")
        return stage
    except (DownloadError, KeyError, IndexError, AttributeError, TypeError, ValueError, OverflowError) as error:
        raise RecoveryRequired("invalid numerical mapping journal; owning recovery required") from error


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
        stages[identifier] = _load_stage(path, request)
    elif stages[identifier]["value"]["binding"] != need["binding"]:
        raise RecoveryRequired("numerical mapping journal binding changed; owning recovery required")

    stage = stages[identifier]
    value, path = stage["value"], stage["path"]

    def persist():
        stage["pending_write"] = True
        storage.atomic_json(path, value, compare=False)
        stage.pop("pending_write")

    if stage.get("pending_write"):
        persist()

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
    if "witness" in stage:
        witness = stage["witness"]
    else:
        start, end = witness_window(need)
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
        stage["witness"] = witness
        persist()  # Complete this observation before any independent action.
    if not witness.rows:
        return finish("provider_mapping_unavailable", request["selected_source_sha256"])
    from libs.alpaca_pricing_download.mapping import MappingAcquisition, MappingCoverageIncomplete

    if "acquisition" not in stage:
        if not required_sessions:
            required_sessions = value.get("required_sessions") or cache.get("required_sessions", {}).get(
                identifier
            )
        if not required_sessions and grid_provider is not None:
            required_sessions = grid_provider()
            cache.setdefault("required_sessions", {})[identifier] = required_sessions
        if not required_sessions:
            raise RecoveryRequired("missing accepted mapping contribution grid; owning recovery required")
        if "required_sessions" not in value:
            value["required_sessions"] = list(required_sessions)
            persist()
        try:
            stage["acquisition"] = MappingAcquisition(
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
        except (DownloadError, TypeError, ValueError, OverflowError) as error:
            raise RecoveryRequired("invalid selected mapping capability; owning recovery required") from error
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
