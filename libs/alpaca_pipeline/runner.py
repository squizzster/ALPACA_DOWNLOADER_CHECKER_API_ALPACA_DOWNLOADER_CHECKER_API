"""Forward native stage values under one continuous store gate."""

from uuid import uuid4

from .execution import controller
from .handoffs import summary
from .histories import produce_histories_native
from .prepare import _capture, capture_error, configuration, encode, prepare_native, request
from .serve import serve_native


def _effects(result):
    """Held scalar outcomes only; never render bulk domain graphs for DEBUG."""
    return {
        "failed": result["failed"],
        "complete": result["complete"],
        "pending_work": result.get("pending_work"),
        "requests": result.get("requests"),
        "stages": {
            name: {
                key: item for key, item in body.items() if type(item) in (int, float, bool, str) or item is None
            }
            for name, body in result["stages"].items()
            if isinstance(body, dict)
        },
    }


def _run(value, *, pricing_client=None, identity_client=None, progress=None):
    debug = value["debug"]
    operation_id = uuid4().hex
    stage = "prepare"
    with controller(value["store"]):
        try:
            if debug["enabled"]:
                _capture(debug, operation_id, stage, "input", encode(value))
            handoff = prepare_native(
                value,
                pricing_client=pricing_client,
                identity_client=identity_client,
                progress=progress,
                operation_id=operation_id,
            )
            if debug["enabled"]:
                captured = encode(summary(handoff))
                _capture(debug, operation_id, stage, "output", captured)
                _capture(debug, operation_id, stage, "effects", encode(_effects(handoff.prepared["result"])))
                _capture(debug, operation_id, "histories", "input", captured)
            stage = "histories"
            handoff = produce_histories_native(handoff, progress=progress)
            if debug["enabled"]:
                captured = encode(summary(handoff))
                _capture(debug, operation_id, stage, "output", captured)
                _capture(debug, operation_id, stage, "effects", encode(_effects(handoff.histories["result"])))
                _capture(debug, operation_id, "serve", "input", captured)
            stage = "serve"
            result = serve_native(handoff, progress=progress)
            del handoff
            if debug["enabled"]:
                _capture(debug, operation_id, stage, "output", encode(result))
                _capture(debug, operation_id, stage, "effects", encode(_effects(result)))
            return result
        except (RuntimeError, OSError, ValueError) as error:
            capture_error(debug, operation_id, stage, error)
            raise


def run_native(value, *, pricing_client=None, identity_client=None, progress=None):
    return _run(
        configuration(value), pricing_client=pricing_client, identity_client=identity_client, progress=progress
    )


def run(data, *, pricing_client=None, identity_client=None, progress=None):
    """Public JSON invocation/result adapter; stage handoffs remain native."""
    return encode(
        _run(request(data), pricing_client=pricing_client, identity_client=identity_client, progress=progress)
    )
