"""Prepare JSON boundary and its actual downstream-consumer feasibility adapter."""

import json
import math
import os
from datetime import date
from pathlib import Path
from uuid import uuid4

from libs.alpaca_lineage_build import storage, universe_state, universe_update
from libs.alpaca_lineage_build.context import accept_context
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.check_summary import assert_current
from libs.alpaca_pricing_download.download import pricing_session_end, request_end

from .codec import Decoder, Encoder, HandoffError
from .evidence import export_evidence, import_evidence
from .execution import stage_lock
from .state import export_recovery, export_state, import_state

VERSION = 7
SCHEMA_VERSION = 1


class StageExecutionError(HandoffError):
    """A failed attempt with a serialized report, never a successful handoff."""

    def __init__(self, message, result):
        super().__init__(message)
        self.result_json = encode(result)


def _runtime_binding():
    from libs.alpaca_end_of_day_security_correlation.correlation_version import RUNTIME_VERSION as correlation
    from libs.alpaca_lineage_build.contract import RUNTIME_VERSION as lineage
    from libs.alpaca_pricing_download.contract import RUNTIME_VERSION as pricing
    from libs.alpaca_serving_catalogue.contract import RUNTIME_VERSION as serving

    return {
        "protocol": VERSION,
        "correlation": correlation,
        "lineage": lineage,
        "pricing": pricing,
        "serving": serving,
    }


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise HandoffError("duplicate JSON key: " + key)
        value[key] = item
    return value


def _nonfinite(value):
    raise HandoffError("nonfinite JSON token: " + value)


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise HandoffError("nonfinite JSON number")
    return result


def parse(data):
    if not isinstance(data, bytes):
        raise HandoffError("pipeline arguments must be UTF-8 JSON bytes")
    try:
        value = json.loads(
            data.decode("utf-8"), object_pairs_hook=_object, parse_constant=_nonfinite, parse_float=_finite_float
        )
    except (ValueError, UnicodeError) as error:
        raise HandoffError("invalid pipeline JSON") from error
    if not isinstance(value, dict):
        raise HandoffError("pipeline document must be an object")
    return value


def encode(document):
    return json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def clients():
    from libs.alpaca_pricing_download.http import BarsClient
    from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient

    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    return BarsClient(key, secret), AlpacaIdentityClient(key, secret)


def request(data):
    value = parse(data)
    required = {"schema", "schema_version", "operation", "store", "registry", "through", "options", "debug"}
    if (
        set(value) - {"end_exclusive"} != required
        or value["schema"] != "alpaca.pipeline.request"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
        or value["operation"] != "daily_update"
    ):
        raise HandoffError("unsupported pipeline request")
    for name in ("store", "registry"):
        if not isinstance(value[name], str) or not Path(value[name]).is_absolute():
            raise HandoffError(name + " must be an absolute path")
    if value["through"] is not None and not isinstance(value["through"], str):
        raise HandoffError("through must be a date or null")
    if "end_exclusive" in value and (
        value["through"] is not None
        or not isinstance(value["end_exclusive"], str)
        or date.fromisoformat(value["end_exclusive"]).isoformat() != value["end_exclusive"]
    ):
        raise HandoffError("end_exclusive requires a canonical date and through null")
    options = value["options"]
    if not isinstance(options, dict) or set(options) - {"workers", "retry_blocked", "coalesce_late_bars"}:
        raise HandoffError("unsupported pipeline options")
    options = {"workers": 3, "retry_blocked": False, "coalesce_late_bars": True, **options}
    if (
        type(options["workers"]) is not int
        or options["workers"] < 1
        or any(type(options[k]) is not bool for k in ("retry_blocked", "coalesce_late_bars"))
    ):
        raise HandoffError("invalid pipeline options")
    debug = value["debug"]
    if (
        not isinstance(debug, dict)
        or type(debug.get("enabled")) is not bool
        or set(debug) - {"enabled", "directory"}
    ):
        raise HandoffError("invalid DEBUG configuration")
    if debug["enabled"] and (
        not isinstance(debug.get("directory"), str) or not Path(debug["directory"]).is_absolute()
    ):
        raise HandoffError("DEBUG requires an absolute capture directory")
    if debug["enabled"] and Path(debug["directory"]).resolve().is_relative_to(Path(value["store"]).resolve()):
        raise HandoffError("DEBUG capture directory must be outside the production store")
    return {**value, "options": options}


def _capture(configuration, operation_id, stage, kind, data):
    if configuration["enabled"]:
        directory = Path(configuration["directory"]) / operation_id
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / (stage + "." + kind + ".json")).open("xb") as stream:
            stream.write(data)


def prepare(data, *, pricing_client=None, identity_client=None, progress=None):
    """Complete preparation; return bytes only after production effects and locks finish.

    Client injection is internal test infrastructure, never part of public API.
    Normal execution creates no handoff files. Delivery failure does not roll
    back committed outputs and never selects automatic handoff reconstruction.
    """
    value = request(data)
    operation_id = uuid4().hex
    end = (
        (
            request_end(value["end_exclusive"])
            if "end_exclusive" in value
            else pricing_session_end(value["through"])
        )
        .date()
        .isoformat()
    )
    _capture(value["debug"], operation_id, "prepare", "input", data)
    try:
        if pricing_client is None and identity_client is None:
            pricing_client, identity_client = clients()
        if pricing_client is None or identity_client is None:
            raise HandoffError("both internal clients are required")
        store = Path(value["store"])
        context = accept_context(store)
        result = {"complete": False, "end_exclusive": end, "failed": 0, "stages": {}}
        metadata_started = identity_client.request_count
        pricing_started = getattr(pricing_client, "request_count", None)
        with stage_lock(store), universe_state.update_lock(store), operation(store):
            universe_state.require_initialized(store)
            prepared = universe_update.prepare_locked(
                store,
                end,
                pricing_client=pricing_client,
                identity_client=identity_client,
                registry=Path(value["registry"]),
                progress=progress,
                context=context,
                result=result,
                **value["options"],
            )
            if "policy" not in prepared:
                raise StageExecutionError("baseline preparation incomplete: " + str(prepared), prepared)
            transport = Encoder()
            body = {
                key: transport.encode(prepared[key])
                for key in (
                    "accepted",
                    "selection",
                    "registered",
                    "storage_symbols",
                    "maintenance_symbols",
                    "result",
                    "workflow_revision",
                )
            }
            body["evidence"] = export_evidence(prepared["evidence"], transport.encode)
            body["state"] = export_state(
                context, prepared["policy"], transport.encode, evidence=prepared["evidence"]
            )
            body["recovery"] = export_recovery(store, transport.encode)
            request_counts = {
                "metadata": identity_client.request_count - metadata_started,
                "pricing": None if pricing_started is None else pricing_client.request_count - pricing_started,
            }
            output = encode(
                {
                    "schema": "alpaca.pipeline.prepared",
                    "schema_version": SCHEMA_VERSION,
                    "operation": {
                        "id": operation_id,
                        "request": value,
                        "end_exclusive": end,
                        "runtime": _runtime_binding(),
                    },
                    "prepared": body,
                    "objects": transport.objects,
                    "requests": request_counts,
                }
            )
        if value["debug"]["enabled"]:
            _capture(value["debug"], operation_id, "prepare", "output", output)
            _capture(
                value["debug"],
                operation_id,
                "prepare",
                "effects",
                encode(
                    {
                        "stages": result["stages"],
                        "requests": request_counts,
                    }
                ),
            )
        return output
    except (RuntimeError, OSError, ValueError) as error:
        if value["debug"]["enabled"]:
            try:
                _capture(
                    value["debug"],
                    operation_id,
                    "prepare",
                    "error",
                    encode(
                        {
                            "schema": "alpaca.pipeline.error",
                            "schema_version": SCHEMA_VERSION,
                            "operation_id": operation_id,
                            "stage": "prepare",
                            "error_type": type(error).__name__,
                            "message": str(error),
                        }
                    ),
                )
            except OSError as capture_error:
                error.add_note("DEBUG error capture failed: " + str(capture_error))
        raise


def load_prepared(data, *, pricing_client=None, identity_client=None):
    """Restore the complete prepared argument before any lock or effects."""
    try:
        return _load_prepared(data, pricing_client=pricing_client, identity_client=identity_client)
    except (KeyError, TypeError, AttributeError) as error:
        raise HandoffError("incomplete prepared contract") from error


def operation_configuration(op):
    """Check the transport envelope, never recompute its frozen cutoff."""
    if not isinstance(op, dict) or op.get("runtime") != _runtime_binding():
        raise HandoffError("stale_handoff: pipeline implementation changed")
    if (
        set(op) != {"id", "request", "end_exclusive", "runtime"}
        or not isinstance(op["id"], str)
        or len(op["id"]) != 32
        or any(c not in "0123456789abcdef" for c in op["id"])
        or not isinstance(op["end_exclusive"], str)
        or date.fromisoformat(op["end_exclusive"]).isoformat() != op["end_exclusive"]
    ):
        raise HandoffError("invalid pipeline operation")
    return request(encode(op["request"]))


def validate_stage_result(result, end, *, histories=False):
    """Require the fields the next owner uses before that owner can publish."""
    if (
        not isinstance(result, dict)
        or result.get("complete") is not False
        or result.get("end_exclusive") != end
        or type(result.get("failed")) is not int
        or result["failed"] < 0
        or not isinstance(result.get("stages"), dict)
        or "serving_catalogue" in result["stages"]
    ):
        raise HandoffError("incomplete pipeline stage result")
    if histories and (
        result.get("execution_complete") is not True
        or not isinstance(result.get("failures"), list)
        or any(not isinstance(failure, dict) for failure in result["failures"])
        or type(result.get("pending_work")) is not int
        or result["pending_work"] < 0
        or not isinstance(result.get("requests"), dict)
        or "metadata_http_requests" not in result["requests"]
        or any(type(count) is not int or count < 0 for count in result["requests"].values())
    ):
        raise HandoffError("incomplete history attempt result")


def _load_prepared(data, *, pricing_client=None, identity_client=None):
    value = parse(data)
    if (
        set(value) != {"schema", "schema_version", "operation", "prepared", "objects", "requests"}
        or value["schema"] != "alpaca.pipeline.prepared"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
    ):
        raise HandoffError("expected completed prepared document")
    op = value["operation"]
    configuration = operation_configuration(op)
    store = Path(configuration["store"])
    decoder = Decoder(value["objects"])
    body = value["prepared"]
    expected = {
        "accepted",
        "selection",
        "registered",
        "storage_symbols",
        "maintenance_symbols",
        "result",
        "workflow_revision",
        "state",
        "evidence",
        "recovery",
    }
    if not isinstance(body, dict) or set(body) != expected:
        raise HandoffError("incomplete prepared handoff")
    counts = value["requests"]
    if (
        not isinstance(counts, dict)
        or set(counts) != {"metadata", "pricing"}
        or type(counts["metadata"]) is not int
        or counts["metadata"] < 0
        or counts["pricing"] is not None
        and (type(counts["pricing"]) is not int or counts["pricing"] < 0)
    ):
        raise HandoffError("invalid prepared request counts")
    # Complete input restoration precedes locks and any producer effects.
    if pricing_client is None and identity_client is None:
        pricing_client, identity_client = clients()
    if pricing_client is None or identity_client is None:
        raise HandoffError("both internal clients are required")
    evidence = import_evidence(body["evidence"], identity_client, decoder.decode)
    context, policy = import_state(store, body["state"], decoder.decode, evidence=evidence)
    evidence.work_policy = policy
    prepared = {key: decoder.decode(body[key]) for key in expected - {"state", "evidence", "recovery"}}
    prepared.update(context=context, policy=policy, evidence=evidence)
    validate_stage_result(prepared["result"], op["end_exclusive"])
    if (
        not isinstance(prepared["storage_symbols"], set)
        or not isinstance(prepared["maintenance_symbols"], set)
        or any(type(symbol) is not str or not symbol for symbol in prepared["storage_symbols"])
    ):
        raise HandoffError("incomplete prepared storage scope")
    recovery = body["recovery"]
    if (
        not isinstance(recovery, dict)
        or set(recovery) != {"store", "values"}
        or recovery["store"] != str(store)
        or not isinstance(decoder.decode(recovery["values"]), dict)
    ):
        raise HandoffError("invalid prepared recovery binding")
    end = op["end_exclusive"]
    if (
        context.store != store
        or policy is None
        or policy.end != end
        or prepared["accepted"]["store"] != str(store)
    ):
        raise HandoffError("inconsistent prepared operation binding")
    if (
        context._accepted_store is None
        or context._authorized_policy is None
        or not context._lineage_recovery_complete
        or any(item is None for item in (context.index, context.summary, context.serving))
        or type(prepared["workflow_revision"]) is not int
    ):
        raise HandoffError("missing completed baseline acceptance")
    if context._accepted_store.store != store or context._accepted_store.root != store / "end_of_day/lineage":
        raise HandoffError("accepted baseline belongs to another store")
    return value, configuration, prepared, decoder, pricing_client, identity_client


def assert_prepared_current(store, prepared):
    """Only establish applicability across the lock gap; do not repeat acceptance."""
    context = prepared["context"]
    if universe_state.revision(store) != prepared["workflow_revision"]:
        raise HandoffError("stale_handoff: workflow changed")
    assert_current(store, prepared["accepted"]["summary"]["token"])
    with storage.publication_lock(context.accept_store().root):
        storage.assert_index_current(context.accept_store().root, prepared["accepted"]["index"])
    from libs.alpaca_serving_catalogue.storage import selector_state

    if selector_state(store)["generation"] != context.serving["state"]["generation"]:
        raise HandoffError("stale_handoff: serving generation changed")


def consume_prepared(data, *, pricing_client=None, identity_client=None, progress=None):
    """Compatibility feasibility consumer, now crossing both actual JSON boundaries."""
    from .histories import consume_histories, produce_histories

    output = produce_histories(
        data, pricing_client=pricing_client, identity_client=identity_client, progress=progress
    )
    return consume_histories(output, progress=progress)
