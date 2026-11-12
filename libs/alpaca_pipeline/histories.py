"""Independent history production with a transient, forward-only serving handoff."""

from pathlib import Path

from libs.alpaca_lineage_build import universe_state, universe_update
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.check_summary import generation_handoff
from libs.common.work_scope import selected_work

from .codec import Decoder, Encoder, HandoffError
from .execution import stage_lock
from .prepare import (
    SCHEMA_VERSION,
    StageExecutionError,
    _capture,
    assert_prepared_current,
    encode,
    load_prepared,
    operation_configuration,
    parse,
    validate_stage_result,
)
from .state import export_recovery, export_serving_context, import_serving_context, restore_recovery


def produce_histories(data, *, pricing_client=None, identity_client=None, progress=None):
    """Consume Prepare once; finish histories without entering serving.

    The output records a completed attempt. Independent failed groups remain
    explicit and history_complete is false. Failure to establish the completion
    boundary raises instead of authorizing downstream work. No stage is replayed.
    """
    value, configuration, prepared, decoder, pricing_client, identity_client = load_prepared(
        data, pricing_client=pricing_client, identity_client=identity_client
    )
    op = value["operation"]
    store = Path(configuration["store"])
    debug = configuration["debug"]
    _capture(debug, op["id"], "histories", "input", data)
    del data  # DEBUG already observed the argument; retain no duplicate archive.
    try:
        with stage_lock(store) as coordinated, universe_state.update_lock(store), operation(store):
            if not coordinated:
                assert_prepared_current(store, prepared)
            restore_recovery(store, value["prepared"]["recovery"], decoder.decode)
            with (
                selected_work(store, prepared["storage_symbols"]) as scope,
                universe_state.mutation_handoff() as mutations,
                generation_handoff(store, prepared["accepted"]["summary"]["token"]) as summary,
            ):
                histories = universe_update.histories_locked(
                    store,
                    op["end_exclusive"],
                    prepared,
                    pricing_client=pricing_client,
                    identity_client=identity_client,
                    registry=Path(configuration["registry"]),
                    workers=configuration["options"]["workers"],
                    progress=progress,
                    metadata_started=identity_client.request_count - value["requests"]["metadata"],
                    pricing_started=None
                    if value["requests"]["pricing"] is None
                    else pricing_client.request_count - value["requests"]["pricing"],
                )
                if histories is None:
                    raise StageExecutionError(
                        prepared["result"].get("error", "history completion boundary unavailable"),
                        prepared["result"],
                    )
                histories["result"]["execution_complete"] = True
                # Actual dependency expansion belongs to the active scope. It
                # must cross JSON, rather than be reconstructed by serving.
                histories["storage_symbols"] = scope.symbols
                histories["workflow_revision"] = mutations.get("revision", prepared["workflow_revision"])
                histories["summary_token"] = summary["token"]
                transport = Encoder()
                body = {key: transport.encode(item) for key, item in histories.items()}
                body["context"] = export_serving_context(prepared["context"], transport.encode)
                body["recovery"] = export_recovery(store, transport.encode)
                # All remaining consumers now have explicit records. Release
                # predecessor bytes/object tables and spent production caches
                # before allocating the complete outgoing JSON document.
                del prepared, decoder, value
                output = encode(
                    {
                        "schema": "alpaca.pipeline.histories",
                        "schema_version": SCHEMA_VERSION,
                        "operation": op,
                        "histories": body,
                        "objects": transport.objects,
                    }
                )
        _capture(debug, op["id"], "histories", "output", output)
        if debug["enabled"]:
            _capture(
                debug,
                op["id"],
                "histories",
                "effects",
                encode(
                    {
                        "stages": histories["result"]["stages"],
                        "requests": histories["result"]["requests"],
                        "history_complete": histories["history_complete"],
                    }
                ),
            )
        return output
    except (RuntimeError, OSError, ValueError) as error:
        if debug["enabled"]:
            try:
                _capture(
                    debug,
                    op["id"],
                    "histories",
                    "error",
                    encode(
                        {
                            "schema": "alpaca.pipeline.error",
                            "schema_version": SCHEMA_VERSION,
                            "operation_id": op["id"],
                            "stage": "histories",
                            "error_type": type(error).__name__,
                            "message": str(error),
                        }
                    ),
                )
            except OSError as capture_error:
                error.add_note("DEBUG error capture failed: " + str(capture_error))
        raise


def consume_histories(data, *, progress=None):
    """Compatibility name for Phase 2 consumers of the serving implementation."""
    from .serve import serve

    return serve(data, progress=progress)


def load_histories(data):
    """Restore the complete serving argument without store access or providers."""
    value = parse(data)
    if (
        set(value) != {"schema", "schema_version", "operation", "histories", "objects"}
        or value["schema"] != "alpaca.pipeline.histories"
        or type(value["schema_version"]) is not int
        or value["schema_version"] != SCHEMA_VERSION
    ):
        raise HandoffError("expected completed histories document")
    op = value["operation"]
    configuration = operation_configuration(op)
    store = Path(configuration["store"])
    body = value["histories"]
    if not isinstance(body, dict) or set(body) != {
        "result",
        "history_complete",
        "storage_symbols",
        "workflow_revision",
        "summary_token",
        "context",
        "recovery",
    }:
        raise HandoffError("incomplete histories handoff")
    decoder = Decoder(value["objects"])
    context = import_serving_context(store, body["context"], decoder.decode)
    histories = {
        name: decoder.decode(body[name])
        for name in ("result", "history_complete", "storage_symbols", "workflow_revision", "summary_token")
    }
    validate_stage_result(histories["result"], op["end_exclusive"], histories=True)
    if (
        type(histories["history_complete"]) is not bool
        or type(histories["workflow_revision"]) is not int
        or not isinstance(histories["storage_symbols"], set)
        or any(type(symbol) is not str or not symbol for symbol in histories["storage_symbols"])
        or not isinstance(histories["summary_token"], tuple)
        or len(histories["summary_token"]) != 2
        or not isinstance(histories["summary_token"][0], str)
        or type(histories["summary_token"][1]) is not int
    ):
        raise HandoffError("missing completed history attempt")
    # Decode every required carried value before locking or production effects.
    recovery = body["recovery"]
    if not isinstance(recovery, dict) or set(recovery) != {"store", "values"} or recovery["store"] != str(store):
        raise HandoffError("invalid histories recovery binding")
    if not isinstance(decoder.decode(recovery["values"]), dict):
        raise HandoffError("invalid histories recovery values")
    return value, store, histories, context, decoder, recovery
