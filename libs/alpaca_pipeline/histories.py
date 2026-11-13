"""Independent history production with a transient, forward-only serving handoff."""

from pathlib import Path

from libs.alpaca_lineage_build import universe_state, universe_update
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.check_summary import generation_handoff
from libs.common.work_scope import selected_work

from .codec import Decoder, Encoder, HandoffError
from .execution import stage_lock
from .handoffs import Histories, Prepared
from .prepare import (
    SCHEMA_VERSION,
    StageExecutionError,
    _capture,
    assert_prepared_current,
    capture_error,
    encode,
    load_prepared,
    operation_configuration,
    parse,
    validate_stage_result,
)
from .state import (
    export_recovery,
    export_serving_context,
    import_serving_context,
    restore_recovery,
    serving_context,
)


def produce_histories_native(handoff, *, progress=None):
    """Consume the complete native preparation; perform no transport work."""
    if not isinstance(handoff, Prepared):
        raise HandoffError("expected completed native preparation")
    op, prepared = handoff.operation, handoff.prepared
    configuration = op["request"]
    try:
        context = prepared["context"]
        prepared["evidence"], prepared["policy"], prepared["accepted"], prepared["storage_symbols"]
    except (KeyError, TypeError) as error:
        raise HandoffError("incomplete native preparation") from error
    pricing_client, identity_client = handoff.pricing_client, handoff.identity_client
    store = Path(configuration["store"])
    with stage_lock(store) as coordinated, universe_state.update_lock(store), operation(store):
        if not coordinated:
            assert_prepared_current(store, prepared)
        restore_recovery(store, handoff.recovery, lambda item: item)
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
                metadata_started=identity_client.request_count - handoff.requests["metadata"],
                pricing_started=None
                if handoff.requests["pricing"] is None
                else pricing_client.request_count - handoff.requests["pricing"],
            )
            if histories is None:
                raise StageExecutionError(
                    prepared["result"].get("error", "history completion boundary unavailable"),
                    prepared["result"],
                )
            histories["result"]["execution_complete"] = True
            histories["storage_symbols"] = scope.symbols
            histories["workflow_revision"] = mutations.get("revision", prepared["workflow_revision"])
            histories["summary_token"] = summary["token"]
            return Histories(op, histories, serving_context(context), export_recovery(store, lambda item: item))


def serialize_histories(handoff):
    """Standalone JSON compatibility adapter, outside ordinary execution."""
    transport = Encoder()
    body = {key: transport.encode(item) for key, item in handoff.histories.items()}
    body["context"] = export_serving_context(handoff.context, transport.encode)
    body["recovery"] = {
        "store": handoff.recovery["store"],
        "values": transport.encode(handoff.recovery["values"]),
    }
    return encode(
        {
            "schema": "alpaca.pipeline.histories",
            "schema_version": SCHEMA_VERSION,
            "operation": handoff.operation,
            "histories": body,
            "objects": transport.objects,
        }
    )


def produce_histories(data, *, pricing_client=None, identity_client=None, progress=None):
    """Standalone JSON history stage around the shared native implementation."""
    value, configuration, prepared, decoder, pricing_client, identity_client = load_prepared(
        data, pricing_client=pricing_client, identity_client=identity_client
    )
    op = value["operation"]
    debug = configuration["debug"]
    _capture(debug, op["id"], "histories", "input", data)
    recovery = value["prepared"]["recovery"]
    handoff = Prepared(
        op,
        prepared,
        value["requests"],
        {"store": recovery["store"], "values": decoder.decode(recovery["values"])},
        pricing_client,
        identity_client,
    )
    del data, value, prepared, decoder, recovery
    try:
        handoff = produce_histories_native(handoff, progress=progress)
        output = serialize_histories(handoff)
        _capture(debug, op["id"], "histories", "output", output)
        if debug["enabled"]:
            _capture(
                debug,
                op["id"],
                "histories",
                "effects",
                encode(
                    {
                        "stages": handoff.histories["result"]["stages"],
                        "requests": handoff.histories["result"]["requests"],
                        "history_complete": handoff.histories["history_complete"],
                    }
                ),
            )
        return output
    except (RuntimeError, OSError, ValueError) as error:
        capture_error(debug, op["id"], "histories", error)
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
