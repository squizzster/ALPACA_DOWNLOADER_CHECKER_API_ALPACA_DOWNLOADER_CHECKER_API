"""Serving stage: complete histories JSON in, published data-operation JSON out."""

from libs.alpaca_lineage_build import storage, universe_state, universe_update
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.check_summary import assert_generation
from libs.alpaca_serving_catalogue.storage import selector_state
from libs.common.work_scope import selected_work

from .codec import HandoffError
from .execution import stage_lock
from .histories import load_histories
from .prepare import SCHEMA_VERSION, _capture, encode
from .state import restore_recovery


def assert_histories_current(store, histories, context):
    """Establish applicability only across a real standalone lock gap."""
    if universe_state.revision(store) != histories["workflow_revision"]:
        raise HandoffError("stale_handoff: workflow changed")
    assert_generation(store, histories["summary_token"])
    with storage.publication_lock(context.accept_store().root):
        storage.assert_index_current(context.accept_store().root, context.index)
    if selector_state(store)["generation"] != context.serving["state"]["generation"]:
        raise HandoffError("stale_handoff: serving generation changed")


def serve(data, *, progress=None):
    """Consume a completed attempt without providers or upstream stage calls.

    The whole input is decoded before locks and effects. The controller lends
    only its actual exclusion; every accepted domain fact comes from the JSON.
    A standalone call checks selected applicability after acquiring its gate.
    Lost delivery errors; it never requests reconstruction or stage replay.
    """
    try:
        value, store, histories, context, decoder, recovery = load_histories(data)
    except (KeyError, TypeError, AttributeError) as error:
        raise HandoffError("incomplete histories contract") from error
    op = value["operation"]
    debug = op["request"]["debug"]
    _capture(debug, op["id"], "serve", "input", data)
    del data
    try:
        with stage_lock(store) as coordinated, universe_state.update_lock(store), operation(store):
            if not coordinated:
                assert_histories_current(store, histories, context)
            restore_recovery(store, recovery, decoder.decode)
            with selected_work(store, histories["storage_symbols"]):
                result = universe_update.serve_locked(store, histories, context=context, progress=progress)
                # Serving has consumed the last bulk-data handoff. Its final
                # result is compact and does not echo partitions or assignments.
                del value, histories, context, decoder, recovery
                output = encode(result)
        _capture(debug, op["id"], "serve", "output", output)
        if debug["enabled"]:
            _capture(
                debug,
                op["id"],
                "serve",
                "effects",
                encode(
                    {
                        "serving_catalogue": result["stages"]["serving_catalogue"],
                        "complete": result["complete"],
                        "failed": result["failed"],
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
                    "serve",
                    "error",
                    encode(
                        {
                            "schema": "alpaca.pipeline.error",
                            "schema_version": SCHEMA_VERSION,
                            "operation_id": op["id"],
                            "stage": "serve",
                            "error_type": type(error).__name__,
                            "message": str(error),
                        }
                    ),
                )
            except OSError as capture_error:
                error.add_note("DEBUG error capture failed: " + str(capture_error))
        raise
