"""Serving stage: complete histories JSON in, published data-operation JSON out."""

from libs.alpaca_lineage_build import storage, universe_state, universe_update
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.check_summary import assert_generation
from libs.alpaca_serving_catalogue.storage import selector_state
from libs.common.work_scope import selected_work

from .codec import HandoffError
from .execution import stage_lock
from .handoffs import Histories
from .histories import load_histories
from .prepare import _capture, capture_error, encode
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


def serve_native(handoff, *, progress=None):
    """Consume completed values without providers, transport or upstream work."""
    if not isinstance(handoff, Histories):
        raise HandoffError("expected completed native histories")
    histories, context = handoff.histories, handoff.context
    try:
        histories["result"], histories["history_complete"], histories["storage_symbols"]
    except (KeyError, TypeError) as error:
        raise HandoffError("incomplete native histories") from error
    store = context.store
    with stage_lock(store) as coordinated, universe_state.update_lock(store), operation(store):
        if not coordinated:
            assert_histories_current(store, histories, context)
        restore_recovery(store, handoff.recovery, lambda item: item)
        with selected_work(store, histories["storage_symbols"]):
            return universe_update.serve_locked(store, histories, context=context, progress=progress)


def serve(data, *, progress=None):
    """Standalone JSON adapter around the same native serving implementation."""
    try:
        value, store, histories, context, decoder, recovery = load_histories(data)
    except (KeyError, TypeError, AttributeError) as error:
        raise HandoffError("incomplete histories contract") from error
    op = value["operation"]
    debug = op["request"]["debug"]
    _capture(debug, op["id"], "serve", "input", data)
    handoff = Histories(
        op, histories, context, {"store": recovery["store"], "values": decoder.decode(recovery["values"])}
    )
    del data, value, histories, context, decoder, recovery
    try:
        result = serve_native(handoff, progress=progress)
        del handoff
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
        capture_error(debug, op["id"], "serve", error)
        raise
