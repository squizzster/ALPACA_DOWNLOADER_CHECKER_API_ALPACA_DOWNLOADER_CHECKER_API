"""Registration ownership for standalone commands that can discover dependencies."""

from contextlib import contextmanager
from pathlib import Path

from libs.alpaca_recovery_registry import DEFAULT_REGISTRY, load_labels

from . import storage, universe_state
from .context import accept_context
from .work_contract import RegistrationIncomplete, WorkPolicy, restriction, satisfying_entry


def command_policy(store, end_date, *, state=None, context=None):
    """Read compact authorities only; command choice does not override restrictions."""
    from libs.alpaca_pricing_download.update_checks import read_checks

    store = Path(store).absolute()
    state = universe_state.read_state(store) if state is None else state
    root = store / "end_of_day/lineage"
    if context is None or not root.exists():
        index = storage.read_index(root)
    else:
        with storage.publication_lock(root):
            index = context.select_index(root)
    pending = read_checks(store, readonly=True)["checks"]
    work = {s: item for s, item in state["work"].items() if item["status"] != "complete"}
    deferred = [
        {"symbol": s, "reason": reason}
        for s, item in work.items()
        if not satisfying_entry(s, item, index["symbols"], pending) and (reason := restriction(item, end_date))
    ]
    return WorkPolicy(store, index, work, deferred, end_date, context=context)


@contextmanager
def standalone_registration(store, end_date, *, registry=DEFAULT_REGISTRY):
    """Require explicit initialization; serialize with daily update orchestration."""
    context = accept_context(store)
    store = Path(store).absolute()
    storage.check_store(store)
    with universe_state.update_lock(store):
        state = universe_state.read_state(store)
        policy = command_policy(store, end_date, state=state, context=context)
        registered = set(load_labels(registry))
        if state["registration_pending"]:
            universe_state.register(store, registry=registry)
            registered.update(state["registration_pending"])

        def register(labels):
            additions = set(labels) - registered
            if additions:
                try:
                    universe_state.register(store, additions, registry=registry)
                except (RuntimeError, OSError, ValueError) as error:
                    raise RegistrationIncomplete(str(error)) from error
                registered.update(additions)

        yield register, policy
        policy.acknowledge()
