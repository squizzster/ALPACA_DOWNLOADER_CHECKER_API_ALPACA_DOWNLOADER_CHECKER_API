"""Controller exclusion, without domain observations or acceptance caches."""

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

from libs.common.store_lock import exclusive_owned, operation_lock

_controller = ContextVar("pipeline_controller_store", default=None)


def controller_owned(store):
    return _controller.get() == Path(store).resolve() and exclusive_owned(store)


@contextmanager
def controller(store):
    """Adopt the CLI's real gate, or hold one across the complete native pipeline."""
    if exclusive_owned(store):
        token = _controller.set(Path(store).resolve())
        try:
            yield
        finally:
            _controller.reset(token)
    else:
        with operation_lock(store, exclusive=True):
            with controller(store):
                yield


@contextmanager
def stage_lock(store):
    """Standalone stages own a gate; coordinated stages borrow the controller's."""
    if controller_owned(store):
        yield True
    else:
        with operation_lock(store, exclusive=True):
            yield False
