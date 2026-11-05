"""Fail closed on per-ticker storage work outside an accepted update selection.

Guard owner storage entry points without inspecting the filesystem or turning
estimated request counts into arbitrary execution budgets.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

from .debug_trace import event

_active = ContextVar("alpaca_store_work_scopes", default=())


class UnexpectedStoreWork(BaseException):
    """Abort the operation, bypassing recoverable per-symbol failure handlers."""


class WorkScope:
    def __init__(self, store, symbols, phase):
        self.store = None if store is None else Path(store).absolute()
        self.symbols = set(symbols)
        self.phase = phase
        self.failure = None

    def stop(self, owner, symbol):
        if self.failure is None:
            self.failure = (
                f"unplanned {owner} storage access for {symbol!r} during {self.phase}; "
                "stopped before access; inspect the selecting code path before restarting"
            )
            event("unexpected_store_work", phase=self.phase, owner=owner, symbol=symbol)
        self.require_running()

    def require_running(self):
        if self.failure is not None:
            raise UnexpectedStoreWork(self.failure)


@contextmanager
def selected_work(store, symbols, *, phase="selected_update"):
    scope = WorkScope(store, symbols, phase)
    token = _active.set((*_active.get(), scope))
    try:
        yield scope
        scope.require_running()  # A swallowed worker failure cannot become success.
    finally:
        _active.reset(token)


def compact_selection(function):
    @wraps(function)
    def call(*args, **kwargs):
        with selected_work(None, (), phase=function.__name__):
            return function(*args, **kwargs)

    return call


def check_path(path):
    scopes = _active.get()
    if not scopes:
        return
    for scope in scopes:
        scope.require_running()
    path = Path(path).absolute()  # Lexical only: no resolve/stat/path inventory.
    parts = path.parts
    for offset in range(len(parts) - 2):
        if parts[offset] != "end_of_day":
            continue
        owner, symbol = parts[offset + 1 : offset + 3]
        if owner not in {"pricing", "manifests", "lineage"}:
            continue
        if owner == "lineage" and symbol in {
            ".worker.lock",
            ".pending.json",
            "..pending.json.writing",
            "index.sqlite3",
            "index.sqlite3-journal",
            "index.sqlite3-wal",
            "index.sqlite3-shm",
        }:
            continue  # Fixed compact controls, not a receipt or assignment body.
        store = Path(*parts[:offset])
        for scope in scopes:
            if scope.store != store or symbol not in scope.symbols:
                scope.stop(owner, symbol)


def forbid_inventory(store):
    for scope in _active.get():
        scope.stop("directory inventory", str(Path(store).absolute()))


def accept_expansion(store, symbols, *, anchors, authority):
    """Called only where a completed metadata/serving selector expands a group."""
    store, symbols, anchors = Path(store).absolute(), set(symbols), set(anchors)
    for scope in _active.get():
        scope.require_running()
        if scope.store != store or not anchors.intersection(scope.symbols):
            scope.stop(authority, min(symbols, default="<empty>"))
    # Validate every enclosing scope before changing any of them. A compact
    # child must not accidentally authorize its parent's next storage access.
    for scope in _active.get():
        added = symbols - scope.symbols
        if added:
            scope.symbols.update(added)
            event("selected_work_expansion", authority=authority, added=len(added), selected=len(scope.symbols))
