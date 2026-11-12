"""Carry selected recovery metadata to later consumers in one operation."""

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path

_selected = ContextVar("lineage_recovery_handoffs", default=None)


@contextmanager
def operation(store):
    token = _selected.set((Path(store).absolute(), {}))
    try:
        yield
    finally:
        _selected.reset(token)


def remember(store, kind, key, value):
    selected = _selected.get()
    if selected is not None and selected[0] == Path(store).absolute():
        selected[1][kind, key] = deepcopy(value)
    return value


def recall(store, kind, key):
    selected = _selected.get()
    if selected is None or selected[0] != Path(store).absolute():
        return None
    value = selected[1].get((kind, key))
    return deepcopy(value) if value is not None else None


def remember_partition(store, source, kind, generation, views):
    """Lend producer-decoded immutable rows under their selected boundary token.

    Consumers still select the current source under their lock. A changed
    boundary cannot match this key. This is operation-local data assembly, with
    no payload authentication or historical acceptance calculation.
    """
    selected = _selected.get()
    if selected is None or selected[0] != Path(store).absolute():
        return views
    from .assignment_handoff import _FrozenDict

    key = "pricing_partition", (source, kind, generation)
    if key not in selected[1]:
        selected[1][key] = _FrozenDict(
            {view: rows if isinstance(rows, tuple) else tuple(rows) for view, rows in views.items()}
        )
    return selected[1][key]


def append_partitions(store, symbols):
    """Lend already-materialized partitions and their publication documents.

    The append owner selects its current source under its lock and uses only an
    exact store/path/boundary-generation match. Changed boundaries require new
    assembly; a new consumer alone never authorizes rereading these held rows.
    """
    selected = _selected.get()
    store = Path(store).absolute()
    if selected is None or selected[0] != store:
        return {}
    symbols, result = set(symbols), {}
    for (record_kind, key), views in selected[1].items():
        if record_kind != "pricing_partition" or key[0] not in symbols:
            continue
        symbol, kind, generation = key
        document = selected[1].get(("pricing_partition_document", key))
        if document is None:
            from .contract import LineageError

            raise LineageError(f"{symbol}: materialized pricing partition lacks its publication document")
        root = store / "end_of_day" / "pricing" / symbol
        result[str(root), kind, generation] = (views, document)
    return result


def lineage_published(store, journal, receipts=None):
    """Retain frozen journal output across recovery and serving in this operation."""
    selected = _selected.get()
    if selected is None or selected[0] != Path(store).absolute():
        return
    from .assignment_handoff import accept_observation_choices

    for name, entry in journal["index_updates"].items():
        receipt = (receipts or {}).get(name)
        if receipt is None:
            receipt = journal["documents"].get(f"{name}/lineage.manifest.json")
        if receipt is None:
            continue
        for kind, handoff in receipt.get("assignment_handoffs", {}).items():
            choices = recall(store, "choices", (name, kind, handoff["generation"]))
            if choices is not None:
                handoff["observation_choices"] = choices
        # Fresh output already carries immutable accepted choices. A recovered
        # JSON journal crosses this schema ingress once, before its consumers.
        accept_observation_choices(receipt)
        remember(store, "receipt", (name, entry["sha256"]), receipt)
        for kind, binding in receipt["boundaries"].items():
            body = journal["documents"].get(f"{name}/{binding['manifest']}")
            if body is not None:
                remember(store, "assignment", (name, kind, binding["sha256"]), body)


def serving_published(store, before, after, replaced):
    """A committed selector advances unchanged selected bodies, retiring changed ones."""
    selected = _selected.get()
    if selected is None or selected[0] != Path(store).absolute():
        return
    values = selected[1]
    for (kind, key), value in tuple(values.items()):
        if kind == "serving_group" and key[0] == before:
            values.pop((kind, key))
            if key[1] not in replaced:
                values[kind, (after, key[1])] = value
