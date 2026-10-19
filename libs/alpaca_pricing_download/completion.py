"""Optional completed-batch memo; deleting it falls back to the source receipts."""

import os
import uuid
from pathlib import Path

from .contract import VERSION, json_bytes
from .storage import read_document, regular_path, write_file
from .update_checks import read_checks

COMPLETION_FILE = "download_completion.json"


def request_key(symbols, end, start):
    return {"version": VERSION, "symbols": symbols, "end_exclusive": end, "from": start}


def completed_batch(store, request):
    path = regular_path(Path(store).absolute(), COMPLETION_FILE)
    if not path.exists():
        return False
    value = read_document(path)
    state = read_checks(path.parent, ())
    return (
        state["epoch"] is not None
        and value.get("request") == request
        and value.get("epoch") == state["epoch"]
        and value.get("revision") == state["revision"]
    )


def remember_batch(store, request, before, events):
    """Publish at successful exit only if no other publisher intervened.

    Each pricing mutation queues its event before becoming visible. Comparing the
    complete interval of event numbers also excludes concurrent publishers whose
    changes could supersede an earlier result in this batch.
    """
    store = Path(store).absolute()
    after = read_checks(store, ())
    if after["epoch"] is None or before["epoch"] not in (None, after["epoch"]):
        return
    if any(epoch != after["epoch"] for epoch, _ in events):
        return
    start = before["revision"] if before["epoch"] is not None else 0
    if sorted(revision for _, revision in events) != list(range(start + 1, after["revision"] + 1)):
        return
    value = {"request": request, "epoch": after["epoch"], "revision": after["revision"]}
    target = regular_path(store, COMPLETION_FILE)
    if target.exists() and target.read_bytes() == json_bytes(value):
        return
    temporary = regular_path(store, ".download_completion." + uuid.uuid4().hex)
    try:
        write_file(temporary, json_bytes(value))
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
