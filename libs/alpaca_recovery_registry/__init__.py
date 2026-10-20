"""Additive recovery labels and immutable build inputs; never security identity."""

import csv
import fcntl
import io
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

from libs.alpaca_pricing_download.contract import symbol_label

VERSION = 1
DEFAULT_REGISTRY = Path(__file__).resolve().parents[2] / "resources/ticker.csv"


def validate_version():
    path = DEFAULT_REGISTRY.with_name("alpaca_recovery_registry_change_log.json")
    entries = json.loads(path.read_bytes())
    if len(entries) != VERSION or any("version" in item or not item.get("modifications") for item in entries):
        raise ValueError("recovery registry changelog/version mismatch")


def load_labels(path=DEFAULT_REGISTRY):
    with Path(path).open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream, strict=True))
    if len(rows) != 1 or not rows[0]:
        raise ValueError("recovery universe must contain exactly one nonempty CSV record")
    labels = tuple(symbol_label(value) for value in rows[0])
    if labels != tuple(sorted(set(labels))):
        raise ValueError("recovery universe must be sorted and unique")
    return labels


def encode_labels(labels):
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerow(sorted({symbol_label(value) for value in labels}))
    return output.getvalue().encode()


def atomic_bytes(path, data):
    """Caller owns serialization. Replacement covers process interruption."""
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + ".tmp.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def registry_lock(path=DEFAULT_REGISTRY):
    path = Path(path)
    with path.with_name("." + path.name + ".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def publish_union(labels, path=DEFAULT_REGISTRY):
    validate_version()
    path = Path(path)
    additions = {symbol_label(value) for value in labels}
    with registry_lock(path):
        previous = set(load_labels(path))
        added = sorted(additions - previous)
        if added:
            atomic_bytes(path, encode_labels(previous | additions))
    return {"added": added, "label_count": len(previous | additions)}
