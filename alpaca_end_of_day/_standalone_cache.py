"""Bounded, generation-selected RAM reuse; never an on-disk cache."""

import json
import sys
from collections import OrderedDict
from threading import RLock

from libs.alpaca_lineage_build.contract import LineageError, selected_document
from libs.alpaca_pricing_download.contract import decode_csv, json_bytes


def _size(value):
    """Conservative retained size: shared immutable children may be counted twice."""
    return sys.getsizeof(value) + (sum(_size(item) for item in value) if isinstance(value, tuple) else 0)


def _rows_size(rows):
    """Conservative Unicode/tuple bound without walking every cell in Python.

    CSV decoding guarantees equally sized tuples of strings. Charge every
    string the largest Unicode header and four bytes per character, even for
    ASCII/shared strings. This overcounts instead of weakening the RAM bound.
    """
    if not rows:
        return sys.getsizeof(rows)
    return (
        sys.getsizeof(rows)
        + len(rows) * (sys.getsizeof(rows[0]) + len(rows[0]) * sys.getsizeof("\U0010ffff"))
        + 4 * sum(len("".join(row)) for row in rows)
    )


class ReadLedger:
    """In-memory counters only; never an audit file or Parquet field."""

    def __init__(self):
        self._counts = {}
        self._lock = RLock()

    def record(self, name, amount=1):
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + amount

    def info(self):
        with self._lock:
            return dict(self._counts)


def _file_key(kind, path, generation):
    # The producer's selected generation is authoritative, including on first use.
    return kind, str(path), generation


class GenerationCache:
    def __init__(self, maximum_bytes, *, ledger=None):
        if type(maximum_bytes) is not int or maximum_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        self.maximum_bytes = maximum_bytes
        self.used_bytes = 0
        self.hits = self.misses = 0
        self._entries = OrderedDict()
        self._lock = RLock()
        self.ledger = ledger if ledger is not None else ReadLedger()

    def resize(self, maximum_bytes):
        if type(maximum_bytes) is not int or maximum_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        with self._lock:
            self.maximum_bytes = maximum_bytes
            while self.used_bytes > maximum_bytes:
                _, (_, size) = self._entries.popitem(last=False)
                self.used_bytes -= size
                self.ledger.record("evictions")

    def _load(self, key, loader, *, measure=_size):
        with self._lock:
            if key in self._entries:
                self.hits += 1
                self.ledger.record("cache_hits")
                self._entries.move_to_end(key)
                return self._entries[key][0]
            self.misses += 1
            self.ledger.record("cache_misses")
            value = loader()
            if not self.maximum_bytes:
                return value
            # Include keys, entry bookkeeping and OrderedDict node headroom.
            size = measure(value) + _size(key) + 512
            if size <= self.maximum_bytes:
                while self.used_bytes + size > self.maximum_bytes:
                    _, (_, old_size) = self._entries.popitem(last=False)
                    self.used_bytes -= old_size
                    self.ledger.record("evictions")
                self._entries[key] = (value, size)
                self.used_bytes += size
            return value

    def document(self, path, generation):
        if not isinstance(generation, str) or len(generation) != 64:
            raise LineageError("invalid selected lineage generation")
        if not self.maximum_bytes:
            return selected_document(path, generation)

        # Bytes are privately retained; each caller gets independent mutable JSON.
        # Published generation equality is authority, not an mtime or a new audit.
        def load():
            packed = json_bytes(selected_document(path, generation))
            self.ledger.record("cached_file_loads")
            self.ledger.record("cached_file_bytes", len(packed))
            return packed

        packed = self._load(_file_key("document", path, generation), load)
        return json.loads(packed)

    def rows(self, path, generation):
        if not isinstance(generation, str) or len(generation) != 64:
            raise LineageError("invalid selected pricing generation")

        # Decoded tuples and strings are immutable. Aliases share this exact entry.
        def load():
            packed = path.read_bytes()
            self.ledger.record("cached_file_loads")
            self.ledger.record("cached_file_bytes", len(packed))
            return decode_csv(packed)

        return self._load(_file_key("rows", path, generation), load, measure=_rows_size)

    def info(self):
        with self._lock:
            return {
                "maximum_bytes": self.maximum_bytes,
                "used_bytes": self.used_bytes,
                "entries": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "ledger": self.ledger.info(),
            }
