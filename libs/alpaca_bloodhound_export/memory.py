"""Accepted columnar output and read reuse share a bounded RAM allowance."""

from .projection import ExportError

DEFAULT_MEMORY_BYTES = 8 * 1024**3
_WORKING_HEADROOM = 2 * 1024**3
_TABLE_OVERHEAD = 32 * 1024


class ExportMemoryError(ExportError):
    """An operational memory limit aborts the export, never rejects an ID."""


class OutputMemory:
    """Retain Arrow buffers, not Python rows or a compressed Parquet buffer.

    Reserve headroom for active histories, metadata and writer workspace. Charge
    each newly built 18-column, single-chunk table for its complete Arrow buffers
    plus conservative object/schema overhead. This accounts retained data, not
    a hard operating-system RSS limit. Never spill to disk or reread old prices.
    """

    def __init__(self, maximum_bytes=DEFAULT_MEMORY_BYTES):
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise ValueError("memory_bytes must be a positive integer")
        self.maximum_bytes = maximum_bytes
        self.retained_limit = maximum_bytes - min(_WORKING_HEADROOM, maximum_bytes // 4)
        self.data_bytes = self.used_bytes = 0
        self._tables = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.clear()

    @property
    def available_bytes(self):
        return self.retained_limit - self.used_bytes

    def add(self, table):
        size = table.get_total_buffer_size()
        charge = size + _TABLE_OVERHEAD
        if charge > self.available_bytes:
            raise ExportMemoryError(
                "accepted output exceeds the retained RAM budget; "
                "no identities omitted, no disk spill and no file published"
            )
        self._tables.append(table)
        self.data_bytes += size
        self.used_bytes += charge

    def table(self):
        import pyarrow as pa

        # Concatenation preserves order and shares existing buffers. In particular,
        # do not combine_chunks(), which would copy the full retained dataset.
        return pa.concat_tables(self._tables)

    def clear(self):
        self._tables.clear()
        self.data_bytes = self.used_bytes = 0
