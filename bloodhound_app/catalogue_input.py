"""Consume the public catalogue export stream without an intermediate format."""

from contextlib import contextmanager
from dataclasses import replace

import numpy as np
import pandas as pd

if __package__:
    from .published_labels import ticker_labels
else:
    from published_labels import ticker_labels

FIELDS = ("open", "high", "low", "close", "volume", "trade_count", "vwap")


class CatalogueHistories:
    def __init__(self, exported, root, history_type, unique_histories):
        self.exported = exported
        self.descriptors = unique_histories(
            [
                history_type(
                    root,
                    {},
                    None,
                    public_id,
                    ",".join(security.tickers),
                    {},
                )
                for public_id, security in exported.securities.items()
            ]
        )
        self._by_id = {history.public_id: history for history in self.descriptors}
        self.catalogue = exported.catalogue.to_dict()

    def __len__(self):
        return len(self.descriptors)

    def __iter__(self):
        for item in self.exported.iter_histories(on_error="record"):
            yield replace(self._by_id[item.security.public_id], catalogue_history=item)


@contextmanager
def open_histories(args, history_type, unique_histories):
    # Applications use only the supported public boundary. The engine owns
    # selectors, group locking, source reuse and history assembly for both CSV
    # and Parquet. No source-store path discovery belongs in this adapter.
    from alpaca_end_of_day import DataLibrary

    data = DataLibrary(
        store=args.store,
        wait=getattr(args, "wait", False),
        cache_bytes=getattr(args, "cache_bytes", 0),
    )
    public_ids = getattr(args, "public_id", None)
    symbols = getattr(args, "symbol", None)
    if symbols:
        resolved = set()
        for symbol in symbols:
            matches = data.resolve_ticker(symbol)
            if not matches:
                raise ValueError("ticker is not available")
            resolved.update(match.public_id for match in matches)
        if public_ids and not set(public_ids) <= resolved:
            raise ValueError("requested public ID is not available in the selected tickers")
        public_ids = list(public_ids) if public_ids else sorted(resolved)
    # Factors consume the full accepted online history before date filtering.
    with data.open_history_export(public_ids=public_ids) as exported:
        yield CatalogueHistories(exported, data.store, history_type, unique_histories)


def load(history, metadata, args, validate_frame):
    """Convert held decimal strings once; retain accepted grid/label alignment."""
    item = history.catalogue_history
    if item.error is not None:
        raise ValueError(str(item.error)) from item.error
    sessions = np.asarray([row["session"] for row in item.views["raw"]["bars"]], dtype=str)
    symbols = ticker_labels(item.ticker_ranges, sessions)
    base = pd.DataFrame({"session": sessions, "symbol": symbols}, copy=False)
    history.projected_metadata = metadata.columns(
        history.public_id, sessions, base, args.unknown_security_type, args.unknown_exchange
    )
    symbols = history.projected_metadata["symbol"]
    frames = {
        name: validate_frame(
            pd.DataFrame(
                {
                    "session": sessions,
                    "symbol": symbols,
                    **{
                        field: np.asarray([row[field] for row in view["bars"]], dtype=np.float64)
                        for field in FIELDS
                    },
                },
                copy=False,
            ),
            history.directory,
            history.public_id,
            accepted_session_grid=True,
        )
        for name, view in item.views.items()
    }
    return frames, [
        {
            "catalogue_generation": item.catalogue_generation,
            "public_id": item.security.public_id,
            "source_authority": "completed_online_catalogue_export",
            "ticker_label_authority": "completed_catalogue_ticker_ranges",
        }
    ]


class OutputTables:
    """Bound completed Arrow output, then serialize once in stable series order."""

    def __init__(self, memory_bytes):
        self.maximum_bytes = memory_bytes
        self.retained_limit = memory_bytes - min(2 * 1024**3, memory_bytes // 4)
        self.used_bytes = 0
        self.tables = []

    def add(self, identifier, table):
        charged = table.get_total_buffer_size() + 32768
        if self.used_bytes + charged > min(self.maximum_bytes, self.retained_limit):
            raise MemoryError("Parquet output exceeds --memory-mib; no histories omitted or source rereads")
        self.used_bytes += charged
        self.tables.append((identifier, table))

    def write(self, writer):
        import pyarrow as pa

        writer.write_table(
            pa.concat_tables([table for _, table in sorted(self.tables, key=lambda item: item[0])]),
            row_group_size=131072,
        )

    def clear(self):
        self.tables.clear()
        self.used_bytes = 0
