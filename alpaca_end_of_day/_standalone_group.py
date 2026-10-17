"""One trusted group consumption, followed by a compact freshness token."""

from copy import deepcopy

from libs.alpaca_lineage_build.acquisition import check_unavailable_pricing, unavailable_pricing
from libs.alpaca_lineage_build.contract import LineageBusyError, document_digest
from libs.alpaca_lineage_build.storage import read_index, reader_snapshot
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

from ._standalone_cache import GenerationCache, _file_key
from .standalone import (
    ReaderBusyError,
    StandaloneReaderError,
    _group_securities,
    _public_id,
    _read_views,
)


class GroupSnapshot:
    def __init__(self, reader, symbol, assignments, sources, generation, receipts, generations):
        self._reader, self._symbol = reader, symbol
        self._assignments, self._sources = assignments, sources
        self._unavailable = tuple(sorted(unavailable_pricing(receipts[symbol])))
        self._cache = GenerationCache(reader._cache.maximum_bytes, ledger=reader._cache.ledger)
        self._summary = {
            "generation": generation,
            "source_symbols": sorted(sources),
            "securities": _group_securities(assignments),
            "sources": {
                name: {
                    "lineage_generation": generations[name],
                    "pricing_generation": receipts[name]["source_sha256"],
                    "pricing_request": source["latest_request"],
                    "history_start": source["history_start"],
                    "processed_through": receipts[name]["processed_through"],
                }
                for name, source in sources.items()
            },
        }
        # These private values came from the completed, locked snapshot above.
        # Keep only its selecting references after closing, never bars/evidence.
        self._expected = {
            name: {
                "lineage": generations[name],
                "pricing": receipts[name]["source_sha256"],
                "boundaries": deepcopy(source["boundaries"]),
                "anchor": source["anchor_month"],
                "source_file": _file_key(
                    "pricing",
                    regular_path(reader.store, f"end_of_day/pricing/{name}") / "source.manifest.json",
                    receipts[name]["source_sha256"],
                ),
            }
            for name, source in sources.items()
        }

    @property
    def summary(self):
        return deepcopy(self._summary)

    def close(self):
        self._assignments = self._sources = self._cache = None

    def set_cache_limit(self, cache_bytes):
        """Adjust retained reuse between reads, within the reader's configured cap."""
        if self._cache is None:
            raise StandaloneReaderError("group snapshot is closed")
        if type(cache_bytes) is not int or not 0 <= cache_bytes <= self._reader._cache.maximum_bytes:
            raise ValueError("cache_bytes must be within the configured reader cache budget")
        self._cache.resize(cache_bytes)

    def get_histories(self, *, public_id):
        from . import API_VERSION

        if self._assignments is None:
            raise StandaloneReaderError("group snapshot is closed")
        public_id = _public_id(public_id)
        security = next((s for s in self._summary["securities"] if s["public_id"] == public_id), None)
        if security is None:
            raise StandaloneReaderError("public ID is unavailable for this group")
        if any(name not in security["adjustments"] for name in ADJUSTMENTS):
            raise StandaloneReaderError("adjustment is unavailable for this public ID")
        try:
            views = _read_views(
                self._assignments,
                self._sources,
                self._reader.store,
                public_id,
                ADJUSTMENTS,
                None,
                None,
                self._cache if self._cache.maximum_bytes else None,
            )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, StopIteration) as error:
            raise StandaloneReaderError(f"cannot read standalone publication: {error}") from error
        return {
            "api_version": API_VERSION,
            "boundary": "standalone_resolved_pricing_views",
            "generation": self._summary["generation"],
            "symbol": security["read_symbol"],
            "security": {
                key: deepcopy(value)
                for key, value in security.items()
                if key not in {"latest_tickers", "read_symbol"}
            },
            "range": {"start": None, "end": None, "inclusive": True},
            "views": views,
            "evidence": self.summary,
        }

    def check_current(self):
        """Check current selections and prices without reopening approved lineage."""

        def select(root):
            selected = {name: read_index(root, name)["symbols"].get(name) for name in self._expected}
            if any(
                entry is None or entry["sha256"] != self._expected[name]["lineage"]
                for name, entry in selected.items()
            ):
                raise StandaloneReaderError("selected lineage group changed; retry export")
            selected.update({name: read_index(root, name)["symbols"].get(name) for name in self._unavailable})
            if any(selected[name] is not None for name in self._unavailable):
                raise StandaloneReaderError("previously unavailable lineage input changed; retry export")
            return selected

        try:
            with reader_snapshot(
                self._reader.store,
                select,
                wait=self._reader.wait,
                include_boundaries=False,
                include_receipts=False,
            ) as snapshot:
                check_unavailable_pricing(self._reader.store, self._unavailable, snapshot["locked"])
                for name, expected in self._expected.items():
                    root = regular_path(self._reader.store, f"end_of_day/pricing/{name}")
                    if name not in snapshot["locked"] or regular_path(root, ".pending.json").exists():
                        raise StandaloneReaderError("pricing publication incomplete; resume downloader first")
                    if read_update(root) is not None:
                        raise StandaloneReaderError("selected pricing group changed; retry export")
                    identity = _file_key("pricing", root / "source.manifest.json", expected["pricing"])
                    if identity == expected["source_file"]:
                        self._reader._cache.ledger.record("source_receipt_reuses")
                        continue
                    source = load_source(root, name)
                    self._reader._cache.ledger.record("changed_source_receipt_reads")
                    if (
                        source is None
                        or document_digest(source) != expected["pricing"]
                        or source["boundaries"] != expected["boundaries"]
                        or source["anchor_month"] != expected["anchor"]
                    ):
                        raise StandaloneReaderError("selected pricing group changed; retry export")
                    expected["source_file"] = identity
        except LineageBusyError as error:
            raise ReaderBusyError(str(error)) from error
        except StandaloneReaderError:
            raise
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            raise StandaloneReaderError(f"cannot check standalone publication: {error}") from error
