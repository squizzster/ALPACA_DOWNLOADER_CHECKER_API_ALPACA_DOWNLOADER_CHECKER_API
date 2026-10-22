"""One trusted group consumption, followed by a compact freshness token."""

from copy import deepcopy

from libs.alpaca_lineage_build.acquisition import check_unavailable_pricing, unavailable_pricing
from libs.alpaca_lineage_build.contract import LineageBusyError, document_digest
from libs.alpaca_lineage_build.storage import read_index, reader_snapshot
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.storage import load_source, published_path, read_update

from ._standalone_cache import GenerationCache
from .standalone import (
    ReaderBusyError,
    StandaloneReaderError,
    _date,
    _group_evidence,
    _group_securities,
    _public_id,
    _read_views,
    _rows,
)


class GroupSnapshot:
    def __init__(
        self,
        reader,
        symbol,
        assignments,
        sources,
        generation,
        receipts,
        generations,
        all_assignments=None,
        *,
        shared_cache=False,
    ):
        self._reader = reader
        self._assignments, self._sources = assignments, sources
        self._all_assignments = all_assignments
        self._receipts, self._generations = receipts, generations
        # Keep the requested symbol: it selects the receipt's unavailable-input set.
        self._unavailable = tuple(sorted(unavailable_pricing(receipts[symbol])))
        self._cache = (
            reader._cache
            if shared_cache
            else GenerationCache(reader._cache.maximum_bytes, ledger=reader._cache.ledger)
        )
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
            }
            for name, source in sources.items()
        }

    @property
    def summary(self):
        return deepcopy(self._summary)

    def close(self):
        self._assignments = self._sources = self._cache = None
        self._all_assignments = self._receipts = None

    @property
    def references(self):
        return {
            "lineage": {
                **{n: e["lineage"] for n, e in self._expected.items()},
                **dict.fromkeys(self._unavailable),
            },
            "pricing": {n: e["pricing"] for n, e in self._expected.items()},
        }

    @property
    def evidence(self):
        if self._assignments is None:
            raise StandaloneReaderError("group snapshot is closed")
        return _group_evidence(
            self._assignments, self._sources, self._receipts, self._generations, self._summary["generation"]
        )

    @property
    def assignments(self):
        if self._all_assignments is None:
            raise StandaloneReaderError("full assignments were not selected")
        return self._all_assignments

    def serving_inputs(self):
        """Compact published ownership runs, independent of physical rollover.

        Row positions include unresolved observations. This describes assignments;
        it neither recomputes ownership nor authenticates the selected prices.
        """
        inputs, offsets = {}, {}
        for symbol, assignments in self.assignments.items():
            views, view_offsets = {}, {}
            for adjustment in ADJUSTMENTS:
                runs, count, last, boundaries = [], 0, None, {}
                for kind in ("completed_years", "current_year", "current_month"):
                    boundaries[kind] = count
                    for view, boundary, segment in assignments:
                        if view != adjustment or boundary != kind:
                            continue
                        identity = segment.get("security_identity") or {}
                        owner = (
                            [identity.get("security_id"), segment.get("historical_symbol")]
                            if segment["branch"] == "correlated"
                            else [None, None]
                        )
                        size = segment["row_count"]
                        if runs and runs[-1]["owner"] == owner:
                            runs[-1]["rows"] += size
                        else:
                            runs.append({"owner": owner, "rows": size})
                        count += size
                        last = max(last or segment["to"], segment["to"])
                views[adjustment] = {"rows": count, "last_session": last, "ownership": runs}
                view_offsets[adjustment] = boundaries
            inputs[symbol] = {"pricing_generation": self._expected[symbol]["pricing"], "views": views}
            offsets[symbol] = view_offsets
        return inputs, offsets

    def preserves_pricing_prefix(self, symbol, previous, *, through=None):
        source = self._sources[symbol]
        if previous["pricing_generation"] == self._expected[symbol]["pricing"]:
            return True
        ancestor = next(
            (
                item
                for item in source.get("history_changes", ())
                if item["source_sha256"] == previous["pricing_generation"]
            ),
            None,
        )
        if ancestor is None:
            return False
        if through is not None:
            return ancestor["changed_from"] is None or ancestor["changed_from"] > through
        return ancestor["changed_from"] is None or all(
            view["last_session"] is None or ancestor["changed_from"] > view["last_session"]
            for view in previous["views"].values()
        )

    def serving_histories(self, public_id, *, prefix=None):
        """One shared assembly, with a producer's completed prefix when selected."""
        coverage = {}
        views = _read_views(
            self._assignments,
            self._sources,
            self._reader.store,
            public_id,
            ADJUSTMENTS,
            None,
            None,
            self._cache if self._cache.maximum_bytes else None,
            prefix=prefix,
            coverage_out=coverage,
        )
        return views, coverage

    def source_sessions(self, *, start=None, end=None):
        """Selected gap evidence across every published view, including unresolved rows."""
        from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

        if self._sources is None:
            raise StandaloneReaderError("group snapshot is closed")
        cache, result = {}, set()
        for name, source in self._sources.items():
            root = self._reader.store / "end_of_day/pricing" / name
            selected = {
                (adjustment, kind)
                for adjustment, kind, segment in self.assignments[name]
                if (start is None or segment["to"] >= start) and (end is None or segment["from"] <= end)
            }
            for adjustment, kind in sorted(selected):
                for row in _rows(
                    root, source, kind, adjustment, cache, self._cache if self._cache.maximum_bytes else None
                ):
                    session = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                    if (
                        (kind != "current_year" or session[:7] < source["anchor_month"])
                        and (start is None or session >= start)
                        and (end is None or session <= end)
                    ):
                        result.add(session)
        return result

    def set_cache_limit(self, cache_bytes):
        """Adjust retained reuse between reads, within the reader's configured cap."""
        if self._cache is None:
            raise StandaloneReaderError("group snapshot is closed")
        if type(cache_bytes) is not int or not 0 <= cache_bytes <= self._reader._cache.maximum_bytes:
            raise ValueError("cache_bytes must be within the configured reader cache budget")
        self._cache.resize(cache_bytes)

    def get_histories(self, *, public_id, adjustments=ADJUSTMENTS, start=None, end=None):
        from . import API_VERSION

        if self._assignments is None:
            raise StandaloneReaderError("group snapshot is closed")
        public_id = _public_id(public_id)
        if (
            not isinstance(adjustments, (tuple, list))
            or not adjustments
            or any(a not in ADJUSTMENTS for a in adjustments)
        ):
            raise ValueError("adjustments must be a nonempty list or tuple of supported views")
        adjustments = tuple(dict.fromkeys(adjustments))
        start, end = _date(start, "start"), _date(end, "end")
        if start is not None and end is not None and start > end:
            raise ValueError("start must not be after end")
        security = next((s for s in self._summary["securities"] if s["public_id"] == public_id), None)
        if security is None:
            raise StandaloneReaderError("public ID is unavailable for this group")
        if any(name not in security["adjustments"] for name in adjustments):
            raise StandaloneReaderError("adjustment is unavailable for this public ID")
        try:
            views = _read_views(
                self._assignments,
                self._sources,
                self._reader.store,
                public_id,
                adjustments,
                start,
                end,
                self._cache if self._cache.maximum_bytes else None,
            )
        except StandaloneReaderError:
            raise
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
            "range": {
                "start": start.isoformat() if start else None,
                "end": end.isoformat() if end else None,
                "inclusive": True,
            },
            "views": views,
            "evidence": self.summary,
        }

    def _iter_export_histories(self, public_ids, *, start=None, end=None):
        """Consume selected online IDs with one I/O memo under the group lock.

        Membership and row ownership come from completed selectors. The memo
        shares physical aliases across IDs, independently of the optional LRU.
        Drop each source after its last selected identity; assembled histories
        and their session scratch remain private to each identity.
        """
        if self._assignments is None:
            raise StandaloneReaderError("group snapshot is closed")
        positions = {public_id: position for position, public_id in enumerate(public_ids)}
        release_at = {}
        for symbol, assignments in self._assignments.items():
            last = max(
                (
                    positions.get(segment["security_identity"]["security_id"], -1)
                    for _, _, segment in assignments
                ),
                default=-1,
            )
            if last >= 0:
                release_at.setdefault(last, []).append(symbol)
        io_cache = {}
        try:
            for position, public_id in enumerate(public_ids):
                try:
                    views = _read_views(
                        self._assignments,
                        self._sources,
                        self._reader.store,
                        public_id,
                        ADJUSTMENTS,
                        start,
                        end,
                        io_cache=io_cache,
                    )
                except StandaloneReaderError:
                    raise
                except (OSError, RuntimeError, ValueError, KeyError, TypeError, StopIteration) as error:
                    raise StandaloneReaderError(f"cannot read standalone publication: {error}") from error
                yield public_id, views
                del views
                for symbol in release_at.get(position, ()):
                    io_cache.pop(symbol, None)
        finally:
            io_cache.clear()

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
                    root = published_path(self._reader.store, f"end_of_day/pricing/{name}")
                    if name not in snapshot["locked"] or published_path(root, ".pending.json").exists():
                        raise StandaloneReaderError("pricing publication incomplete; resume downloader first")
                    if read_update(root) is not None:
                        raise StandaloneReaderError("selected pricing group changed; retry export")
                    # The lock was released. Read only the current selector; the
                    # earlier digest cannot establish whether a publisher advanced it.
                    source = load_source(root, name)
                    self._reader._cache.ledger.record("source_receipt_reads")
                    if (
                        source is None
                        or document_digest(source) != expected["pricing"]
                        or source["boundaries"] != expected["boundaries"]
                        or source["anchor_month"] != expected["anchor"]
                    ):
                        raise StandaloneReaderError("selected pricing group changed; retry export")
        except LineageBusyError as error:
            raise ReaderBusyError(str(error)) from error
        except StandaloneReaderError:
            raise
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            raise StandaloneReaderError(f"cannot check standalone publication: {error}") from error
