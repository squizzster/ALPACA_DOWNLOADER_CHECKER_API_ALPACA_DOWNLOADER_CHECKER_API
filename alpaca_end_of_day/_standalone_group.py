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


def raw_source_presence(store, sources, generations, queries, *, payload_cache=None):
    """Selected raw source/date observations for the history producer.

    Caller holds the selected pricing locks. Source manifests already select the
    payload inventory; no assignment, upstream authentication or other view is
    required to establish these raw-presence facts.
    """
    from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

    result, cache = {}, {}
    for name, days in sorted(queries.items()):
        source = sources[name]
        anchor = source["anchor_month"]
        kinds = {
            "completed_years"
            if day[:4] < anchor[:4]
            else "current_year"
            if day[:7] < anchor
            else "current_month"
            for day in days
            if day[:7] <= anchor
        }
        present = set()
        for kind in sorted(kinds):
            for row in _rows(store / "end_of_day/pricing" / name, source, kind, "raw", cache, payload_cache):
                day = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                if day in days and (kind != "current_year" or day[:7] < anchor):
                    present.add(day)
        result[name] = {
            "pricing_generation": generations[name],
            "checked_sessions": sorted(days),
            "present_sessions": sorted(present),
        }
    return result


def dated_intervals(receipts, unavailable=()):
    """Intersect the completed member decisions in the locked snapshot."""
    if not receipts:
        return {}
    selections = next(iter(receipts.values())).get("dated_price_intervals", {})
    return {
        pid: item
        for pid, item in selections.items()
        if set(item["members"]) == receipts.keys()
        and set(item["unavailable"]) == set(unavailable)
        and all(
            receipt.get("dated_price_intervals", {}).get(pid, {}).get("token") == item["token"]
            for receipt in receipts.values()
        )
    }


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
        self._evidence = None
        self._dated_intervals = dated_intervals(receipts, unavailable_pricing(receipts[symbol]))
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
        # Caller mutation must not alter the accepted private snapshot or the
        # selectors subsequently passed to serving/export consumers.
        return deepcopy(self._summary)

    def close(self):
        self._assignments = self._sources = self._cache = None
        self._all_assignments = self._receipts = None
        self._evidence = None
        if hasattr(self, "_transfer_date_rows"):
            self._transfer_date_rows.clear()

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
        if self._evidence is None:
            self._evidence = _group_evidence(
                self._assignments, self._sources, self._receipts, self._generations, self._summary["generation"]
            )
        return deepcopy(self._evidence)

    @property
    def assignments(self):
        if self._all_assignments is None:
            raise StandaloneReaderError("full assignments were not selected")
        return self._all_assignments

    def duplicate_source_slices(self):
        """Consume the selected group's completed dated ownership correction."""
        return next(iter(self._receipts.values())).get("duplicate_source_slices", {})

    def membership_rebinds(self):
        """Completed lineage transfers; serving owns only numerical composition."""
        return {
            name: receipt["membership_only_rebind"]
            for name, receipt in self._receipts.items()
            if receipt.get("membership_only_rebind") is not None
        }

    def dated_market_projections(self):
        """Completed label projections, independently of numerical validation."""
        return {
            name: receipt["dated_market_projection"]
            for name, receipt in self._receipts.items()
            if receipt.get("dated_market_projection") is not None
        }

    def numerical_source_revisions(self):
        return {
            name: receipt["numerical_source_revision"]
            for name, receipt in self._receipts.items()
            if receipt.get("numerical_source_revision") is not None
        }

    def numerical_source_equivalences(self):
        """Borrow completed equality projections from the selected receipts."""
        return {
            name: receipt["numerical_source_equivalence"]
            for name, receipt in self._receipts.items()
            if receipt.get("numerical_source_equivalence") is not None
        }

    def revised_observations(self, public_id, revision, *, days=None):
        """Consume held selected bundles, or assemble only an unfinished scope."""
        from libs.alpaca_lineage_build.recovery_handoffs import recall

        held = recall(self._reader.store, "source_choice_bundles", revision["id"])
        days = set(revision["sessions"][public_id]) if days is None else set(days)
        if held is not None and public_id in held:
            return {day: values for day, values in held[public_id].items() if day in days}
        from bisect import bisect_left

        from libs.alpaca_pricing_download.contract import COLUMNS, NEW_YORK, timestamp

        selected, cache, dates, frames = {day: {} for day in days}, {}, {}, {}

        def session(row):
            if row[0] not in dates:
                dates[row[0]] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            return dates[row[0]]

        for name, assignments in self.assignments.items():
            for adjustment, kind, segment in assignments:
                if (segment.get("security_identity") or {}).get("security_id") != public_id or segment.get(
                    "observation_selection"
                ) != "selected":
                    continue
                required = days & {day for day in days if segment["from"] <= day <= segment["to"]}
                if not required:
                    continue
                rows = _rows(
                    self._reader.store / "end_of_day/pricing" / name,
                    self._sources[name],
                    kind,
                    adjustment,
                    cache,
                    self._cache,
                )
                for day in sorted(required):
                    index = bisect_left(
                        rows, day, lo=segment["row_start"] - 1, hi=segment["row_end"], key=session
                    )
                    if index == segment["row_end"] or session(rows[index]) != day:
                        continue
                    if adjustment in selected[day]:
                        raise StandaloneReaderError("revised observation has multiple selected sources")
                    frame_key = id(rows), index, tuple(segment["observation_tickers"])
                    if frame_key not in frames:
                        frames[frame_key] = {
                            **dict(zip(COLUMNS, rows[index])),
                            "session": day,
                            "tickers": segment["observation_tickers"],
                        }
                    selected[day][adjustment] = {"bar": frames[frame_key]}
        if any(set(values) != set(ADJUSTMENTS) for values in selected.values()):
            raise StandaloneReaderError("revised observations lack their selected complete bundle")
        return selected

    def serving_inputs(self):
        """Compact published ownership runs, independent of physical rollover.

        Row positions include unresolved observations. This describes assignments;
        it neither recomputes ownership nor authenticates the selected prices.
        """
        inputs, offsets = {}, {}
        for symbol, assignments in self.assignments.items():
            completed = self._receipts[symbol].get("serving_ownership")
            if completed is not None:
                inputs[symbol] = {
                    "pricing_generation": self._expected[symbol]["pricing"],
                    "views": completed["views"],
                }
                offsets[symbol] = completed["offsets"]
                continue
            if set(self._receipts[symbol].get("assignment_handoff_kinds", ())) == set(
                self._receipts[symbol]["boundaries"]
            ):
                raise StandaloneReaderError(
                    "completed lineage lacks serving ownership; resume lineage publication"
                )
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
                            [
                                identity.get("security_id"),
                                segment.get("observation_tickers", [segment.get("historical_symbol")]),
                            ]
                            if segment["branch"] == "correlated"
                            and segment.get("observation_selection") != "superseded"
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

    def observation_choice_handoffs(self):
        """Borrow explicit lineage choices independently of serving ownership."""
        from libs.alpaca_lineage_build.checkpoint_choices import copied, project

        return copied(project(self._receipts, self._generations))

    def preserves_pricing_prefix(self, symbol, previous, *, through=None):
        if not hasattr(self, "_accepted_pricing_prefixes"):
            self._accepted_pricing_prefixes = {}
        key = symbol, id(previous), through
        if key not in self._accepted_pricing_prefixes:
            self._accepted_pricing_prefixes[key] = (
                previous,
                self._preserves_pricing_prefix(symbol, previous, through=through),
            )
        return self._accepted_pricing_prefixes[key][1]

    def _preserves_pricing_prefix(self, symbol, previous, *, through=None):
        # The pricing producer owns this change ancestry. Consume its compact
        # changed-from guarantee; DO NOT compare old payload bytes or rebuild the
        # history to prove it again. Missing ancestry cannot authorize reuse.
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

    def retained_pricing_insertions(self, symbol, previous, *, through):
        """Borrow pricing's selected insertions under the accepted ancestor binding."""
        from libs.alpaca_pricing_download.insertions import retained_insertions

        if not hasattr(self, "_pricing_insertions"):
            self._pricing_insertions = {}
        key = symbol, previous["pricing_generation"], through
        if key not in self._pricing_insertions:
            self._pricing_insertions[key] = retained_insertions(self._sources[symbol], key[1], through=through)
        return self._pricing_insertions[key]

    def historical_label_intervals(self):
        return getattr(self._assignments, "historical_labels", {})

    def price_basis_limits(self):
        from libs.alpaca_lineage_build.price_basis_limits import group_limits

        return group_limits(self._receipts, self._unavailable, self._dated_intervals)

    def dated_price_intervals(self):
        return deepcopy(self._dated_intervals)

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
            **(
                {"dated_interval": self._dated_intervals[public_id]}
                if public_id in self._dated_intervals
                else {}
            ),
        )
        return views, coverage

    def serving_session_dates(self, public_id):
        """Owning legacy projection: assemble dates from accepted raw positions.

        Numerical outcomes, ownership and overlap compatibility remain accepted.
        This does not compare prices, calculate coverage order or select owners.
        """
        from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

        dates, cache, selected_rows = set(), {}, {}
        for symbol, assignments in self._assignments.items():
            for adjustment, kind, segment in assignments:
                if (
                    adjustment != "raw"
                    or segment["security_identity"]["security_id"] != public_id
                    or segment.get("observation_selection") == "superseded"
                ):
                    continue
                key = symbol, kind
                if key not in selected_rows:
                    selected_rows[key] = _rows(
                        self._reader.store / "end_of_day/pricing" / symbol,
                        self._sources[symbol],
                        kind,
                        "raw",
                        cache,
                        self._cache,
                    )
                rows = selected_rows[key]
                dates.update(
                    timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                    for row in rows[segment["row_start"] - 1 : segment["row_end"]]
                )
        return sorted(dates)

    def _serving_transferred_session_dates(self, runs, offsets):
        """Assemble an accepted whole donor's raw dates after transfer selection.

        The serving owner has accepted generation, positions and labels. Read
        only the named raw partitions and never repeat their numerical checks.
        Keep partition assembly for the lifetime of this locked group snapshot.
        """
        from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

        if self._assignments is None:
            raise StandaloneReaderError("group snapshot is closed")
        if not hasattr(self, "_transfer_date_rows"):
            self._transfer_date_rows = {}
        dates = set()
        for run in runs:
            symbol, kind = run["source"], run["boundary"]
            rows = _rows(
                self._reader.store / "end_of_day/pricing" / symbol,
                self._sources[symbol],
                kind,
                "raw",
                self._transfer_date_rows,
                self._cache,
            )
            offset = offsets[symbol]["raw"][kind]
            dates.update(
                timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                for row in rows[run["row_start"] - offset - 1 : run["row_end"] - offset]
            )
        return sorted(dates)

    def latest_pricing_revision(self, symbol, previous):
        """Lend the selected producer's localized revision, without payload reads."""
        from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

        source = self._sources[symbol]
        correction = source.get("latest_session_correction")
        if not correction or not correction.get("previous_session"):
            return None
        generation = previous["pricing_generation"]
        if generation not in {correction["source_sha256"], *correction.get("unchanged_ancestors", ())}:
            return None
        day = timestamp(correction["session"]).astimezone(NEW_YORK).date().isoformat()
        if not any(
            item["source_sha256"] == generation and item["changed_from"] == day
            for item in source.get("history_changes", ())
        ):
            return None
        return {
            "session": day,
            "preserves_timestamps": correction.get("preserves_timestamps", False),
            "previous_session": timestamp(correction["previous_session"])
            .astimezone(NEW_YORK)
            .date()
            .isoformat(),
        }

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

    def _selected_gap_presence(self, queries, observed_raw):
        """Complete outstanding all-view source/date observations after a component change."""
        from bisect import bisect_left, bisect_right

        from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

        present, cache, sessions = set(), {}, {}

        def day_of(row):
            if row[0] not in sessions:
                sessions[row[0]] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            return sessions[row[0]]

        for name, dates in queries.items():
            source = self._sources[name]
            raw = observed_raw.setdefault(name, {})
            present.update(day for day in dates if raw.get(day))
            by_view = {}
            for view, kind, segment in self.assignments[name]:
                by_view.setdefault(view, []).append((kind, segment["from"], segment["to"]))
            for view in ADJUSTMENTS:
                needed = sorted(dates - present - (raw.keys() if view == "raw" else set()))
                if not needed:
                    continue
                partitions = {}
                found = set()
                for kind, first, last in by_view.get(view, ()):
                    left, right = bisect_left(needed, first), bisect_right(needed, last)
                    for day in needed[left:right]:
                        if kind == "current_year" and day[:7] >= source["anchor_month"]:
                            continue
                        if day == first or day == last:
                            found.add(day)
                        else:
                            partitions.setdefault(kind, set()).add(day)
                for kind, possible in partitions.items():
                    unresolved = possible - found
                    if not unresolved:
                        continue
                    root = self._reader.store / "end_of_day/pricing" / name
                    rows = _rows(
                        root, source, kind, view, cache, self._cache if self._cache.maximum_bytes else None
                    )
                    for day in unresolved:
                        index = bisect_left(rows, day, key=day_of)
                        if index < len(rows) and day_of(rows[index]) == day:
                            found.add(day)
                if view == "raw":
                    raw.update((day, day in found) for day in needed)
                present.update(found)
        return present

    def source_raw_presence(self, queries):
        if self._sources is None:
            raise StandaloneReaderError("group snapshot is closed")
        return raw_source_presence(
            self._reader.store,
            self._sources,
            self.references["pricing"],
            queries,
            payload_cache=self._cache if self._cache.maximum_bytes else None,
        )

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
                minimum_session=start.isoformat() if start else None,
                maximum_session=end.isoformat() if end else None,
                **(
                    {"dated_interval": self._dated_intervals[public_id]}
                    if public_id in self._dated_intervals
                    else {}
                ),
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

    def check_current(self):
        """Check current selections and prices without reopening approved lineage."""

        # Closing releases the original locks. This later check must acquire a
        # fresh coherent selection, including formerly unavailable inputs; the
        # retained token is not itself a lock. DO NOT reread accepted assignments
        # or price payloads: only their current selectors can invalidate the token.
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


class ExportGroup:
    """Borrow selected rows under the catalogue gate for one export group."""

    def __init__(self, store, assignments, sources, intervals=None, receipts=None):
        self._selected_receipts = receipts
        self._dated_intervals = intervals or {}
        self._store, self._assignments, self._sources = store, assignments, sources

    def close(self):
        self._assignments = self._sources = self._selected_receipts = None

    def bounded_acquisition_views(self, public_id, first, last, *, modes=ADJUSTMENTS, cache=None):
        from datetime import date

        return _read_views(
            self._assignments,
            self._sources,
            self._store,
            public_id,
            modes,
            date.fromisoformat(first),
            date.fromisoformat(last),
            io_cache={} if cache is None else cache,
            dated_interval=self._dated_intervals.get(public_id),
            minimum_session=first,
            maximum_session=last,
        )

    def dated_acquisition_inputs(self, public_id, first, cut, ranges, *, carried=False, same_label=False):
        """Lend the earlier bars and the first traded successor witnesses once."""
        from datetime import date, timedelta
        from decimal import Decimal

        from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval

        labels = [r for r in ranges if r["first_session"] <= cut <= r["last_session"]]
        expected_start = first.isoformat() if same_label else cut
        if len(labels) != 1 or labels[0]["first_session"] != expected_start:
            raise UnsupportedInterval("select an unambiguous dated ticker range")
        cache = {}

        def read(modes, lower, upper):
            return _read_views(
                self._assignments,
                self._sources,
                self._store,
                public_id,
                modes,
                lower,
                upper,
                io_cache=cache,
                dated_interval=self._dated_intervals.get(public_id),
                minimum_session=lower.isoformat(),
                maximum_session=upper.isoformat(),
            )

        opening = date.fromisoformat(cut)
        end = date.fromisoformat(labels[0]["last_session"])
        if opening <= first:
            raise UnsupportedInterval("selected rename lacks an earlier published session")
        end_prior = opening - timedelta(days=1)
        if carried:
            from libs.alpaca_pricing_download.carried_rename import preceding_scope

            raw = preceding_scope(first, end_prior, lambda lo, hi: read(("raw",), lo, hi)["raw"]["bars"])
            first = date.fromisoformat(raw[0]["session"])
        else:
            raw = read(("raw",), first, end_prior)["raw"]["bars"]
        witness_days = []
        lower = opening
        while lower <= end and len(witness_days) < 3:
            upper = min(end, lower + timedelta(days=6))
            for bar in read(("raw",), lower, upper)["raw"]["bars"]:
                raw.append(bar)
                if (
                    bar["tickers"] == [labels[0]["ticker"]]
                    and min(Decimal(bar["volume"]), Decimal(bar["trade_count"])) > 0
                ):
                    witness_days.append(bar["session"])
                    if len(witness_days) == 3:
                        break
            lower = upper + timedelta(days=1)
        if len(witness_days) < 2:
            raise UnsupportedInterval("insufficient traded successor sessions")
        views = read(
            tuple(mode for mode in ADJUSTMENTS if mode != "raw"), first, date.fromisoformat(witness_days[-1])
        )
        views["raw"] = {"bars": raw, "bar_count": len(raw)}
        return views

    def _iter_export_histories(
        self, public_ids, *, start=None, end=None, online_starts=None, online_windows=None
    ):
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
                lower = (online_starts or {}).get(public_id, start)
                physical_scope = (
                    {"minimum_session": lower.isoformat()}
                    if online_starts and public_id in online_starts
                    else {}
                )
                try:

                    def read(first, last):
                        scope = physical_scope
                        if online_windows and public_id in online_windows:
                            scope = {"minimum_session": first.isoformat(), "maximum_session": last.isoformat()}
                        return _read_views(
                            self._assignments,
                            self._sources,
                            self._store,
                            public_id,
                            ADJUSTMENTS,
                            first,
                            last,
                            io_cache=io_cache,
                            **scope,
                            **(
                                {"dated_interval": self._dated_intervals[public_id]}
                                if public_id in self._dated_intervals
                                else {}
                            ),
                        )

                    if online_windows and public_id in online_windows:
                        from ._online_price_spans import assemble

                        views = assemble(ADJUSTMENTS, online_windows[public_id], read)
                    else:
                        views = read(lower, end)
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
