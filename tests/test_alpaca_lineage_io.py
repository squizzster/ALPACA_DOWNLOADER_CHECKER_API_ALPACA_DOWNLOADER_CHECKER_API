"""Reuse locked handoffs without hiding newer publications or interrupted work."""

import unittest
from collections import Counter
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as fixtures
from test_alpaca_lineage_batching import Provider, event
from test_alpaca_pricing_download import bar, snapshot

from libs.alpaca_lineage_build import check_summary, pipeline, storage, universe_state
from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import check_summary as summary
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import json_bytes
from libs.common.sqlite_connections import connect_sqlite


class LineageIOTests(unittest.TestCase):
    setUp = fixtures.LineageBuildTests.setUp
    download = fixtures.LineageBuildTests.download
    run_build = fixtures.LineageBuildTests.run_build
    receipt = fixtures.LineageBuildTests.receipt
    notice = fixtures.LineageBuildTests.notice

    def test_unchanged_progress_reuses_receipt_without_opening_boundaries(self):
        self.download()
        provider = Provider(["GOOG"])
        self.run_build(["GOOG"], client=provider.client)
        check_summary.rebuild(self.store)
        before = snapshot(self.store)
        opened, events = Counter(), []
        read = Path.read_bytes

        def counted(path):
            opened[path] += 1
            return read(path)

        provider.calls.clear()
        with (
            patch.object(Path, "read_bytes", counted),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("progress opened decisions")),
        ):
            result = self.run_build(["GOOG"], client=provider.client, on_processed=events.append)
        pricing = self.store / "end_of_day/pricing/GOOG"
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(events, [["GOOG"]])
        self.assertEqual(opened[self.root / "GOOG/lineage.manifest.json"], 1)
        self.assertEqual(opened[pricing / "source.manifest.json"], 1)  # Planning uses the accepted index.
        self.assertEqual(opened[pricing / "validator_first_last_sessions.json"], 1)
        self.assertEqual(provider.calls, [])
        self.assertEqual(before, snapshot(self.store))

    def test_publication_reuses_locked_sources_and_reads_old_lineage_only_once(self):
        self.download()
        self.run_build(["GOOG"])
        check_summary.rebuild(self.store)
        prior = self.receipt()
        pricing = self.store / "end_of_day/pricing/GOOG"
        frozen = snapshot(pricing)
        opened = Counter()
        read = Path.read_bytes

        def counted(path):
            opened[path] += 1
            return read(path)

        client = fixtures.IdentityClient({"GOOG": {**fixtures.asset("GOOG"), "name": "Updated company"}})
        with patch.object(Path, "read_bytes", counted):
            self.run_build(["GOOG"], client=client, refresh=True)
        self.assertEqual(opened[pricing / "source.manifest.json"], 1)
        self.assertEqual(opened[pricing / "validator_first_last_sessions.json"], 1)
        self.assertEqual(opened[self.root / "GOOG/lineage.manifest.json"], 1)
        for reference in prior["boundaries"].values():
            self.assertEqual(opened[self.root / "GOOG" / reference["manifest"]], 1)
        lookup = self.receipt()["evidence"]["asset_lookups"][0]
        self.assertEqual(lookup["asset"]["name"], "Updated company")
        self.assertEqual(summary.read_snapshot(self.store)["entries"][0]["symbol"], "GOOG")
        self.assertEqual(frozen, snapshot(pricing))

    def test_cache_tracks_publication_and_reloads_when_its_selected_generation_changes(self):
        self.download()
        self.run_build(["GOOG"])
        with storage.writer_lock(self.store) as root:
            index = storage.read_index(root)
            cache = storage.ReceiptCache(root, index)
            original = cache.read("GOOG")
            for name, supplied_cache in (("First update", cache), ("Second update", None)):
                client = fixtures.IdentityClient({"GOOG": {**fixtures.asset("GOOG"), "name": name}})
                result = pipeline.process_group(
                    self.store,
                    root,
                    "GOOG",
                    identity_client=client,
                    refresh=True,
                    index=index,
                    receipt_cache=supplied_cache,
                )
                self.assertEqual(result["status"], "processed")
                with patch.object(storage, "read_receipt", wraps=storage.read_receipt) as read:
                    current = cache.read("GOOG")
                    self.assertIs(current, cache.read("GOOG"))
                self.assertEqual(read.call_count, int(supplied_cache is None))
                self.assertNotEqual(original, current)
                self.assertEqual(current["evidence"]["asset_lookups"][0]["asset"]["name"], name)
                original = current
            index["symbols"].pop("GOOG")
            self.assertIsNone(cache.read("GOOG"))

    def test_receipt_cache_capacity_is_a_hard_bound(self):
        index = {
            "symbols": {
                symbol: {"sha256": symbol, "dependencies": [], "asset_ids": []} for symbol in ("A", "B", "C")
            }
        }
        with patch.object(storage, "read_receipt", side_effect=lambda _root, symbol, _index: {"symbol": symbol}):
            cache = storage.ReceiptCache(self.root, index, capacity=2)
            self.assertEqual(cache.read("A")["symbol"], "A")
            self.assertEqual(cache.read("B")["symbol"], "B")
            self.assertEqual(cache.read("C")["symbol"], "C")
            self.assertEqual(list(cache.receipts), ["B", "C"])
            self.assertEqual(cache.read("B")["symbol"], "B")
            self.assertEqual(list(cache.receipts), ["C", "B"])

    def test_selected_handoffs_need_no_filesystem_revalidation_or_authentication(self):
        self.download()
        self.run_build(["GOOG"])
        frozen = snapshot(self.store)
        index = storage.read_index(self.root)
        pricing = self.store / "end_of_day/pricing/GOOG"
        with storage.pricing_locks(self.store, ["GOOG"]):
            sources = pipeline._current_sources(self.store, ["GOOG"])
            with (
                patch.object(Path, "stat", side_effect=AssertionError("selected path rechecked")),
                patch.object(Path, "iterdir", side_effect=AssertionError("selected directory scanned")),
                patch.object(pricing_storage, "digest", side_effect=AssertionError("selected data rehashed")),
            ):
                receipt = storage.read_receipt(self.root, "GOOG", index)
                boundaries = storage.read_boundaries(self.root, receipt)
                inputs = pipeline.load_pricing(self.store, "GOOG", source=sources["GOOG"])
                entry = check_summary.project(self.store, receipt, source=sources["GOOG"])
                notice = pricing_storage.read_update(pricing, published=True)
        self.assertEqual(len(boundaries), 3)
        self.assertEqual(len(inputs.views["all"]), len(self.bars))
        self.assertEqual(entry["last_all"], list(inputs.views["all"][-1]))
        self.assertIsNone(notice)
        self.assertEqual(frozen, snapshot(self.store))

    def test_lineage_materializes_each_exact_copy_history_once(self):
        self.download()
        with storage.pricing_locks(self.store, ["GOOG"]):
            source = pipeline._current_sources(self.store, ["GOOG"])["GOOG"]
            inputs = pipeline.load_pricing(self.store, "GOOG", source=source)
        rows = [download.rows for download in inputs.downloads.values()]
        self.assertTrue(all(item is rows[0] for item in rows))

    def test_first_consumption_trusts_indexed_pricing_before_loading_any_receipt(self):
        self.download()
        self.run_build(["GOOG"])
        check_summary.rebuild(self.store)
        pricing = self.store / "end_of_day/pricing/GOOG"
        lineage = self.root / "GOOG"
        stat = Path.stat

        def no_published_stat(path, *args, **kwargs):
            if path.is_relative_to(pricing) or path.is_relative_to(lineage):
                self.assertEqual(path.name, ".pending.json", f"re-proved a published path: {path}")
            return stat(path, *args, **kwargs)

        with patch.object(Path, "stat", no_published_stat):
            result = self.run_build()  # Exercises incomplete hint coverage and indexed discovery too.
        self.assertEqual(result["results"], [])

    def test_missing_indexed_source_is_an_input_failure_not_permission_to_redownload(self):
        self.download()
        self.run_build(["GOOG"])
        (self.store / "end_of_day/pricing/GOOG/source.manifest.json").unlink()
        with patch.object(pipeline, "download_pricing", side_effect=AssertionError("unrequested repair")):
            result = pipeline.run_lineage(
                self.store, ["GOOG"], identity_client=fixtures.IdentityClient(), pricing_client=object()
            )
        self.assertEqual(result["failed"], 1)
        self.assertIn("published pricing receipt is missing", result["results"][0]["error"])

    def test_publication_trusts_locked_selection_through_acknowledgement(self):
        self.download()
        self.run_build(["GOOG"])
        check_summary.rebuild(self.store)
        self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
        finish = storage.finish_publication
        stat = Path.stat
        pricing = self.store / "end_of_day/pricing"
        notices = self.store / "end_of_day/manifests"

        def no_input_recheck(path, *args, **kwargs):
            self.assertFalse(path.is_relative_to(pricing) or path.is_relative_to(notices), path)
            return stat(path, *args, **kwargs)

        def selected(*args, **kwargs):
            self.assertIsNotNone(kwargs["sources"])
            with (
                patch.object(Path, "stat", no_input_recheck),
                patch.object(storage, "load_source", side_effect=AssertionError("source reselected")),
                patch.object(storage, "read_update", side_effect=AssertionError("notice reselected")),
                patch.object(storage, "read_checks", side_effect=AssertionError("hints reselected")),
            ):
                return finish(*args, **kwargs)

        with (
            patch.object(storage, "finish_publication", side_effect=selected),
            patch.object(summary, "invalidate", wraps=summary.invalidate) as invalidate,
        ):
            self.run_build(["GOOG"])
        self.assertEqual(invalidate.call_count, 1)
        self.assertFalse(self.notice().exists())
        self.assertEqual(summary.read_snapshot(self.store)["entries"][0]["last_all"][0][:10], "2026-10-14")

    def test_pending_pricing_is_rejected_before_trusting_its_handoff(self):
        self.download()
        self.run_build(["GOOG"])
        pending = self.store / "end_of_day/pricing/GOOG/.pending.json"
        pending.write_text("{}")
        with patch.object(pipeline, "load_pricing", side_effect=AssertionError("incomplete source consumed")):
            result = pipeline.run_lineage(self.store, ["GOOG"], identity_client=fixtures.IdentityClient())
        self.assertEqual(result["failed"], 1)
        self.assertIn("pricing publication incomplete", result["results"][0]["error"])

    def test_pricing_published_after_prefetch_is_reselected_under_lock(self):
        self.download()
        provider = Provider(["GOOG"])
        self.run_build(["GOOG"], client=provider.client)
        check_summary.rebuild(self.store)
        prefetch = pipeline._prefetch_metadata

        def changed(*args, **kwargs):
            prefetch(*args, **kwargs)
            self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])

        provider.calls.clear()
        with patch.object(pipeline, "_prefetch_metadata", side_effect=changed):
            result = self.run_build(["GOOG"], client=provider.client)
        self.assertEqual(result["results"][0]["status"], "processed")
        self.assertEqual(self.receipt()["source_rows"], 8 * (len(self.bars) + 1))
        entry = summary.read_snapshot(self.store)["entries"][0]
        self.assertEqual(entry["asof"], "2026-10-15")
        self.assertEqual(entry["last_all"][0][:10], "2026-10-14")
        self.assertFalse(self.notice().exists())
        self.assertEqual([query["end"] for kind, query in provider.calls if kind == "actions"], ["2026-10-15"])

    def test_pending_group_refresh_is_batched_at_its_required_full_range(self):
        symbols = ["A", "B", "C"]
        provider = Provider(symbols)
        for symbol in symbols:
            self.download(symbol)
        self.run_build(symbols, client=provider.client)
        for symbol in symbols:
            result = self.download(symbol, end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
            self.assertEqual(result["mode"], "update")
        universe_state.initialize(self.store)
        policy = WorkPolicy(self.store, storage.read_index(self.root), {}, [], "2026-10-15")
        for symbol in symbols[:2]:
            policy.require(symbol, "2026-10-15", reason="explicit_lineage_refresh", refresh=True)
            policy.require_dependencies({symbol}, "2026-10-15", {symbol})
        provider.actions.append(event("A", "newly-reported-history", process_date="2016-02-01"))
        provider.calls.clear()
        result = self.run_build(symbols, client=provider.client, work_policy=policy)
        queries = [query for kind, query in provider.calls if kind == "actions"]
        self.assertEqual(len(queries), 2, queries)
        self.assertEqual(
            {(query["symbols"], query["start"], query["end"]) for query in queries},
            {("A,B", "2016-01-01", "2026-10-15"), ("C", "2026-10-01", "2026-10-15")},
        )
        self.assertEqual(result["failed"], 0)
        for symbol in symbols[:2]:
            receipt = self.receipt(symbol)
            self.assertEqual(receipt["processed_through"], "2026-10-15T00:00:00Z")
            self.assertEqual(receipt["update_obligation_id"], policy.work[symbol]["obligation_id"])
        self.assertEqual(self.receipt("A")["evidence"]["corporate_actions"][0]["id"], "newly-reported-history")
        provider.calls.clear()
        pricing = snapshot(self.store / "end_of_day/pricing")
        self.run_build(symbols, client=provider.client, work_policy=policy)
        self.assertEqual(provider.calls, [])
        self.assertEqual(snapshot(self.store / "end_of_day/pricing"), pricing)

    def test_pending_refresh_prefetches_even_when_pricing_generation_is_unchanged(self):
        provider = Provider(["A", "B"])
        for symbol in ("A", "B"):
            self.download(symbol)
        self.run_build(["A", "B"], client=provider.client)
        universe_state.initialize(self.store)
        policy = WorkPolicy(self.store, storage.read_index(self.root), {}, [], "2026-10-14")
        for symbol in ("A", "B"):
            policy.require(symbol, "2026-10-14", reason="explicit_lineage_refresh", refresh=True)
        provider.calls.clear()
        pricing = snapshot(self.store / "end_of_day/pricing")
        self.run_build(["A", "B"], client=provider.client, work_policy=policy)
        queries = [query for kind, query in provider.calls if kind == "actions"]
        self.assertEqual(len(queries), 1, queries)
        self.assertEqual(queries[0]["symbols"], "A,B")
        self.assertEqual(queries[0]["start"], "2016-01-01")
        self.assertEqual(snapshot(self.store / "end_of_day/pricing"), pricing)

    def test_execution_version_twenty_keeps_its_publication_and_reader_handoff(self):
        self.download()
        provider = Provider(["GOOG"])
        self.run_build(["GOOG"], client=provider.client)
        receipt = self.receipt()
        receipt["schema_version"] = 20
        receipt["versions"]["lineage"] = 20
        (self.root / "GOOG/lineage.manifest.json").write_bytes(json_bytes(receipt))
        entry = storage.read_index(self.root)["symbols"]["GOOG"]
        storage._update_index(
            self.root,
            {
                "GOOG": {
                    **entry,
                    "sha256": storage.document_digest(receipt),
                    "versions": receipt["versions"],
                }
            },
        )
        with closing(connect_sqlite(self.root / "index.sqlite3")) as connection:
            connection.execute("PRAGMA user_version=20")
        with storage.reader_snapshot(
            self.store,
            lambda root: storage.read_index(root)["symbols"],
            include_boundaries=False,
            include_receipts=False,
        ):
            pass
        before = snapshot(self.store / "end_of_day/pricing")
        provider.calls.clear()
        result = self.run_build(["GOOG"], client=provider.client)
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(self.receipt(), receipt)
        self.assertEqual(provider.calls, [])
        self.assertEqual(snapshot(self.store / "end_of_day/pricing"), before)

    def test_failed_publication_neither_updates_cache_nor_reports_completed_progress(self):
        self.download()
        self.run_build(["GOOG"])
        client = fixtures.IdentityClient({"GOOG": {**fixtures.asset("GOOG"), "status": "inactive"}})
        events, caches = [], []
        create_cache = storage.ReceiptCache

        def capture_cache(*args, **kwargs):
            cache = create_cache(*args, **kwargs)
            caches.append(cache)
            return cache

        original = self.receipt()
        with (
            patch.object(storage, "ReceiptCache", side_effect=capture_cache),
            patch.object(storage, "_install", side_effect=OSError("interrupted install")),
        ):
            result = pipeline.run_lineage(
                self.store, ["GOOG"], identity_client=client, refresh=True, on_processed=events.append
            )
        self.assertEqual(result["failed"], 1)
        self.assertEqual(events, [])
        self.assertEqual(caches[0].read("GOOG"), original)
        result = self.run_build(["GOOG"], client=client, on_processed=events.append)
        self.assertTrue(result["recovered"])
        self.assertEqual(events, [["GOOG"]])
        self.assertFalse(check_summary.stored_active(self.receipt()))

    def test_rule_migration_skips_metadata_prefetch_and_incompatible_old_boundaries(self):
        self.download()
        self.run_build()
        index = storage.read_index(self.root)
        receipt = self.receipt()
        receipt["schema_version"] = 11
        receipt["versions"] = {**receipt["versions"], "lineage": 11, "correlation": 54}
        (self.root / "GOOG/lineage.manifest.json").write_bytes(json_bytes(receipt))
        entry = index["symbols"]["GOOG"]
        storage._update_index(
            self.root,
            {
                "GOOG": {
                    **entry,
                    "sha256": storage.document_digest(receipt),
                    "versions": receipt["versions"],
                }
            },
        )
        with (
            patch.object(pipeline, "_prefetch_metadata", side_effect=AssertionError("metadata prefetch")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("old boundaries")),
        ):
            result = self.run_build()
        self.assertEqual(result["results"][0]["status"], "processed")
        self.assertEqual(self.receipt()["versions"], versions())

    def test_metadata_migration_uses_bounded_prefetch(self):
        self.download()
        self.run_build()
        index = storage.read_index(self.root)
        receipt = self.receipt()
        receipt["versions"] = {
            **receipt["versions"],
            "identity_report": receipt["versions"]["identity_report"] - 1,
        }
        (self.root / "GOOG/lineage.manifest.json").write_bytes(json_bytes(receipt))
        entry = index["symbols"]["GOOG"]
        storage._update_index(
            self.root,
            {
                "GOOG": {
                    **entry,
                    "sha256": storage.document_digest(receipt),
                    "versions": receipt["versions"],
                }
            },
        )
        planned = []
        prefetch = pipeline._prefetch_metadata

        def capture(_store, _root, labels, *args, **kwargs):
            planned.extend(labels)
            return prefetch(_store, _root, labels, *args, **kwargs)

        with patch.object(pipeline, "_prefetch_metadata", side_effect=capture):
            result = self.run_build()
        self.assertEqual(result["results"][0]["status"], "processed")
        self.assertEqual(planned, ["GOOG"])

    def test_large_explicit_selection_uses_dependency_bounded_prefetch_caches(self):
        self.download()
        selected = [f"T{number:04}" for number in range(2500)]
        current = versions()
        index = {
            "symbols": {
                symbol: {
                    "sha256": "0" * 64,
                    "source_sha256": "1" * 64,
                    "dependencies": [],
                    "asset_ids": [],
                    "group_members": [symbol],
                    "versions": current,
                }
                for symbol in selected
            }
        }
        caches, planned = [], []

        def prefetch(
            _store,
            _root,
            labels,
            _index,
            _graph,
            _client,
            _refresh,
            receipt_cache,
            end_date=None,
            work_policy=None,
            pricing_handoff=None,
        ):
            caches.append(receipt_cache)
            planned.append(list(labels))
            receipt_cache.receipts.update((label, ("generation", {})) for label in labels)

        def processed(_store, _root, symbol, **_kwargs):
            return {"symbol": symbol, "status": "unchanged", "processed": [symbol]}

        with (
            patch.object(storage, "read_index", return_value=index),
            patch.object(pipeline, "_prefetch_metadata", side_effect=prefetch),
            patch.object(pipeline, "process_group", side_effect=processed),
        ):
            result = pipeline.run_lineage(self.store, selected, identity_client=fixtures.IdentityClient())
        self.assertEqual(result["failed"], 0)
        self.assertEqual([len(labels) for labels in planned], [1000, 1000, 500])
        self.assertEqual([len(cache.receipts) for cache in caches], [1000, 1000, 500])
