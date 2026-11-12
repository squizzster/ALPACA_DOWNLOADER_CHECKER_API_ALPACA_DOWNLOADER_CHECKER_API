"""The first/tail producer boundary and bounded legacy assembly."""

import csv
import gzip
import io
import json
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as lineage_fixture
from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
from test_alpaca_pricing_download import FakeClient, bar, single_session_request

from libs.alpaca_pricing_download import check_summary, download, storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, COLUMNS, DownloadError, timestamp, transform
from libs.alpaca_pricing_download.endpoints import (
    EndpointSelection,
    compare_history,
    comparison_rows,
    endpoints_of,
    validate_tail,
)
from libs.alpaca_pricing_download.history_tail import readall_rsplit, upgrade_legacy_entries


def rows(count, start="2026-10-20"):
    day = date.fromisoformat(start)
    return tuple(
        ((day + timedelta(days=i)).isoformat() + "T05:00:00Z", "10", "11", "9", "10", "100", "5", "10")
        for i in range(count)
    )


def packed(values, *, newline="\n", trailing=True):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator=newline)
    writer.writerow(COLUMNS)
    writer.writerows(values)
    data = stream.getvalue()
    return gzip.compress((data if trailing else data.removesuffix(newline)).encode())


class HistoryTailTests(unittest.TestCase):
    def test_legacy_raw_seam_cannot_use_another_views_predecessor(self):
        from libs.alpaca_serving_catalogue.reuse import _legacy_previous_raw

        def view(count, last):
            return {
                "rows": count,
                "last_session": last,
                "ownership": [{"rows": count, "owner": ["pid", ["A"]]}],
            }

        # A.raw has Oct 13 while A.all does not. B.all supplies that date, so
        # accepted group-wide session compatibility cannot identify A.raw's seam.
        completed = {
            "inputs": {
                "A": {"views": {"raw": view(4, "2026-10-14"), "all": view(3, "2026-10-14")}},
                "B": {"views": {"raw": view(1, "2026-10-01"), "all": view(2, "2026-10-13")}},
            }
        }
        record = {"public_id": "pid", "last_session": "2026-10-14", "price_reasons": []}
        revisions = {"A": {"previous_session": "2026-10-12"}}
        self.assertIsNone(_legacy_previous_raw(completed, record, {"A", "B"}, revisions))

    def test_exact_three_way_decision_and_one_session_history(self):
        history = rows(20)
        tail = history[-10:]
        baseline = comparison_rows(history[0], tail)
        self.assertEqual(compare_history(history[0], tail, baseline).decision, "unchanged")
        for index, decision in ((0, "full_history"), (-2, "full_history"), (-1, "latest_session")):
            with self.subTest(index=index):
                live = list(baseline)
                live[index] = (*live[index][:-1], "10.00000000000000000000000001")
                result = compare_history(history[0], tail, live)
                self.assertEqual(result.decision, decision)
                self.assertEqual(result.live_all, tuple(live))
        changed = (*history[0][:-1], "12")
        self.assertEqual(compare_history(history[0], history[:1], [changed]).decision, "full_history")

    def test_missing_duplicate_and_new_retained_session(self):
        history = rows(5)
        for actual in (history[:-1], (*history, history[-1])):
            with self.assertRaises(DownloadError):
                compare_history(history[0], history, actual)
        sparse = (history[0], history[-1])
        self.assertEqual(compare_history(sparse[0], sparse, history).decision, "full_history")

    def test_short_history_overlap_and_numeric_equivalence(self):
        history = rows(3)
        actual = tuple(
            (row[0].replace("05:00:00Z", "00:00:00-05:00"), *(v + ".0" for v in row[1:])) for row in history
        )
        self.assertEqual(comparison_rows(history[0], history), history)
        self.assertTrue(compare_history(history[0], history, actual).matched)
        endpoints = endpoints_of({name: history for name in ADJUSTMENTS})
        selected = EndpointSelection(
            "store", "GOOG", "generation", "end", "asof", tuple(endpoints.items()), tail_all=endpoints.tail_all
        )
        self.assertEqual(selected.consume("store", "GOOG", "generation", "end", "asof").tail_all, history)
        with self.assertRaises(DownloadError):
            selected.consume("store", "GOOG", "new-generation", "end", "asof")

    def test_tail_must_be_ten_or_the_complete_short_history(self):
        history = rows(20)
        self.assertEqual(validate_tail(history[-10:], history[0], history[-1]), history[-10:])
        for tail in (None, (), history[-9:], history[-11:], tuple(reversed(history[-10:]))):
            with self.subTest(tail=tail), self.assertRaises(DownloadError):
                validate_tail(tail, history[0], history[-1])
        self.assertEqual(validate_tail((), None, None), ())

    def test_readall_rsplit_excludes_header_and_preserves_data_rows(self):
        for count in (0, 1, 3, 10, 25):
            history = rows(count)
            for newline in ("\n", "\r\n"):
                for trailing in (False, True):
                    with self.subTest(count=count, newline=newline, trailing=trailing):
                        self.assertEqual(
                            readall_rsplit(packed(history, newline=newline, trailing=trailing), 10),
                            history[-10:],
                        )

    def test_year_overlap_is_discarded_before_taking_older_tail(self):
        history = rows(50)
        older = tuple(row for row in history if row[0][:7] < "2026-11")
        with patch("libs.alpaca_pricing_download.history_tail.gzip.GzipFile", wraps=gzip.GzipFile) as opened:
            self.assertEqual(readall_rsplit(packed(history), 8, before_month="2026-11"), older[-8:])
        self.assertEqual(opened.call_count, 1)


class HistoryTailPublicationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(download, "utc_now", return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc))
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name)
        self.root = self.store / "end_of_day/pricing/GOOG"
        self.bars = [bar("2016-01-04"), bar("2025-12-31")]
        self.bars += [bar((date(2026, 10, 1) + timedelta(days=i)).isoformat()) for i in range(20)]
        download.download_pricing(self.store, "GOOG", "2026-10-21", client=FakeClient(self.bars))

    def test_selected_generation_is_calculated_once_across_checks_and_publication(self):
        import hashlib

        from libs.alpaca_pricing_download.contract import digest, json_bytes

        before = storage.load_source(self.root, "GOOG")
        encoded = json_bytes(before)
        generation = digest(encoded)
        original = hashlib.sha256
        hashed = []
        source_hashes_before_staging = []
        staging_started = False
        write_file = storage.write_file

        def record(data=b"", *args, **kwargs):
            if data == encoded:
                hashed.append(data)
            if not staging_started and b"alpaca_standalone_pricing_source" in data:
                source_hashes_before_staging.append(data)
            return original(data, *args, **kwargs)

        def begin_staging(*args, **kwargs):
            nonlocal staging_started
            staging_started = True
            return write_file(*args, **kwargs)

        revised = [*self.bars[:-1], {**self.bars[-1], "v": 12345}]
        with (
            patch.object(hashlib, "sha256", side_effect=record),
            patch.object(storage, "write_file", side_effect=begin_staging),
        ):
            result = download.download_pricing(
                self.store,
                "GOOG",
                "2026-10-21",
                client=FakeClient(revised),
                refresh_all=True,
                selected_source_sha256=generation,
                expected_source_sha256=generation,
            )
        self.assertTrue(result["changed"])
        self.assertEqual(len(hashed), 1, "selected generation calculated again by a downstream consumer")
        self.assertEqual(len(source_hashes_before_staging), 2, "journal recalculated the source generation")
        self.assertEqual(len(set(source_hashes_before_staging)), 2)

    def test_identical_publication_returns_its_accepted_generation_without_hashing_again(self):
        import hashlib

        from libs.alpaca_pricing_download.contract import json_bytes

        before = storage.load_source(self.root, "GOOG")
        endpoints = storage.read_validator(self.root, before)
        reordered = dict(reversed(tuple(before.items())))
        handoff = {}
        with storage.ticker_lock(self.root, exclusive=True):
            storage.publish(self.root, before, reordered, {}, {}, endpoints=endpoints, handoff=handoff)
        before = reordered
        encoded = json_bytes(before)
        generation = handoff["source_sha256"]
        original = hashlib.sha256

        def reject_repeated_generation(data=b"", *args, **kwargs):
            self.assertNotEqual(data, encoded, "completed source generation was recalculated")
            return original(data, *args, **kwargs)

        with (
            storage.ticker_lock(self.root, exclusive=True),
            patch.object(hashlib, "sha256", side_effect=reject_repeated_generation),
        ):
            result = download.publish_acquired(
                self.root,
                before,
                "GOOG",
                before["anchor_month"],
                before["latest_request"],
                timestamp(before["history_start"]),
                endpoints,
                {},
                None,
                previous_source_sha256=generation,
            )
        self.assertEqual(result["source_sha256"], generation)
        self.assertIs(result["source"], before)
        self.assertEqual(json_bytes(result["source"]), encoded)
        self.assertEqual(result["files_written"], 0)

    def test_latest_correction_and_append_publish_one_hot_partition(self):
        revised = [*self.bars[:-1], {**self.bars[-1], "v": 12345}, bar("2026-10-21")]
        client = FakeClient(revised)
        before = storage.load_source(self.root, "GOOG")
        read = download.read_boundary
        calls = []

        def selected(root, source, kind, **kwargs):
            calls.append(kind)
            self.assertEqual(kind, "current_month", "unaffected payload read")
            return read(root, source, kind, **kwargs)

        with patch.object(download, "read_boundary", side_effect=selected):
            result = download.download_pricing(self.store, "GOOG", "2026-10-22", client=client)
        self.assertEqual(result["mode"], "latest_session")
        self.assertEqual(calls, ["current_month"])
        self.assertEqual(result["updated_boundaries"], ["current_month"])
        historical = [call for call in client.calls if call[2].date() < date(2026, 10, 20)]
        self.assertEqual(len(historical), 2)
        self.assertEqual({call[1] for call in historical}, {"all"})
        corrected_all = [
            call for call in client.calls if call[1] == "all" and call[2].date() == date(2026, 10, 20)
        ]
        self.assertEqual(corrected_all, [], "completed comparison observation fetched again")
        after = storage.load_source(self.root, "GOOG")
        for kind in ("completed_years", "current_year"):
            self.assertEqual(after["boundaries"][kind], before["boundaries"][kind])
        endpoints = storage.read_validator(self.root, after)
        for values in download.read_pricing_views(self.store, "GOOG").values():
            self.assertEqual(values[-2][5], "12345")
            self.assertEqual(values[-1][0][:10], "2026-10-21")
        self.assertEqual(len(endpoints.tail_all), 10)
        self.assertEqual(endpoints.tail_all[-2][5], "12345")
        self.assertEqual(endpoints.tail_all[-1][0][:10], "2026-10-21")

    def test_one_earlier_tail_change_selects_full_replacement(self):
        revised = list(self.bars)
        revised[-2] = {**revised[-2], "v": 12345}
        result = download.download_pricing(self.store, "GOOG", "2026-10-22", client=FakeClient(revised))
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(download.read_pricing_views(self.store, "GOOG")["all"][-2][5], "12345")

    def test_correction_and_append_preserve_eight_views_across_sparse_partitions_and_rollover(self):
        cases = (
            ("2026-10-30", "2026-10-31", "2026-11-02", "2026-11-03"),
            ("2026-12-31", "2027-01-01", "2027-01-04", "2027-01-05"),
            ("2026-09-30", "2026-10-21", "2026-10-21", "2026-10-22"),
            ("2025-12-31", "2026-10-21", "2026-10-21", "2026-10-22"),
        )

        class DistinctViews(FakeClient):
            def fetch(self, symbol, adjustment, start, end, **kwargs):
                selected = super().fetch(symbol, adjustment, start, end, **kwargs)
                scale = ADJUSTMENTS.index(adjustment) + 1
                return [
                    {**row, **{key: row[key] * scale for key in ("o", "h", "l", "c", "vw")}} for row in selected
                ]

        for index, (latest, covered, added, target) in enumerate(cases):
            with self.subTest(latest=latest, target=target):
                store = self.store / f"rollover-{index}"
                bars = [bar("2016-01-04"), bar("2025-12-30"), bar(latest)]
                download.download_pricing(store, "GOOG", covered, client=DistinctViews(bars))
                revised = [*bars[:-1], {**bars[-1], "v": 12345}, bar(added)]
                client = DistinctViews(revised)
                read = download.read_boundary
                reads = []

                def once(root, source, kind, **kwargs):
                    self.assertNotIn(kind, reads, "partition decoded twice during correction and append")
                    reads.append(kind)
                    return read(root, source, kind, **kwargs)

                with patch.object(download, "read_boundary", side_effect=once):
                    result = download.download_pricing(store, "GOOG", target, client=client)
                self.assertEqual(result["mode"], "latest_session")
                self.assertTrue(all(call[2].date().isoformat() >= latest for call in client.calls[2:]))
                actual = download.read_pricing_views(store, "GOOG")
                expected_client = DistinctViews(revised)
                start, end = timestamp("2016-01-01T00:00:00Z"), timestamp(target + "T00:00:00Z")
                for adjustment in ADJUSTMENTS:
                    self.assertEqual(
                        actual[adjustment],
                        transform(expected_client.fetch("GOOG", adjustment, start, end), start, end),
                    )
                root = store / "end_of_day/pricing/GOOG"
                endpoints = storage.read_validator(root, storage.load_source(root, "GOOG"))
                self.assertEqual(endpoints.tail_all, actual["all"][-10:])

    def test_missing_new_tail_fails_without_reconstructing_history(self):
        from libs.alpaca_pricing_download.contract import VALIDATOR_FILE, json_bytes

        path = self.root / VALIDATOR_FILE
        document = storage.read_document(path)
        del document["tail_all"]
        path.write_bytes(json_bytes(document))
        with patch.object(
            download, "publish_legacy_tail", side_effect=AssertionError("new source reconstructed")
        ):
            with self.assertRaises(DownloadError):
                download.download_pricing(self.store, "GOOG", "2026-10-22", client=FakeClient(self.bars))

    def legacy_entry(self):
        from libs.alpaca_pricing_download.contract import VALIDATOR_FILE, digest, json_bytes

        source = storage.load_source(self.root, "GOOG")
        validator_path = self.root / VALIDATOR_FILE
        validator = storage.read_document(validator_path)
        validator["schema_version"] = 31
        del validator["tail_all"]
        validator_path.write_bytes(json_bytes(validator))
        source["schema_version"] = 31
        source["validator"]["sha256"] = digest(json_bytes(validator))
        (self.root / "source.manifest.json").write_bytes(json_bytes(source))
        entry = check_summary.price_entry(self.root, source)
        token = check_summary.begin_rebuild(self.store)
        check_summary.finish_rebuild(self.store, token, [entry])
        return source, entry

    def test_legacy_upgrade_reads_one_alias_and_preserves_all_payloads(self):
        from libs.alpaca_pricing_download import history_tail

        source, entry = self.legacy_entry()
        read = history_tail.read_artifact
        paths = []

        def selected(path, **kwargs):
            paths.append(str(path))
            return read(path, **kwargs)

        with patch.object(history_tail, "read_artifact", side_effect=selected):
            changed = upgrade_legacy_entries(self.store, [entry])["GOOG"]
        self.assertFalse(changed["changed"])
        self.assertEqual(changed["source"]["boundaries"], source["boundaries"])
        self.assertEqual(len(paths), 1)
        self.assertTrue(paths[0].endswith("2026/Oct/raw.csv.gz"), paths)
        fresh = check_summary.read_snapshot(self.store)["entries"]
        self.assertEqual(len(fresh[0]["tail_all"]), 10)
        with patch.object(
            history_tail, "read_artifact", side_effect=AssertionError("legacy reconstruction repeated")
        ):
            self.assertEqual(upgrade_legacy_entries(self.store, fresh), {})

    def test_format_three_summary_selects_and_publishes_its_legacy_upgrade(self):
        from libs.alpaca_pricing_download import history_tail

        _, _ = self.legacy_entry()
        with sqlite3.connect(check_summary.summary_path(self.store)) as db:
            db.execute("ALTER TABLE entries RENAME TO current_entries")
            db.execute(
                "CREATE TABLE entries (symbol TEXT PRIMARY KEY, asof TEXT NOT NULL, "
                "end_exclusive TEXT NOT NULL, last_all TEXT NOT NULL, first_all TEXT NOT NULL)"
            )
            db.execute(
                "INSERT INTO entries SELECT symbol, asof, end_exclusive, last_all, first_all FROM current_entries"
            )
            db.execute("DROP TABLE current_entries")
            db.execute("PRAGMA user_version=3")
        selected = check_summary.read_snapshot(self.store)
        self.assertIsNone(selected["entries"][0]["tail_all"])
        self.assertIsNone(selected["entries"][0]["source_sha256"])
        self.assertEqual(check_summary.describe(selected)["legacy_tail_upgrades"], 1)
        read = history_tail.read_artifact
        with patch.object(history_tail, "read_artifact", wraps=read) as opened:
            publication = upgrade_legacy_entries(self.store, selected["entries"])["GOOG"]
        self.assertEqual(opened.call_count, 1)
        current = check_summary.read_snapshot(self.store)
        self.assertEqual(current["entries"][0]["source_sha256"], publication["source_sha256"])
        self.assertEqual(len(current["entries"][0]["tail_all"]), 10)
        self.assertEqual(check_summary.describe(current)["legacy_tail_upgrades"], 0)

    def test_ordinary_legacy_upgrade_survives_provider_failure_without_reextraction(self):
        from libs.alpaca_pricing_download import acquire, history_tail
        from libs.alpaca_pricing_download.contract import timestamp
        from libs.alpaca_pricing_download.http import BarsClient

        for mode, end in (("ordinary", "2026-10-22"), ("retained", "2026-10-23")):
            with self.subTest(mode=mode):
                source, _ = self.legacy_entry()
                client = FakeClient(self.bars) if mode == "ordinary" else BarsClient("test", "test")

                def run():
                    if mode == "ordinary":
                        return download.download_pricing(self.store, "GOOG", end, client=client)
                    return acquire.acquire_selected(self.store, ["GOOG"], end, client=client)

                method = "fetch" if mode == "ordinary" else "_get"
                with patch.object(client, method, side_effect=DownloadError("provider unavailable")):
                    if mode == "ordinary":
                        with self.assertRaisesRegex(DownloadError, "provider unavailable"):
                            run()
                    else:
                        self.assertEqual(run()["failed"], 1)
                published = storage.load_source(self.root, "GOOG")
                self.assertEqual(published["boundaries"], source["boundaries"])
                self.assertEqual(published["latest_request"], source["latest_request"])
                self.assertEqual(len(storage.read_validator(self.root, published).tail_all), 10)

                def page(params):
                    start, end = timestamp(params["start"]), timestamp(params["end"])
                    return {"bars": {"GOOG": [row for row in self.bars if start <= timestamp(row["t"]) <= end]}}

                with patch.object(
                    history_tail, "read_artifact", side_effect=AssertionError("tail reconstructed after restart")
                ):
                    if mode == "ordinary":
                        self.assertEqual(run()["consistency_check"], "matched")
                    else:
                        with patch.object(client, "_get", side_effect=page):
                            self.assertEqual(run()["failed"], 0)


class HistoryTailPipelineTests(unittest.TestCase):
    def test_same_cutoff_correction_journal_recovers_without_replaying_producers(self):
        from libs.alpaca_lineage_build import pipeline, pricing
        from libs.alpaca_lineage_build import storage as lineage_storage
        from libs.alpaca_pricing_download.refresh import check_retained_source
        from libs.alpaca_serving_catalogue.producer import maintain

        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        old = lineage_fixture.informative_bars("2025-12-01", "2026-10-14")
        case.download(bars=old)
        case.run_build()
        self.assertEqual(case.receipt()["unresolved_rows"], 0)
        self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
        client = FakeClient([*old[:-1], {**old[-1], "v": 12345}])
        selection = check_retained_source(case.store, "GOOG", "2026-10-14", client=client)
        publication = download.download_pricing(
            case.store, "GOOG", "2026-10-14", client=client, endpoint_selection=selection["endpoint_selection"]
        )
        with (
            patch.object(lineage_storage, "_install", side_effect=ProcessInterrupted()),
            self.assertRaises(ProcessInterrupted),
        ):
            case.run_build(pricing_handoff={"GOOG": publication})
        self.assertTrue((case.root / ".pending.json").exists())
        identity = lineage_fixture.IdentityClient()
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed correction recalculated")),
            patch.object(
                pricing, "load_published_boundary", side_effect=AssertionError("accepted prices reread")
            ),
            patch.object(
                download, "download_pricing", side_effect=AssertionError("completed pricing reacquired")
            ),
        ):
            recovered = case.run_build(client=identity)
        self.assertTrue(recovered["recovered"])
        self.assertEqual(identity.request_count, 0)
        self.assertFalse((case.root / ".pending.json").exists())
        self.assertEqual(case.receipt()["source_sha256"], publication["source_sha256"])
        self.assertTrue(maintain(case.store)["complete"])

    def test_earlier_change_after_latest_correction_invalidates_the_prefix(self):
        from libs.alpaca_pricing_download.refresh import check_retained_source

        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        old = lineage_fixture.informative_bars("2025-12-01", "2026-10-14")
        case.download(bars=old)
        case.run_build()
        revised = [*old[:-1], {**old[-1], "v": 12345}]
        client = FakeClient(revised)
        selection = check_retained_source(case.store, "GOOG", "2026-10-14", client=client)
        download.download_pricing(
            case.store, "GOOG", "2026-10-14", client=client, endpoint_selection=selection["endpoint_selection"]
        )
        later = case.download(bars=[{**revised[0], "v": 23456}, *revised[1:]], refresh_all=True)
        result = case.run_build(pricing_handoff={"GOOG": later})
        self.assertFalse(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual(result["results"][0]["pricing_rows_processed"], 8 * len(old))
        self.assertEqual(case.receipt()["source_sha256"], later["source_sha256"])

    def test_same_cutoff_correction_processes_only_the_revised_session(self):
        from libs.alpaca_lineage_build import pipeline, pricing, resume
        from libs.alpaca_pricing_download.refresh import check_retained_source
        from libs.alpaca_serving_catalogue.producer import maintain

        for members in (("GOOG",), ("GOOG", "OTHER")):
            with self.subTest(members=members):
                case = lineage_fixture.LineageBuildTests()
                case.setUp()
                self.addCleanup(case.doCleanups)
                identity = lineage_fixture.IdentityClient(
                    {name: lineage_fixture.asset(name, "shared-id") for name in members}
                )
                changed = members[-1]
                old = {}
                for name in members:
                    old[name] = lineage_fixture.informative_bars(
                        "2025-12-01" if name == "GOOG" else "2026-10-01",
                        "2026-10-01" if len(members) > 1 and name == "GOOG" else "2026-10-14",
                    )
                    case.download(symbol=name, bars=old[name])
                case.run_build(symbols=members, client=identity)
                before = case.receipt(changed)
                self.assertEqual(before["unresolved_rows"], 0)
                self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
                client = FakeClient([*old[changed][:-1], {**old[changed][-1], "v": 12345}])
                selection = check_retained_source(case.store, changed, "2026-10-14", client=client)
                publication = download.download_pricing(
                    case.store,
                    changed,
                    "2026-10-14",
                    client=client,
                    endpoint_selection=selection["endpoint_selection"],
                )
                self.assertEqual(publication["mode"], "latest_session")
                correlate, read = pipeline.correlate, Path.read_bytes
                processed, decoded = [], set()
                load = pricing.load_published_boundary

                def only_correction(store, inputs, *args, **kwargs):
                    processed.extend(row[0][:10] for view in inputs.values() for row in view.views["all"])
                    return correlate(store, inputs, *args, **kwargs)

                def once(root, source, kind, **kwargs):
                    key = source["symbol"], kind
                    self.assertNotIn(key, decoded, "accepted pricing partition decoded again")
                    decoded.add(key)
                    return load(root, source, kind, **kwargs)

                def no_cold_prices(path):
                    if path.suffix == ".gz":
                        self.assertEqual(path.parent.name, "Oct", path)
                        self.assertIn(changed, path.parts, path)
                    return read(path)

                with (
                    patch.object(
                        resume,
                        "expected_market_session_coverage",
                        side_effect=AssertionError("empty append calendar calculation"),
                    ),
                    patch.object(pricing, "load_published_boundary", side_effect=once),
                    patch.object(pipeline, "correlate", side_effect=only_correction),
                    patch.object(Path, "read_bytes", no_cold_prices),
                ):
                    result = case.run_build(
                        symbols=members, client=identity, pricing_handoff={changed: publication}
                    )
                    serving = maintain(case.store)
                self.assertTrue(result["results"][0]["reused_closed_boundaries"])
                self.assertEqual(processed, ["2026-10-13"])
                self.assertEqual(decoded, {(changed, "current_month")})
                self.assertEqual(case.receipt(changed)["source_rows"], before["source_rows"])
                self.assertTrue(serving["complete"], serving)
                self.assertEqual(serving["checked_bars"], 8)
                self.assertEqual(serving["full_history_evaluations"], 0)

    def test_fresh_unchanged_decision_acknowledges_superseded_correction_across_restart(self):
        from libs.alpaca_lineage_build import universe_state

        with fixture() as case:
            case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 12345}, bar("2026-10-19")]

            def interrupt_acquisition(params):
                if "AAA" in params["symbols"].split(",") and params["adjustment"] == "raw":
                    raise ProcessInterrupted()
                return case.page(params)

            with (
                patch.object(case.pricing, "_get", side_effect=interrupt_acquisition),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-20")
            download.download_pricing(case.store, "AAA", "2026-10-20", client=FakeClient(case.live["AAA"]))
            with (
                patch.object(download, "publish_acquired", side_effect=ProcessInterrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-20")
            obligation = universe_state.read_state(case.store)["work"]["AAA"]["pricing_correction"]
            self.assertEqual(obligation["scope"], "latest_session")
            self.assertEqual(obligation["selection"]["comparison"]["decision"], "unchanged")

            def no_replay(params):
                self.assertNotIn("AAA", params["symbols"].split(","), "accepted unchanged comparison repeated")
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=no_replay):
                recovered = case.update("2026-10-20")
            self.assertTrue(recovered["complete"], recovered)
            self.assertEqual(download.read_pricing_views(case.store, "AAA")["all"][-2][5], "12345")

    def test_multisource_correction_reuses_earlier_serving_and_matches_full_evaluation(self):
        self.check_multisource_correction()

    def test_legacy_disjoint_serving_checkpoint_uses_accepted_seam_without_historical_reads(self):
        self.check_multisource_correction(legacy=True)

    def test_legacy_negative_serving_checkpoint_requires_explicit_recovery_without_history_audit(self):
        self.check_multisource_correction(legacy=True, negative=True)

    def check_multisource_correction(self, *, legacy=False, negative=False):
        from alpaca_end_of_day import StandaloneReader
        from libs.alpaca_serving_catalogue import policy
        from libs.alpaca_serving_catalogue import storage as serving_storage
        from libs.alpaca_serving_catalogue.producer import maintain

        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        names = ("GOOG", "OTHER")
        identity = lineage_fixture.IdentityClient(
            {name: lineage_fixture.asset(name, "shared-id") for name in names}
        )
        make_bars = lineage_fixture.calendar_bars if negative else lineage_fixture.informative_bars
        old = {
            "GOOG": make_bars("2025-12-01", "2026-10-01"),
            "OTHER": make_bars("2026-10-01", "2026-10-14"),
        }
        for name in names:
            case.download(symbol=name, bars=old[name])
        case.run_build(symbols=names, client=identity)
        self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
        if legacy:
            selected = serving_storage.read_groups(
                case.store, ["GOOG"], generation=serving_storage.selector_state(case.store)["generation"]
            )
            completed = selected["GOOG"]["history_validation"]
            completed.pop("revision_prefix_version")
            for record in completed["prefix_records"]:
                record.pop("before_last_raw_bar")
                record.pop("before_last_price_reasons")
            serving_storage.commit(case.store, selected, list(selected.values()))
        publications = {
            name: case.download(
                symbol=name,
                end="2026-10-15",
                bars=old[name]
                if name == "GOOG"
                else [
                    *old[name][:-1],
                    {**old[name][-1], "v": 12345},
                    *make_bars("2026-10-14", "2026-10-15"),
                ],
            )
            for name in names
        }
        case.run_build(symbols=names, client=identity, pricing_handoff=publications)
        read = Path.read_bytes

        def no_earlier_prices(path):
            if path.suffix == ".gz" and ("GOOG" in path.parts or path.parent.name != "Oct"):
                self.fail(f"unchanged serving history reopened: {path}")
            return read(path)

        with patch.object(Path, "read_bytes", no_earlier_prices):
            result = maintain(case.store)
        if negative:
            self.assertFalse(result["complete"], result)
            self.assertEqual(result["checked_bars"], 0)
            self.assertEqual(result["full_history_evaluations"], 0)
            self.assertIn("explicit catalogue rebuild required", result["failures"][0]["error"])
            return
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assertEqual(result["checked_bars"], 16)
        actual = serving_storage.read_groups(
            case.store, ["GOOG"], generation=serving_storage.selector_state(case.store)["generation"]
        )["GOOG"]
        with StandaloneReader(case.store)._open_group("GOOG", include_unresolved=True) as group:
            expected = policy.evaluate(group)
        self.assertEqual(actual["securities"], expected["securities"])
        self.assertEqual(actual["decisions"], expected["decisions"])

    def test_sparse_correction_keeps_accepted_historical_metadata_closed(self):
        from test_alpaca_lineage_batching import Provider

        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        old = lineage_fixture.calendar_bars("2025-12-01", "2026-10-01")
        case.download(bars=old)
        case.run_build(client=Provider(["GOOG"]).client)
        publication = case.download(
            end="2026-10-15", bars=[*old[:-1], {**old[-1], "v": 12345}, bar("2026-10-14")]
        )
        provider = Provider(["GOOG"])
        result = case.run_build(client=provider.client, pricing_handoff={"GOOG": publication})
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual([query["start"] for kind, query in provider.calls if kind == "actions"], ["2026-10-01"])

    def test_full_replacement_obligation_does_not_repeat_accepted_comparisons(self):
        with fixture() as case:
            case.live["AAA"] = [bar("2016-01-04", "50"), case.old[-1]]

            def interrupt(params):
                if "AAA" in params["symbols"].split(",") and params["adjustment"] == "raw":
                    raise ProcessInterrupted()
                return case.page(params)

            with (
                patch.object(case.pricing, "_get", side_effect=interrupt),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-17")
            case.pages.clear()
            result = case.update("2026-10-17")
            self.assertTrue(result["complete"], result)
            estimate = result["plan"]["expected_io"]
            self.assertEqual(estimate["history_check_actions"], 0)
            self.assertEqual(estimate["full_replacements_selected"], 1)
            self.assertEqual(estimate["correction_actions_estimate"], 8)
            self.assertEqual(estimate["pricing_publications_estimate"], 3)  # correction and two new sources
            calls = [p for p in case.pages if "AAA" in p["symbols"].split(",")]
            self.assertEqual(len(calls), 3, calls)  # raw/all replacement; accepted all observation is retained.
            self.assertFalse(any(p["start"][:10] == "2026-10-16" for p in calls), calls)

    def test_invalidated_latest_obligation_carries_the_fresh_local_decision(self):
        with fixture() as case:
            case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 12345}]

            def interrupt(params):
                if "AAA" in params["symbols"].split(",") and params["adjustment"] == "raw":
                    raise ProcessInterrupted()
                return case.page(params)

            with (
                patch.object(case.pricing, "_get", side_effect=interrupt),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-17")
            root = case.store / "end_of_day/pricing/AAA"
            with storage.ticker_lock(root, exclusive=True):
                source = storage.load_source(root, "AAA")
                endpoints = storage.read_validator(root, source)
                download.publish_acquired(
                    root,
                    source,
                    "AAA",
                    source["anchor_month"],
                    source["latest_request"],
                    timestamp(source["history_start"]),
                    endpoints,
                    {},
                    None,
                    acquisition_evidence_sha256="a" * 64,
                )
            case.pages.clear()
            result = case.update("2026-10-17")
            self.assertTrue(result["complete"], result)
            calls = [p for p in case.pages if "AAA" in p["symbols"].split(",")]
            self.assertEqual(len(calls), 3, calls)  # Fresh first/tail, then raw correction using accepted all.
            self.assertTrue(all(single_session_request(p) for p in calls), calls)
            self.assertEqual(download.read_pricing_views(case.store, "AAA")["all"][-1][5], "12345")

    def test_correction_journal_recovers_payload_tail_source_and_summary_without_provider_replay(self):
        from libs.alpaca_pricing_download.contract import VALIDATOR_FILE

        for boundary in ("payload", "tail", "source", "summary"):
            with self.subTest(boundary=boundary), fixture() as case:
                case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 12345}, bar("2026-10-19")]
                replace_file = storage.os.replace
                refresh_summary = check_summary.refresh_prices
                interrupted = False

                def stop_install(source, target):
                    nonlocal interrupted
                    target = Path(target)
                    selected = "AAA" in target.parts and (
                        boundary == "payload"
                        and target.suffix == ".gz"
                        or boundary == "tail"
                        and target.name == VALIDATOR_FILE
                        or boundary == "source"
                        and target.name == "source.manifest.json"
                    )
                    if selected and not interrupted:
                        interrupted = True
                        raise ProcessInterrupted()
                    return replace_file(source, target)

                def stop_summary(root, **kwargs):
                    nonlocal interrupted
                    if boundary == "summary" and Path(root).name == "AAA" and not interrupted:
                        interrupted = True
                        raise ProcessInterrupted()
                    return refresh_summary(root, **kwargs)

                with (
                    patch.object(storage.os, "replace", side_effect=stop_install),
                    patch.object(check_summary, "refresh_prices", side_effect=stop_summary),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update("2026-10-20")
                self.assertTrue(interrupted)
                root = case.store / "end_of_day/pricing/AAA"
                self.assertTrue((root / ".pending.json").exists())
                with self.assertRaises(DownloadError):
                    download.read_pricing_views(case.store, "AAA")
                case.pages.clear()

                def no_replay(params):
                    if "AAA" in params["symbols"].split(","):
                        self.fail(f"completed correction requested again during recovery: {params}")
                    return case.page(params)

                with patch.object(case.pricing, "_get", side_effect=no_replay):
                    recovered = case.update("2026-10-20")
                self.assertTrue(recovered["complete"], recovered)
                self.assertTrue(recovered["stages"]["serving_catalogue"]["complete"])
                self.assertFalse((root / ".pending.json").exists())
                values = download.read_pricing_views(case.store, "AAA")["all"]
                self.assertEqual(values[-2][5], "12345")
                self.assertEqual(values[-1][0][:10], "2026-10-19")
                published = check_summary.read_snapshot(case.store)["entries"]
                self.assertEqual(
                    next(entry for entry in published if entry["symbol"] == "AAA")["tail_all"],
                    [list(row) for row in values],
                )

    def test_latest_correction_retains_earlier_lineage_without_cold_pricing_reads(self):
        from libs.alpaca_lineage_build import pipeline, pricing
        from libs.alpaca_serving_catalogue.producer import maintain

        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        old = lineage_fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download(bars=old)
        case.run_build()
        self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
        before = case.receipt()
        self.assertEqual(before["unresolved_rows"], 0)
        revised = [*old[:-1], {**old[-1], "v": 12345}, bar("2026-10-14")]
        publication = case.download(end="2026-10-15", bars=revised)
        correlate = pipeline.correlate

        def only_tail(store, inputs, *args, **kwargs):
            for view in inputs.values():
                self.assertEqual(len(view.views["all"]), 2)
            return correlate(store, inputs, *args, **kwargs)

        with (
            patch.object(
                pricing,
                "load_published_views",
                side_effect=AssertionError("earlier pricing reopened for lineage"),
            ),
            patch.object(pipeline, "correlate", side_effect=only_tail),
        ):
            result = case.run_build(pricing_handoff={"GOOG": publication})
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual(case.receipt()["source_rows"], before["source_rows"] + 8)
        read = Path.read_bytes

        def no_cold_prices(path):
            if path.suffix == ".gz" and path.parent.name != "Oct":
                self.fail(f"serving reopened unchanged prices: {path}")
            return read(path)

        with patch.object(Path, "read_bytes", no_cold_prices):
            serving = maintain(case.store)
        self.assertTrue(serving["complete"], serving)
        self.assertEqual(serving["full_history_evaluations"], 0, serving)
        self.assertEqual(serving["checked_bars"], 16, serving)

    def test_daily_latest_only_correction_reaches_serving_without_full_requests(self):
        with fixture() as case:
            case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 12345}, bar("2026-10-19")]
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
            old_requests = [
                p for p in case.pages if "AAA" in p["symbols"].split(",") and p["start"][:10] < "2026-10-16"
            ]
            self.assertEqual(len(old_requests), 1, case.pages)
            self.assertEqual(old_requests[0]["adjustment"], "all")
            for values in download.read_pricing_views(case.store, "AAA").values():
                self.assertEqual(values[-2][5], "12345")
                self.assertEqual(values[-1][0][:10], "2026-10-19")

    def test_restart_keeps_latest_scope_and_completed_live_observation(self):
        from libs.alpaca_lineage_build import universe_state
        from libs.common.debug_trace import DebugTrace

        with fixture() as case:
            case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 12345}, bar("2026-10-19")]

            def fail_correction(params):
                if (
                    params["symbols"] == "AAA"
                    and params["start"][:10] == "2026-10-16"
                    and params["adjustment"] == "raw"
                ):
                    raise DownloadError("correction acquisition interrupted")
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=fail_correction):
                result = case.update("2026-10-20")
            self.assertFalse(result["complete"])
            work = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertEqual(work["pricing_correction"]["scope"], "latest_session")
            case.pages.clear()

            def no_repeated_comparison(params):
                if "AAA" in params["symbols"].split(","):
                    self.assertGreaterEqual(params["start"][:10], "2026-10-16", params)
                    self.assertFalse(
                        params["start"][:10] == "2026-10-16" and params["adjustment"] == "all", params
                    )
                return case.page(params)

            trace_path = case.directory / "resumed-correction.jsonl"
            trace = DebugTrace(trace_path)
            try:
                with trace.activate(), patch.object(case.pricing, "_get", side_effect=no_repeated_comparison):
                    result = case.update("2026-10-20", retry_blocked=True)
            finally:
                trace.close("completed", 0)
            self.assertTrue(result["complete"], result)
            estimate = result["plan"]["expected_io"]
            self.assertEqual(estimate["history_check_actions"], 1)  # NEW peer; AAA retains its comparison.
            self.assertEqual(estimate["latest_session_corrections_selected"], 1)
            self.assertEqual(estimate["full_replacements_selected"], 0)
            self.assertEqual(estimate["correction_actions_estimate"], 7)
            records = [json.loads(line) for line in trace_path.read_text().splitlines()]
            traced = [record for record in records if record["stage"] == "update_work_estimate"]
            self.assertEqual(len(traced), 1)
            self.assertEqual(traced[0]["details"]["history_check_actions"], 1)
            self.assertEqual(traced[0]["details"]["latest_session_corrections_selected"], 1)


class WorkEstimateTests(unittest.TestCase):
    def test_durable_work_estimates_follow_scope_binding_and_deferral(self):
        from libs.alpaca_lineage_build.universe_plan import preview

        for mode, expected in (
            ("latest", (1, 0, 0, 7, 1)),
            ("invalidated", (0, 1, 0, 8, 1)),
            ("legacy", (0, 1, 0, 8, 1)),
            ("deferred", (0, 0, 0, 0, 0)),
            ("completed", (0, 0, 0, 0, 0)),
            ("stale", (0, 0, 2, 0, 0)),
            ("moved_store", (0, 0, 2, 0, 0)),
            ("changed_mapping", (0, 0, 2, 0, 0)),
            ("changed_scope", (0, 0, 2, 0, 0)),
            ("frozen", (1, 0, 0, 7, 2)),
            ("unchanged", (0, 0, 0, 0, 1)),
        ):
            with self.subTest(mode=mode):
                stored = rows(2, "2026-10-15")
                entry = {
                    "symbol": "AAA",
                    "asof": "2026-10-17",
                    "end_exclusive": "2026-10-17T00:00:00Z",
                    "first_all": stored[0],
                    "last_all": stored[-1],
                    "tail_all": stored,
                    "tail_version": 32,
                    "source_sha256": "a" * 64,
                }
                contract = {
                    "schema_version": 1,
                    "start": "2016-01-01T00:00:00Z",
                    "comparison_asof": entry["asof"],
                    "scope": "latest_session",
                    "source_sha256": entry["source_sha256"],
                    "asof": "2026-10-20" if mode == "frozen" else entry["asof"],
                    "end_exclusive": "2026-10-20T00:00:00Z" if mode == "frozen" else entry["end_exclusive"],
                    "comparison_end_exclusive": entry["end_exclusive"],
                    "selection": {
                        "store": "/tmp/another-store" if mode == "moved_store" else "/tmp/estimate-store",
                        "symbol": "AAA",
                        "end": "2026-10-20T00:00:00Z"
                        if mode in ("frozen", "changed_scope")
                        else entry["end_exclusive"],
                        "asof": "2026-10-20" if mode in ("frozen", "changed_mapping") else entry["asof"],
                        "generation": "b" * 64 if mode == "stale" else entry["source_sha256"],
                        "comparison_asof": entry["asof"],
                        "comparison": {"decision": "unchanged" if mode == "unchanged" else "latest_session"},
                    },
                }
                work = {
                    "acquire": False,
                    "invalidate_pricing": mode == "invalidated",
                    "correction_id": "correction",
                    "pricing_correction": None if mode == "legacy" else contract,
                }
                accepted = {
                    "store": "/tmp/estimate-store",
                    "summary": {"entries": [entry]},
                    "index": {
                        "symbols": {
                            "AAA": {
                                "source_sha256": entry["source_sha256"],
                                "projection": {
                                    "history_correction_id": "correction" if mode == "completed" else None,
                                    "pricing_request": {"history_start": "2016-01-01T00:00:00Z"},
                                },
                            }
                        }
                    },
                    "late_bar_repairs": {"sources": {}},
                    "serving_catalogue": {"pending_symbols": []},
                }
                selected = {
                    "work": {"AAA": work},
                    "deferred": [{"symbol": "AAA"}] if mode == "deferred" else [],
                }
                result = preview(
                    accepted,
                    selected,
                    {"catalogue_observed_at_utc": "2026-10-17", "records": []},
                    "2026-10-21" if mode == "frozen" else "2026-10-17",
                )["expected_io"]
                self.assertEqual(
                    tuple(
                        result[key]
                        for key in (
                            "latest_session_corrections_selected",
                            "full_replacements_selected",
                            "history_check_actions",
                            "correction_actions_estimate",
                            "pricing_publications_estimate",
                        )
                    ),
                    expected,
                )
                if mode == "frozen":
                    self.assertEqual(result["append_actions_before_reconciliation"], 16)
                    self.assertEqual(result["http_pages_minimum"], 6)


if __name__ == "__main__":
    unittest.main()
