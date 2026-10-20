import copy
import io
import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_cli import main
from libs.alpaca_lineage_build import check_summary as projection
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import check_summary as summary
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import DownloadError, digest, json_bytes
from libs.alpaca_pricing_download.download import download_pricing


class CheckSummaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dates = patch(
            "libs.alpaca_pricing_download.download.utc_now",
            return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
        )
        cls.dates.start()
        cls.addClassCleanup(cls.dates.stop)
        cls.fixture = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.base = Path(cls.fixture.name) / "fixture"
        cls.rows = [bar("2026-10-01"), bar("2026-10-02")]
        download_pricing(cls.base, "GOOG", "2026-10-14", client=FakeClient(cls.rows))
        result = run_lineage(cls.base, identity_client=IdentityClient())
        if result["failed"]:
            raise AssertionError(result)
        projection.rebuild(cls.base)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "store"
        shutil.copytree(self.base, self.store)
        self.pricing = self.store / "end_of_day/pricing/GOOG"
        self.lineage = self.store / "end_of_day/lineage"

    def read(self):
        return summary.read_snapshot(self.store)

    def download(self, end="2026-10-15", rows=None):
        return download_pricing(self.store, "GOOG", end, client=FakeClient(self.rows if rows is None else rows))

    def build(self, **kwargs):
        result = run_lineage(self.store, identity_client=IdentityClient(), **kwargs)
        self.assertEqual(result["failed"], 0, result)
        return result

    def test_rebuild_preserves_all_receipts_and_payloads_and_exact_minimal_values(self):
        before = snapshot(self.store / "end_of_day")
        with (
            patch.object(FakeClient, "fetch", side_effect=AssertionError("provider")),
            patch.object(IdentityClient, "get_asset_lookup", side_effect=AssertionError("asset query")),
            patch.object(pricing_storage, "read_boundary", side_effect=AssertionError("history")),
        ):
            result = projection.rebuild(self.store)
        self.assertEqual(snapshot(self.store / "end_of_day"), before)
        current = self.read()
        self.assertEqual(result["tickers"], 1)
        self.assertEqual(result["sha256"], digest(json_bytes(current["entries"])))
        entry = current["entries"][0]
        self.assertEqual(set(entry), {"symbol", "asof", "last_all"})
        self.assertEqual(entry["asof"], "2026-10-14")
        self.assertEqual(entry["last_all"][0], "2026-10-02T05:00:00Z")
        self.assertEqual(entry["last_all"][1], "123.123456789012345678901234567890")
        self.assertEqual(entry["last_all"][5:], ["9007199254740993", "7", entry["last_all"][1]])

    def test_fast_read_and_cli_require_no_ticker_io_or_credentials_or_writes(self):
        before = snapshot(self.store)
        output = io.StringIO()
        with (
            patch.object(Path, "iterdir", side_effect=AssertionError("directory scan")),
            patch.dict("os.environ", {}, clear=True),
            redirect_stdout(output),
        ):
            self.assertEqual(main(["status", "--store", str(self.store)]), 1)
        self.assertEqual(json.loads(output.getvalue())["summary"]["tickers"], 1)
        self.assertEqual(snapshot(self.store), before)

    def test_pricing_updates_existing_baseline_without_waiting_for_lineage(self):
        previous = self.read()
        summary.assert_current(self.store, previous["token"])
        self.download(rows=self.rows + [bar("2026-10-14", "456.000000000000000001")])
        current = self.read()
        self.assertNotEqual(previous["sha256"], current["sha256"])
        self.assertNotEqual(previous["token"], current["token"])
        self.assertEqual(current["entries"][0]["asof"], "2026-10-15")
        self.assertEqual(current["entries"][0]["last_all"][1], "456.000000000000000001")
        with self.assertRaisesRegex(DownloadError, "baseline changed"):
            summary.assert_current(self.store, previous["token"])

    def test_identical_download_and_reused_lineage_do_not_rewrite_summary(self):
        before = snapshot(self.store)
        self.download(end="2026-10-14")
        self.build(symbols=["GOOG"])
        self.assertEqual(snapshot(self.store), before)

    def test_interrupted_pricing_blocks_summary_until_recovery(self):
        replace = pricing_storage.os.replace
        target = self.pricing / "source.manifest.json"

        def interrupt(source, destination):
            if Path(destination) == target:
                raise OSError("interrupted source publication")
            return replace(source, destination)

        with patch.object(pricing_storage.os, "replace", side_effect=interrupt), self.assertRaises(OSError):
            self.download()
        with self.assertRaisesRegex(DownloadError, "pending inputs"):
            self.read()
        with patch.object(FakeClient, "fetch", side_effect=AssertionError("provider")):
            self.assertTrue(self.download()["recovered"])
        self.assertEqual(self.read()["entries"][0]["asof"], "2026-10-15")

    def test_lineage_eligibility_removal_and_recovery_are_automatic(self):
        client = IdentityClient({"GOOG": {**asset("GOOG"), "status": "inactive"}})
        with patch.object(projection, "refresh", side_effect=OSError("interrupted summary publication")):
            result = run_lineage(self.store, ["GOOG"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 1)
        with self.assertRaisesRegex(DownloadError, "pending inputs"):
            self.read()
        self.download()  # A newer price publication must not clear the interrupted status change.
        with self.assertRaisesRegex(DownloadError, "pending inputs"):
            self.read()
        with lineage_storage.writer_lock(self.store) as root:
            self.assertTrue(lineage_storage.recover(self.store, root))
        self.assertEqual(self.read()["entries"], [])

    def test_new_active_ticker_is_added_by_normal_lineage_publication(self):
        download_pricing(self.store, "OTHER", "2026-10-14", client=FakeClient(self.rows + [bar("2026-10-14")]))
        with self.assertRaisesRegex(DownloadError, "pending inputs"):
            self.read()
        client = IdentityClient({"GOOG": asset("GOOG"), "OTHER": asset("OTHER")})
        result = run_lineage(self.store, identity_client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual([row["symbol"] for row in self.read()["entries"]], ["GOOG", "OTHER"])

    def test_membership_uses_only_unique_stored_active_same_symbol_equity(self):
        receipt = json.loads((self.lineage / "GOOG/lineage.manifest.json").read_bytes())
        lookup = next(item for item in receipt["evidence"]["asset_lookups"] if item["lookup"] == "GOOG")
        for edits in ({"status": "inactive"}, {"symbol": "OTHER"}, {"class": "crypto"}, {"status": None}):
            with self.subTest(edits=edits):
                value = copy.deepcopy(receipt)
                value["evidence"]["asset_lookups"] = [{**lookup, "asset": {**lookup["asset"], **edits}}]
                self.assertFalse(projection.stored_active(value))
        for lookups in ([], [lookup, lookup], [{**lookup, "asset": None}]):
            value = copy.deepcopy(receipt)
            value["evidence"]["asset_lookups"] = lookups
            self.assertFalse(projection.stored_active(value))
        self.assertTrue(projection.stored_active(receipt))

    def test_empty_active_source_is_omitted_then_added_when_rows_arrive(self):
        download_pricing(self.store, "OTHER", "2026-10-14", client=FakeClient([]))
        client = IdentityClient({"GOOG": asset("GOOG"), "OTHER": asset("OTHER")})
        self.assertEqual(run_lineage(self.store, identity_client=client)["failed"], 0)
        self.assertEqual(len(self.read()["entries"]), 1)
        download_pricing(self.store, "OTHER", "2026-10-15", client=FakeClient(self.rows + [bar("2026-10-14")]))
        with self.assertRaisesRegex(DownloadError, "pending inputs"):
            self.read()
        self.assertEqual(run_lineage(self.store, identity_client=client)["failed"], 0)
        self.assertEqual(len(self.read()["entries"]), 2)

    def test_deleted_summary_needs_explicit_rebuild_and_cannot_appear_empty_valid(self):
        summary.summary_path(self.store).unlink()
        with self.assertRaisesRegex(DownloadError, "missing"):
            self.read()
        self.download()
        self.build()
        self.assertFalse(summary.summary_path(self.store).exists())
        projection.rebuild(self.store)
        self.assertEqual(self.read()["entries"][0]["asof"], "2026-10-15")

    def test_failed_rebuild_cannot_publish_partial_coverage(self):
        with patch.object(projection, "project", side_effect=OSError("unreadable receipt")):
            with self.assertRaises(OSError):
                projection.rebuild(self.store)
        with self.assertRaisesRegex(DownloadError, "complete rebuild"):
            self.read()
        projection.rebuild(self.store)
        self.assertEqual(len(self.read()["entries"]), 1)

    def test_concurrent_publication_during_rebuild_cannot_be_overwritten(self):
        entries = self.read()["entries"]
        token = summary.begin_rebuild(self.store)
        summary.invalidate(self.store, ["GOOG"])
        with self.assertRaisesRegex(DownloadError, "changed during summary rebuild"):
            summary.finish_rebuild(self.store, token, entries)
        with self.assertRaisesRegex(DownloadError, "complete rebuild"):
            self.read()

    def test_recreated_summary_rejects_old_rebuild_token(self):
        entries = self.read()["entries"]
        old = summary.begin_rebuild(self.store)
        summary.summary_path(self.store).unlink()
        replacement = summary.begin_rebuild(self.store)
        with self.assertRaisesRegex(DownloadError, "changed during summary rebuild"):
            summary.finish_rebuild(self.store, old, entries)
        summary.finish_rebuild(self.store, replacement, entries)
        self.assertNotEqual(self.read()["token"][0], old[0])

    def test_rebuild_trusts_published_validator_while_explicit_audit_checks_bytes(self):
        expected = self.read()["entries"]
        path = self.pricing / "validator_first_last_sessions.json"
        path.write_bytes(path.read_bytes() + b" ")
        before = snapshot(self.store / "end_of_day")
        projection.rebuild(self.store)
        self.assertEqual(self.read()["entries"], expected)
        with self.assertRaisesRegex(DownloadError, "checksum differs"):
            pricing_storage.authenticate_validator(
                self.pricing, pricing_storage.load_source(self.pricing, "GOOG")
            )
        self.assertEqual(snapshot(self.store / "end_of_day"), before)

    def test_rebuild_rejects_pending_publications_without_recovering_them(self):
        pending = self.lineage / ".pending.json"
        pending.write_text("{}")
        with self.assertRaisesRegex(RuntimeError, "interrupted lineage"):
            projection.rebuild(self.store)
        self.assertEqual(pending.read_text(), "{}")

    def test_grouping_preserves_mapping_date_and_uses_new_york_session_date(self):
        entry = self.read()["entries"][0]
        other = copy.deepcopy(entry)
        other["symbol"] = "OTHER"
        other["asof"] = "2026-10-15"
        other["last_all"][0] = "2026-10-03T00:00:00Z"  # Still October 2 in New York.
        summary.replace_entries(self.store, {"OTHER": other})
        result = summary.describe(self.read())
        self.assertEqual((result["tickers"], result["last_session_dates"], result["query_groups"]), (2, 1, 2))

    def test_invalid_summary_rows_fail_closed_and_have_no_extra_fact_columns(self):
        with sqlite3.connect(summary.summary_path(self.store)) as db:
            self.assertEqual(
                [row[1] for row in db.execute("PRAGMA table_info(entries)")], ["symbol", "asof", "last_all"]
            )
            db.execute("UPDATE entries SET last_all='[\"bad\"]'")
        with self.assertRaisesRegex(DownloadError, "invalid pricing check row"):
            self.read()

    def test_offline_rebuild_cli_needs_no_credentials(self):
        output = io.StringIO()
        with patch.dict("os.environ", {}, clear=True), redirect_stdout(output):
            self.assertEqual(main(["summary", "rebuild", "--store", str(self.store)]), 0)
        self.assertEqual(json.loads(output.getvalue())["indexed_tickers"], 1)


if __name__ == "__main__":
    unittest.main()
