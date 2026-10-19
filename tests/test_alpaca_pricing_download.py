import http.client
import io
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

from test_alpaca_pricing_rate_limit import FakeClock

from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.command_line import build_parser, main
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    VALIDATOR_FILE,
    VERSION,
    DownloadError,
    digest,
    json_bytes,
    timestamp,
    transform,
    validate_version,
)
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import parse_response


def bar(day, value="123.123456789012345678901234567890"):
    return {
        "t": day + "T05:00:00Z",
        "o": Decimal(value),
        "h": Decimal(value),
        "l": Decimal(value),
        "c": Decimal(value),
        "v": 9007199254740993,
        "n": 7,
        "vw": Decimal(value),
    }


class FakeClient:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []
        self.asofs = []

    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        self.calls.append((symbol, adjustment, start, end))
        self.asofs.append(asof if asof is not None else end.date().isoformat())
        return [row for row in self.bars if start <= timestamp(row["t"]) < end]


def snapshot(root):
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_ino)
        for path in root.rglob("*")
        if path.is_file()
    }


class PricingDownloadTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "pricing"
        self.root = self.store / "end_of_day/pricing/GOOG"
        self.bars = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-01-02", "2026-09-30", "2026-10-01")]

    def run_download(self, end="2026-10-14", bars=None, **kwargs):
        client = FakeClient(self.bars if bars is None else bars)
        result = download_pricing(self.store, "GOOG", end, client=client, **kwargs)
        return client, result

    def test_initial_three_boundaries_and_exact_values(self):
        client, result = self.run_download()
        self.assertEqual(len(client.calls), 16)
        self.assertEqual(result["updated_boundaries"], ["completed_years", "current_year", "current_month"])
        self.assertTrue((self.root / "completed_years.manifest.json").is_file())
        self.assertTrue((self.root / "2026/year.manifest.json").is_file())
        self.assertTrue((self.root / "2026/Oct/month.manifest.json").is_file())
        self.assertFalse((self.root / "2016").exists())
        self.assertFalse((self.root / "2026/Sep").exists())
        self.assertEqual(len(list(self.root.rglob("*.csv.gz"))), 3)
        rows = read_pricing_views(self.store, "GOOG")["raw"]
        self.assertEqual(len(rows), len(self.bars))
        self.assertEqual(rows[0][1], "123.123456789012345678901234567890")
        self.assertEqual(rows[0][5], "9007199254740993")
        source = storage.load_source(self.root, "GOOG")
        year = storage.read_boundary(self.root, source, "current_year")["raw"]
        self.assertEqual(year[-1][0], "2026-10-01T05:00:00Z")

    def test_ordinary_update_only_changes_month_and_source_receipt(self):
        self.run_download()
        before = snapshot(self.root)
        changed = self.bars + [bar("2026-10-14")]
        original_read = Path.read_bytes

        def no_unneeded_year_payload_read(path):
            if path.parent == self.root / "2026" and path.name.endswith("csv.gz"):
                self.fail("endpoint check unnecessarily opened the year snapshot")
            return original_read(path)

        with patch.object(Path, "read_bytes", no_unneeded_year_payload_read):
            client, result = self.run_download("2026-10-15", changed)
        after = snapshot(self.root)
        self.assertEqual(result["mode"], "update")
        self.assertEqual(result["consistency_check"], "matched")
        self.assertEqual(result["updated_boundaries"], ["current_month"])
        self.assertEqual(len(client.calls), 32)
        for index, adjustment in enumerate(ADJUSTMENTS):
            self.assertEqual(
                client.calls[index * 2 : index * 2 + 2],
                [
                    ("GOOG", adjustment, timestamp("2016-01-04T05:00:00Z"), timestamp("2016-01-05T05:00:00Z")),
                    ("GOOG", adjustment, timestamp("2026-10-01T04:00:00Z"), timestamp("2026-10-02T04:00:00Z")),
                ],
            )
        self.assertTrue(all(call[2] == timestamp("2026-10-01T00:00:00Z") for call in client.calls[16:24]))
        self.assertEqual(set(client.asofs), {"2026-10-15"})
        for relative in before:
            if relative not in ("source.manifest.json", VALIDATOR_FILE) and not relative.startswith("2026/Oct/"):
                self.assertEqual(before[relative], after[relative], relative)
        rows = read_pricing_views(self.store, "GOOG")["raw"]
        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[-2][1], str(self.bars[-1]["o"]))

    def test_month_rollover_finalizes_previous_month_and_retires_it(self):
        self.run_download()
        cold = (self.root / "completed_years.raw.csv.gz").stat()
        updated = self.bars + [bar("2026-10-30"), bar("2026-11-02")]
        client, result = self.run_download("2026-11-03", updated)
        self.assertEqual(result["updated_boundaries"], ["current_year", "current_month"])
        self.assertEqual(client.calls[16][2], timestamp("2026-10-01T00:00:00Z"))
        self.assertFalse((self.root / "2026/Oct").exists())
        self.assertTrue((self.root / "2026/Nov/month.manifest.json").exists())
        self.assertEqual(cold.st_ino, (self.root / "completed_years.raw.csv.gz").stat().st_ino)
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(updated))
        source = storage.load_source(self.root, "GOOG")
        self.assertEqual(len(storage.read_boundary(self.root, source, "current_year")["raw"]), 5)

    def test_year_rollover_merges_once_and_removes_old_year(self):
        self.run_download()
        updated = self.bars + [bar("2026-12-31"), bar("2027-01-04")]
        _client, result = self.run_download("2027-01-05", updated)
        self.assertIn("completed_years", result["updated_boundaries"])
        self.assertFalse((self.root / "2026").exists())
        self.assertTrue((self.root / "2027/Jan/month.manifest.json").exists())
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(updated))
        source = storage.load_source(self.root, "GOOG")
        self.assertEqual(len(storage.read_boundary(self.root, source, "completed_years")["raw"]), 6)

    def test_multiple_missed_years_are_caught_up(self):
        self.run_download()
        updated = self.bars + [bar("2027-06-01"), bar("2028-01-03"), bar("2028-02-01")]
        self.run_download("2028-02-02", updated)
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(updated))
        self.assertFalse((self.root / "2026").exists())
        self.assertFalse((self.root / "2027").exists())

    def test_explicit_full_refresh_skips_probes_and_can_revise_completed_years(self):
        self.run_download()
        revised = [bar("2016-01-04", "777.1234567890123456789")] + self.bars[1:]
        client, result = self.run_download(bars=revised, refresh_all=True)
        self.assertEqual(client.calls[0][2], timestamp("2016-01-01T00:00:00Z"))
        self.assertEqual(len(client.calls), 16)
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(read_pricing_views(self.store, "GOOG")["raw"][0][1], "777.1234567890123456789")

    def test_either_endpoint_mismatch_replaces_all_history_including_new_days(self):
        self.run_download()
        for offset, index in enumerate((0, -1)):
            with self.subTest(endpoint=index):
                end = f"2026-10-{15 + offset:02d}"
                revised = list(self.bars)
                revised[index] = {**revised[index], "c": Decimal("777.1234567890123456789")}
                # A non-endpoint revision must also arrive through the full replacement.
                revised[1] = {**revised[1], "vw": Decimal("888.00000000000000001")}
                revised.append(bar("2026-10-14"))
                client, result = self.run_download(end, revised)
                self.assertEqual(result["mode"], "full_refresh")
                self.assertEqual(result["consistency_check"], "mismatch")
                self.assertEqual(
                    result["updated_boundaries"], ["completed_years", "current_year", "current_month"]
                )
                self.assertEqual(len(client.calls), 32)
                self.assertTrue(
                    all(call[2] == timestamp("2016-01-01T00:00:00Z") for call in client.calls[16:24])
                )
                expected = transform(revised, timestamp("2016-01-01T00:00:00Z"), timestamp(end + "T00:00:00Z"))
                self.assertTrue(
                    all(rows == expected for rows in read_pricing_views(self.store, "GOOG").values())
                )
                self.assertFalse((self.root / ".pending.json").exists())
                self.assertFalse((self.root / ".staging").exists())
                self.run_download(end, self.bars, refresh_all=True)

    def test_mismatch_in_any_adjustment_or_field_triggers_full_refresh(self):
        self.run_download()
        end = date(2026, 10, 14)
        for adjustment in ADJUSTMENTS:
            for field in ("o", "h", "l", "c", "v", "n", "vw"):
                with self.subTest(adjustment=adjustment, field=field):
                    end += timedelta(days=1)
                    client = FakeClient(self.bars)
                    original_fetch = client.fetch

                    def revised(symbol, selected, start, end, **kwargs):
                        rows = original_fetch(symbol, selected, start, end, **kwargs)
                        return [
                            {**row, field: Decimal("999.000000000000001")}
                            if selected == adjustment and row["t"] == self.bars[0]["t"]
                            else row
                            for row in rows
                        ]

                    with patch.object(client, "fetch", side_effect=revised):
                        result = download_pricing(self.store, "GOOG", end.isoformat(), client=client)
                    self.assertEqual(result["mode"], "full_refresh")
                    self.assertEqual(result["consistency_check"], "mismatch")
                    self.run_download(end.isoformat(), refresh_all=True)

    def test_single_session_checked_once_per_view_and_empty_history_has_no_probes(self):
        self.run_download(bars=[])
        client, result = self.run_download("2026-10-15", bars=[bar("2026-10-01")])
        self.assertEqual(result["consistency_check"], "no_stored_sessions")
        self.assertEqual(len(client.calls), 16)
        client, result = self.run_download("2026-10-16", bars=[bar("2026-10-01"), bar("2026-10-13")])
        self.assertEqual(result["consistency_check"], "matched")
        self.assertEqual(len(client.calls), 24)
        self.assertTrue(all(call[2] == timestamp("2026-10-01T04:00:00Z") for call in client.calls[:8]))

    def test_only_endpoints_are_probed_not_the_last_five_sessions(self):
        rows = [bar(f"2026-10-{day:02d}") for day in (1, 2, 5, 6, 7, 8, 9, 12, 13)]
        self.run_download(bars=rows)
        client, result = self.run_download("2026-10-15", bars=rows)
        self.assertEqual(result["consistency_check"], "matched")
        starts = {call[2] for call in client.calls[:16]}
        self.assertEqual(starts, {timestamp("2026-10-01T04:00:00Z"), timestamp("2026-10-13T04:00:00Z")})

    def test_last_session_comes_from_authoritative_month_not_stale_year(self):
        self.run_download()
        updated = self.bars + [bar("2026-10-13")]
        self.run_download("2026-10-15", bars=updated)
        client, result = self.run_download("2026-10-16", bars=updated)
        self.assertEqual(result["mode"], "update")
        self.assertEqual({call[2] for call in client.calls[1:16:2]}, {timestamp("2026-10-13T04:00:00Z")})

    def test_empty_active_month_uses_year_or_completed_endpoints(self):
        for offset, rows in enumerate((self.bars[:-1], self.bars[:2])):
            with self.subTest(last=rows[-1]["t"]):
                self.run_download(f"2026-10-{14 + offset:02d}", bars=rows, refresh_all=True)
                client, result = self.run_download(f"2026-10-{15 + offset:02d}", bars=rows)
                self.assertEqual(result["consistency_check"], "matched")
                self.assertEqual(client.calls[1][2].date(), timestamp(rows[-1]["t"]).date())

    def test_missing_endpoint_triggers_full_replacement(self):
        self.run_download()
        _client, result = self.run_download("2026-10-15", bars=self.bars[1:])
        self.assertEqual(result["consistency_check"], "mismatch")
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(self.bars) - 1)

    def test_adjustment_views_use_their_own_endpoints_without_assuming_equal_grids(self):
        client = FakeClient(self.bars)
        original_fetch = client.fetch

        def shorter_dividend(symbol, adjustment, start, end, **kwargs):
            rows = original_fetch(symbol, adjustment, start, end, **kwargs)
            return [row for row in rows if row["t"] != self.bars[0]["t"]] if adjustment == "dividend" else rows

        with patch.object(client, "fetch", side_effect=shorter_dividend):
            download_pricing(self.store, "GOOG", "2026-10-14", client=client)
            client.calls.clear()
            result = download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertEqual(result["mode"], "update")
        self.assertEqual(client.calls[4][2], timestamp("2025-12-31T05:00:00Z"))

    def test_exact_numeric_equality_does_not_depend_on_json_spelling(self):
        rows = [bar("2016-01-04", "123.000"), bar("2026-10-01", "123.000")]
        self.run_download(bars=rows)
        equivalent = [bar("2016-01-04", "1.23E+2"), bar("2026-10-01", "123")]
        _client, result = self.run_download("2026-10-15", bars=equivalent)
        self.assertEqual(result["mode"], "update")
        self.assertEqual(result["consistency_check"], "matched")

    def test_timestamp_mismatch_also_triggers_full_refresh(self):
        self.run_download()
        revised = [{**self.bars[0], "t": "2016-01-04T06:00:00Z"}] + self.bars[1:]
        _client, result = self.run_download("2026-10-15", bars=revised)
        self.assertEqual(result["mode"], "full_refresh")

    def test_full_refresh_failure_after_mismatch_preserves_every_file(self):
        self.run_download()
        before = snapshot(self.root)
        revised = [{**self.bars[0], "c": Decimal("999")}] + self.bars[1:]
        client = FakeClient(revised)
        original_fetch = client.fetch

        def fail_refresh(symbol, adjustment, start, end, **kwargs):
            if start == timestamp("2016-01-01T00:00:00Z") and adjustment == "all":
                raise DownloadError("full refresh failed")
            return original_fetch(symbol, adjustment, start, end, **kwargs)

        with patch.object(client, "fetch", side_effect=fail_refresh):
            with self.assertRaisesRegex(DownloadError, "full refresh failed"):
                download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertEqual(before, snapshot(self.root))

    def test_probe_failure_even_after_another_mismatch_preserves_every_file(self):
        self.run_download()
        before = snapshot(self.root)
        revised = [{**self.bars[0], "c": Decimal("999")}] + self.bars[1:]
        client = FakeClient(revised)
        original_fetch = client.fetch

        def fail_probe(symbol, adjustment, start, end, **kwargs):
            if adjustment == "all":
                raise DownloadError("probe failed")
            return original_fetch(symbol, adjustment, start, end, **kwargs)

        with patch.object(client, "fetch", side_effect=fail_probe):
            with self.assertRaisesRegex(DownloadError, "probe failed"):
                download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertTrue(all(call[2] != timestamp("2016-01-01T00:00:00Z") for call in client.calls))
        self.assertEqual(before, snapshot(self.root))

    def test_malformed_probe_is_an_error_not_a_refresh_decision(self):
        self.run_download()
        before = snapshot(self.root)
        revised = [{**self.bars[0], "c": 0.1}] + self.bars[1:]
        with self.assertRaisesRegex(DownloadError, "not float"):
            self.run_download("2026-10-15", bars=revised)
        self.assertEqual(before, snapshot(self.root))

    def test_interrupted_automatic_refresh_recovers_without_mixing_old_prices(self):
        self.run_download()
        revised = [{**row, "c": Decimal("555.0000000000001")} for row in self.bars] + [bar("2026-10-14")]
        original_replace = storage.os.replace
        installed = 0

        def interrupt(source, target):
            nonlocal installed
            if Path(target).suffix == ".gz":
                installed += 1
                if installed == 2:
                    raise OSError("refresh interrupted")
            return original_replace(source, target)

        with patch.object(storage.os, "replace", interrupt):
            with self.assertRaisesRegex(OSError, "refresh interrupted"):
                self.run_download("2026-10-15", revised)
        with self.assertRaisesRegex(DownloadError, "incomplete"):
            read_pricing_views(self.store, "GOOG")
        client, result = self.run_download("2026-10-15", revised)
        self.assertTrue(result["recovered"])
        self.assertFalse(client.calls)
        expected = transform(revised, timestamp("2016-01-01T00:00:00Z"), timestamp("2026-10-15T00:00:00Z"))
        self.assertTrue(all(rows == expected for rows in read_pricing_views(self.store, "GOOG").values()))

    def test_v1_source_remains_readable_and_updates_without_rewriting_cold_files(self):
        self.run_download()
        source = storage.load_source(self.root, "GOOG")
        for reference in source["boundaries"].values():
            path = self.root / reference["manifest"]
            document = storage.read_document(path)
            document["schema_version"] = 1
            content = json_bytes(document)
            path.write_bytes(content)
            reference["sha256"] = digest(content)
        source["schema_version"] = 1
        (self.root / "source.manifest.json").write_bytes(json_bytes(source))
        before = snapshot(self.root)
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(self.bars))
        _client, result = self.run_download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertEqual(result["consistency_check"], "matched")
        self.assertEqual(storage.load_source(self.root, "GOOG")["schema_version"], VERSION)
        after = snapshot(self.root)
        for relative in before:
            if relative not in ("source.manifest.json", VALIDATOR_FILE) and not relative.startswith("2026/Oct/"):
                self.assertEqual(before[relative], after[relative])

    def test_repeating_identical_request_does_not_rewrite_any_files(self):
        self.run_download()
        before = snapshot(self.root)
        _client, result = self.run_download()
        self.assertEqual(result["files_written"], 0)
        self.assertEqual(before, snapshot(self.root))

    def test_backwards_end_refused_before_network_or_data_mutation(self):
        self.run_download()
        before = snapshot(self.root)
        client = FakeClient([])
        with self.assertRaisesRegex(DownloadError, "backwards"):
            download_pricing(self.store, "GOOG", "2026-10-10", client=client)
        self.assertFalse(client.calls)
        self.assertEqual(before, snapshot(self.root))

    def test_complete_empty_replacement_removes_old_history_and_notifies(self):
        self.run_download()
        _, result = self.run_download("2026-10-15", [])
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(read_pricing_views(self.store, "GOOG")["raw"], ())
        self.assertEqual(storage.read_update(self.root), {"from": "2016-01-04", "to": "2026-10-01"})

    def test_empty_initial_response_and_empty_new_month(self):
        self.run_download(bars=[])
        self.assertEqual(read_pricing_views(self.store, "GOOG")["raw"], ())
        self.run_download("2026-11-03", [bar("2026-10-30")])
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 1)

    def test_first_of_month_request_finalizes_previous_month(self):
        _client, result = self.run_download("2026-11-01", self.bars + [bar("2026-10-30")])
        self.assertEqual(result["anchor_month"], "2026-10")

    def test_fetch_failure_leaves_existing_publication_unchanged(self):
        self.run_download()
        before = snapshot(self.root)
        client = FakeClient(self.bars)
        with patch.object(client, "fetch", side_effect=DownloadError("network failure")):
            with self.assertRaisesRegex(DownloadError, "network failure"):
                download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertEqual(before, snapshot(self.root))

    def test_interrupted_rollover_resumes_without_redownloading(self):
        self.run_download()
        updated = self.bars + [bar("2026-10-30"), bar("2026-11-02")]
        original_replace = storage.os.replace
        installed = 0

        def interrupt(source, target):
            nonlocal installed
            if Path(target).suffix == ".gz":
                installed += 1
                if installed == 2:
                    raise OSError("simulated interruption")
            return original_replace(source, target)

        with patch.object(storage.os, "replace", interrupt):
            with self.assertRaisesRegex(OSError, "simulated interruption"):
                self.run_download("2026-11-03", updated)
        self.assertTrue((self.root / ".pending.json").exists())
        self.assertTrue((self.root / "2026/Oct/month.manifest.json").exists())
        with self.assertRaisesRegex(DownloadError, "incomplete"):
            read_pricing_views(self.store, "GOOG")
        client, result = self.run_download("2026-11-03", updated)
        self.assertTrue(result["recovered"])
        self.assertFalse(client.calls)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertFalse((self.root / ".staging").exists())
        self.assertFalse((self.root / "2026/Oct").exists())
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(updated))

    def test_staging_failure_preserves_previous_publication(self):
        self.run_download()
        before = snapshot(self.root)
        with patch.object(storage, "write_file", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                self.run_download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertEqual(before, snapshot(self.root))

    def test_publication_journal_cannot_retire_unowned_files(self):
        self.run_download()
        before = snapshot(self.root)
        journal = {
            "schema_version": 1,
            "writes": [{"path": "source.manifest.json", "sha256": "0" * 64}],
            "remove": {".download.lock": "0" * 64},
        }
        with self.assertRaisesRegex(DownloadError, "unsafe"):
            storage._finish_publication(self.root, journal)
        self.assertEqual(before, snapshot(self.root))

    def test_alias_change_retires_only_obsolete_payload(self):
        self.run_download()
        client = FakeClient(self.bars)
        original_fetch = client.fetch

        def different_dividend(symbol, adjustment, start, end, **kwargs):
            rows = original_fetch(symbol, adjustment, start, end, **kwargs)
            return [{**row, "c": Decimal("555.001")} for row in rows] if adjustment == "dividend" else rows

        with patch.object(client, "fetch", side_effect=different_dividend):
            download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertTrue((self.root / "2026/Oct/dividend.csv.gz").exists())
        self.run_download("2026-10-16")
        self.assertFalse((self.root / "2026/Oct/dividend.csv.gz").exists())
        self.assertEqual(
            read_pricing_views(self.store, "GOOG")["raw"], read_pricing_views(self.store, "GOOG")["dividend"]
        )

    def test_retirement_interruption_is_idempotently_recovered(self):
        self.run_download()
        original_unlink = Path.unlink
        old_manifest = self.root / "2026/Oct/month.manifest.json"

        def interrupt(path, *args, **kwargs):
            if path == old_manifest:
                raise OSError("retirement interrupted")
            return original_unlink(path, *args, **kwargs)

        updated = self.bars + [bar("2026-11-02")]
        with patch.object(Path, "unlink", interrupt), self.assertRaisesRegex(OSError, "retirement interrupted"):
            self.run_download("2026-11-03", updated)
        client, result = self.run_download("2026-11-03", updated)
        self.assertTrue(result["recovered"])
        self.assertFalse(client.calls)
        self.assertFalse((self.root / "2026/Oct").exists())
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), len(updated))

    def test_existing_other_format_refused_without_mutation(self):
        self.store.mkdir()
        (self.store / "store.manifest.json").write_text('{"dataset":"alpaca_end_of_day_v4"}')
        before = snapshot(self.store)
        with self.assertRaisesRegex(DownloadError, "another format"):
            self.run_download()
        self.assertEqual(before, snapshot(self.store))

    def test_symlink_ticker_refused(self):
        self.run_download()
        (self.store / "end_of_day/pricing/OTHER").symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(DownloadError, "symlink"):
            download_pricing(self.store, "OTHER", "2026-10-15", client=FakeClient([]))

    def test_gzip_corruption_stops_rollover_before_retirement(self):
        self.run_download()
        (self.root / "2026/raw.csv.gz").write_bytes(b"corruption")
        with self.assertRaisesRegex(DownloadError, "checksum"):
            self.run_download("2026-11-03", self.bars + [bar("2026-11-02")])
        self.assertTrue((self.root / "2026/Oct/month.manifest.json").exists())

    def test_version_and_no_identity_imports(self):
        validate_version()
        code = (
            "import sys; import libs.alpaca_pricing_download.command_line; "
            "assert not any('lineage' in x or 'correlation' in x or 'alpaca_end_of_day_v6' in x "
            "for x in sys.modules)"
        )
        subprocess.run([sys.executable, "-B", "-c", code], check=True)

    def test_cli_requires_explicit_destination(self):
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            main(["--symbol", "GOOG"])
        parsed = build_parser().parse_args(["--symbol", "GOOG", "--store", "/example", "--refresh-all"])
        self.assertTrue(parsed.refresh_all)


class PricingHttpTests(unittest.TestCase):
    def test_transient_transport_failures_retry_without_partial_response(self):
        for error in (
            TimeoutError("timeout"),
            ConnectionResetError("reset"),
            URLError("DNS"),
            http.client.IncompleteRead(b"partial", 10),
        ):
            with self.subTest(error=type(error).__name__):
                with patch(
                    "urllib.request.urlopen", side_effect=[error, io.BytesIO(b'{"bars":{"GOOG":[]}}')]
                ) as opener:
                    client = FakeClock().client()
                    self.assertEqual(client._get({}), {"bars": {"GOOG": []}})
                    self.assertEqual(opener.call_count, 2)

    def test_transport_retries_are_bounded_and_fail_as_download_error(self):
        with patch("urllib.request.urlopen", side_effect=TimeoutError("timeout")) as opener:
            client = FakeClock().client()
            with self.assertRaisesRegex(DownloadError, "transport failed"):
                client._get({})
            self.assertEqual(opener.call_count, 6)

    def test_exact_json_decimals_and_integer(self):
        result = parse_response(b'{"price":123.123456789012345678901234567890,"volume":9007199254740993}')
        self.assertEqual(str(result["price"]), "123.123456789012345678901234567890")
        self.assertEqual(result["volume"], 9007199254740993)

    def test_nonfinite_and_duplicate_keys_rejected(self):
        for data in (b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1,"x":2}'):
            with self.subTest(data=data), self.assertRaises(DownloadError):
                parse_response(data)

    def test_transform_rejects_float_duplicate_and_out_of_range(self):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        end = datetime(2026, 10, 15, tzinfo=timezone.utc)
        value = bar("2026-10-01")
        with self.assertRaises(DownloadError):
            transform([{**value, "o": 0.1}], start, end)
        with self.assertRaises(DownloadError):
            transform([value, value], start, end)
        with self.assertRaises(DownloadError):
            transform([bar("2026-10-15")], start, end)

    def test_pagination_and_symbol_only_endpoint(self):
        requests = []
        responses = [
            json_bytes(
                {
                    "bars": {
                        "GOOG": [
                            {**dict.fromkeys(("o", "h", "l", "c", "v", "n", "vw"), 1), "t": day + "T04:00:00Z"}
                        ]
                    },
                    "next_page_token": token,
                }
            )
            for day, token in (("2026-10-01", "page2"), ("2026-10-02", None))
        ]

        def opener(request, **_kwargs):
            requests.append(request)
            return io.BytesIO(responses.pop(0))

        client = FakeClock().client(opener=opener)
        result = client.fetch(
            "GOOG", "raw", timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-15T00:00:00Z")
        )
        self.assertEqual(len(result), 2)
        self.assertEqual([row["t"] for row in result], ["2026-10-01T04:00:00Z", "2026-10-02T04:00:00Z"])
        parameters = [parse_qs(urlparse(request.full_url).query) for request in requests]
        self.assertEqual(parameters[0]["symbols"], ["GOOG"])
        self.assertEqual(parameters[1]["page_token"], ["page2"])
        self.assertEqual(parameters[0]["end"], ["2026-10-14T23:59:59.999999Z"])
        self.assertEqual(parameters[0]["asof"], ["2026-10-15"])
        self.assertTrue(all(urlparse(request.full_url).path == "/v2/stocks/bars" for request in requests))

    def test_probe_window_keeps_the_updates_asof_mapping_through_pagination(self):
        client = FakeClock().client()
        parameters = []

        def response(query):
            parameters.append(dict(query))
            return {"bars": {"GOOG": []}, "next_page_token": "page2" if len(parameters) == 1 else None}

        with patch.object(client, "_get", side_effect=response):
            client.fetch(
                "GOOG",
                "all",
                timestamp("2016-01-04T05:00:00Z"),
                timestamp("2016-01-05T05:00:00Z"),
                asof="2026-10-14",
            )
        self.assertEqual(len(parameters), 2)
        self.assertTrue(all(query["asof"] == "2026-10-14" for query in parameters))
        self.assertTrue(all(query["start"] == "2016-01-04T05:00:00Z" for query in parameters))
        self.assertTrue(all(query["end"] == "2016-01-05T04:59:59.999999Z" for query in parameters))

    def test_wrong_symbol_and_repeated_page_tokens_rejected(self):
        for response in (
            {"bars": {"OTHER": []}, "next_page_token": None},
            {"bars": {"GOOG": []}, "next_page_token": "same"},
        ):
            client = FakeClock().client()
            with patch.object(client, "_get", return_value=response), self.assertRaises(DownloadError):
                client.fetch("GOOG", "raw", timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-15T00:00:00Z"))


if __name__ == "__main__":
    unittest.main()
