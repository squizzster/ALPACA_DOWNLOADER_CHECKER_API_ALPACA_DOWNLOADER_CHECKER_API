"""Independent baseline preserves evidence and never consumes price histories."""

import csv
import fcntl
import io
import json
import os
import tempfile
import unittest
from contextlib import nullcontext, redirect_stderr, redirect_stdout
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_pricing_download import snapshot

from alpaca_end_of_day import ReaderBusyError, StandaloneReader, StandaloneReaderError
from alpaca_end_of_day import _published_evidence as published
from IS_ELIGIBLE_LISTED_BUSINESS_EQUITY import ROOT, VERSION, validate_version
from IS_ELIGIBLE_LISTED_BUSINESS_EQUITY.baseline import BaselineError, write_baseline
from IS_ELIGIBLE_LISTED_BUSINESS_EQUITY.cli import main


def group_fixture():
    """Two distinct identities sharing a dated ticker in one dependency group."""
    sources = {}
    securities = []
    for symbol, public_id, member, first, last in (
        ("OLD", "a" * 64, "old-id", "2020-01-02", "2020-02-03"),
        ("NEW", "b" * 64, "new-id", "2021-01-04", "2021-02-03"),
    ):
        securities.append(
            {
                "public_id": public_id,
                "tickers": ["REUSED"],
                "latest_tickers": ["REUSED"],
                "read_symbol": symbol,
                "first_session": first,
                "last_session": last,
                "adjustments": ["raw", "all"],
            }
        )
        sources[symbol] = {
            "lineage_generation": f"{symbol}-generation",
            "pricing_generation": f"{symbol}-pricing",
            "evidence": {
                "asset_lookups": [
                    {
                        "lookup": symbol,
                        "lookup_kind": "symbol",
                        "found": True,
                        "asset": {
                            "id": member,
                            "symbol": symbol,
                            "name": f"{symbol} company",
                            "exchange": "NASDAQ",
                            "class": "us_equity",
                        },
                    },
                    {
                        "lookup": "123456789",
                        "lookup_kind": "cusip",
                        "found": True,
                        "asset": {"id": member, "symbol": "REUSED", "name": "Today's unrelated name"},
                    },
                ],
                "corporate_actions": [
                    {
                        "id": "split-1",
                        "action_type": "forward_split",
                        "process_date": "2021-01-05",
                        "symbol": "REUSED",
                        "old_rate": "1",
                        "new_rate": "2",
                    }
                ],
            },
            "assignments": [
                {
                    "from": first,
                    "to": last,
                    "historical_symbol": "REUSED",
                    "canonical_security": {"alpaca_asset_id": member, "cusip": "123456789"},
                    "security_identity": {"security_id": public_id, "member_alpaca_asset_ids": [member]},
                    "ticker_ownership_interval": {
                        "historical_symbol": "REUSED",
                        "start_date_inclusive": None,
                        "end_date_exclusive": None,
                    },
                    "adjustments": ["raw", "all"],
                }
            ],
        }
    return {
        "generation": "group-generation",
        "source_symbols": ["NEW", "OLD"],
        "securities": securities,
        "sources": sources,
    }


class BusinessEquityBaselineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.output = self.directory / "baseline"
        self.group = group_fixture()
        self.roster = {
            "generation": "roster-generation",
            "group_count": 1,
            "sources": [
                {"symbol": symbol, "generation": source["lineage_generation"]}
                for symbol, source in self.group["sources"].items()
            ],
        }
        self.reader = Mock(store=self.directory / "store")
        self.reader.group = self.group
        self.selected = Mock(roster=self.roster)
        self.selected.iter_groups.side_effect = lambda: iter([self.reader.group])
        self.reader.open_published_evidence.side_effect = lambda: nullcontext(self.selected)

    def export(self):
        return write_baseline(self.reader, self.selected, self.output)

    def test_reused_ticker_preserves_ids_and_group_actions_without_inventing_dates(self):
        result = self.export()
        self.selected.iter_groups.assert_called_once_with()
        self.reader.list_published_sources.assert_not_called()
        self.reader.open_group.assert_not_called()
        self.assertEqual(result["source_count"], 2)
        self.assertEqual(result["group_count"], 1)
        self.assertEqual(result["public_id_count"], 2)
        self.assertFalse(result["assessment_performed"])
        rows = [json.loads(line) for line in (self.output / "securities.jsonl").read_text().splitlines()]
        self.assertEqual([r["public_id"] for r in rows], ["a" * 64, "b" * 64])
        self.assertEqual([r["tickers"] for r in rows], [["REUSED"], ["REUSED"]])
        self.assertEqual([r["names"] for r in rows], [["OLD company"], ["NEW company"]])
        for row in rows:
            self.assertNotIn("corporate_actions", row)
            self.assertNotIn("IS_ELIGIBLE_LISTED_BUSINESS_EQUITY", row)
            self.assertNotIn("bars", row)
            self.assertIsNone(row["metadata_observations"][0]["observed_at"])
            self.assertIsNone(row["metadata_observations"][0]["effective_from"])
            self.assertEqual(len(row["lineage_assignments"]), 1)
        groups = [json.loads(line) for line in (self.output / "groups.jsonl").read_text().splitlines()]
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["sources"], self.group["sources"])
        with (self.output / "securities.csv").open(newline="") as stream:
            flat = list(csv.DictReader(stream))
        self.assertEqual(json.loads(flat[0]["names"]), ["OLD company"])
        self.assertEqual((self.output / "public_ids.txt").read_text().splitlines(), ["a" * 64, "b" * 64])
        manifest = json.loads((self.output / "manifest.json").read_text())
        for name in manifest["files"].values():
            self.assertTrue((self.output / name).is_file())
        self.assertEqual((self.output / "definition.yaml").read_bytes(), (ROOT / "definition.yaml").read_bytes())

    def test_catalogue_observation_time_is_retained_without_effective_dates(self):
        source = self.group["sources"]["NEW"]["evidence"]
        source["catalogue_observed_at_utc"] = "2026-10-20T00:00:00Z"
        source["asset_lookups"][0]["source"] = "asset_catalogue"
        source["asset_lookups"].append(
            {**deepcopy(source["asset_lookups"][0]), "observed_at_utc": "2026-10-21T00:00:00Z"}
        )
        source["asset_lookups"][-1]["asset"]["name"] = "Conflicting name"
        self.export()
        rows = [json.loads(line) for line in (self.output / "securities.jsonl").read_text().splitlines()]
        new = rows[1]
        self.assertEqual(new["names"], ["Conflicting name", "NEW company"])
        self.assertEqual(new["metadata_observations"][0]["observed_at"], "2026-10-20T00:00:00Z")
        self.assertEqual(new["metadata_observations"][-1]["observed_at"], "2026-10-21T00:00:00Z")
        self.assertTrue(all(item["effective_from"] is None for item in new["metadata_observations"]))

    def test_selected_payload_read_failure_never_publishes_partial_run(self):
        def groups():
            yield self.group
            raise OSError("selected lineage body unavailable")

        self.selected.iter_groups.side_effect = groups
        with self.assertRaisesRegex(OSError, "selected lineage body unavailable"):
            self.export()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.directory.glob(".baseline-*")), [])

    def test_duplicate_identity_across_groups_aborts_instead_of_merging(self):
        first, second = deepcopy(self.group), deepcopy(self.group)
        first["source_symbols"], second["source_symbols"] = ["NEW"], ["OLD"]
        first["sources"] = {"NEW": first["sources"]["NEW"]}
        second["sources"] = {"OLD": second["sources"]["OLD"]}
        first["securities"] = [first["securities"][1]]
        second["securities"] = [second["securities"][0]]
        second["securities"][0]["public_id"] = "b" * 64
        second["sources"]["OLD"]["assignments"][0]["security_identity"]["security_id"] = "b" * 64
        self.selected.iter_groups.side_effect = lambda: iter([first, second])
        with self.assertRaisesRegex(BaselineError, "multiple groups"):
            self.export()
        self.assertFalse(self.output.exists())

    def test_empty_or_missing_roster_fails_closed(self):
        self.roster["sources"] = []
        with self.assertRaisesRegex(BaselineError, "no published source entries"):
            self.export()
        self.selected.iter_groups.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_refuses_existing_output_and_output_inside_source_store(self):
        self.output.mkdir()
        (self.output / "keep").write_text("retained")
        with self.assertRaises(FileExistsError):
            self.export()
        self.assertEqual((self.output / "keep").read_text(), "retained")
        with self.assertRaisesRegex(ValueError, "outside"):
            write_baseline(self.reader, self.selected, self.reader.store / "output")
        self.selected.iter_groups.assert_not_called()

    def test_interruption_and_output_failure_remove_unpublished_staging(self):
        self.selected.iter_groups.side_effect = KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.export()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.directory.glob(".baseline-*")), [])
        self.selected.iter_groups.side_effect = lambda: iter([self.group])
        with patch("IS_ELIGIBLE_LISTED_BUSINESS_EQUITY.baseline.os.replace", side_effect=OSError("disk")):
            with self.assertRaisesRegex(OSError, "disk"):
                self.export()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.directory.glob(".baseline-*")), [])

    def test_cli_plan_opens_only_roster_and_busy_has_distinct_exit_code(self):
        output = io.StringIO()
        with patch("IS_ELIGIBLE_LISTED_BUSINESS_EQUITY.cli.StandaloneReader", return_value=self.reader):
            with redirect_stdout(output):
                self.assertEqual(main(["baseline", "--plan", "--output", str(self.output)]), 0)
            self.assertEqual(json.loads(output.getvalue())["selected_source_count"], 2)
            self.selected.iter_groups.assert_not_called()
            self.assertFalse(self.output.exists())
            self.reader.open_published_evidence.side_effect = ReaderBusyError("busy")
            with redirect_stderr(io.StringIO()):
                self.assertEqual(main(["baseline", "--plan"]), 3)

    def test_definition_preserves_outcomes_as_strings_and_runtime_version_is_owned(self):
        validate_version()
        self.assertEqual(VERSION, len(json.loads((ROOT / "change_log.json").read_text())))
        definition = yaml.safe_load((ROOT / "definition.yaml").read_text())
        self.assertEqual(definition["definition_id"], "IS_ELIGIBLE_LISTED_BUSINESS_EQUITY")
        self.assertEqual(set(definition["definitions"]), {"TRUE", "FALSE"})


class BusinessEquityNativeBaselineTests(NativeCase):
    def test_real_public_reader_exports_metadata_without_opening_prices_or_changing_store(self):
        reader = StandaloneReader(self.store)
        before = snapshot(self.store)
        original_open, original_stat = Path.open, Path.stat
        metadata_opens = []

        def guard_open(path, *args, **kwargs):
            if path.is_relative_to(self.store):
                self.assertTrue(path.is_relative_to(self.root), path)
                metadata_opens.append(path)
            return original_open(path, *args, **kwargs)

        def guard_stat(path, *args, **kwargs):
            if path != self.store and path.is_relative_to(self.store):
                self.assertTrue(path.is_relative_to(self.root), path)
            return original_stat(path, *args, **kwargs)

        with (
            patch.object(Path, "open", guard_open),
            patch.object(Path, "stat", guard_stat),
            patch("hashlib.sha256", side_effect=AssertionError("rehashing")),
            patch.object(reader, "list_published_sources", side_effect=AssertionError("reselection")),
            patch.object(reader, "open_group", side_effect=AssertionError("pricing reader")),
            patch.object(published, "read_index", wraps=published.read_index) as selected_index,
        ):
            with reader.open_published_evidence() as selected:
                result = write_baseline(reader, selected, self.directory / "baseline")
        self.assertEqual(result["public_id_count"], 1)
        selected_index.assert_called_once_with(self.root)
        self.assertEqual(len(metadata_opens), 4)
        self.assertEqual(len(set(metadata_opens)), 4)
        record = json.loads((self.directory / "baseline/securities.jsonl").read_text())
        self.assertEqual(record["names"], ["TEST company"])
        self.assertEqual(record["exchanges"], ["NASDAQ"])
        self.assertEqual(record["tickers"], ["TEST"])
        self.assertTrue(record["lineage_assignments"])
        self.assertEqual(snapshot(self.store), before)

    def test_plan_uses_only_index_and_closed_snapshot_cannot_be_consumed(self):
        reader = StandaloneReader(self.store)
        with (
            patch.object(published, "read_receipt", side_effect=AssertionError("plan opened receipt")),
            patch.object(published, "read_boundaries", side_effect=AssertionError("plan opened assignments")),
            reader.open_published_evidence() as selected,
        ):
            self.assertEqual(selected.roster["group_count"], 1)
            roster = selected.roster
            roster["sources"].clear()
            self.assertEqual(len(selected.roster["sources"]), 1)
        with self.assertRaisesRegex(StandaloneReaderError, "closed"):
            list(selected.iter_groups())

    def test_one_publication_gate_keeps_snapshot_stable_and_preserves_consumer_errors(self):
        reader = StandaloneReader(self.store)
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with self.assertRaisesRegex(OSError, "consumer output failed"):
                with reader.open_published_evidence() as selected:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.assertEqual(len(list(selected.iter_groups())), 1)
                    with self.assertRaisesRegex(StandaloneReaderError, "already consumed"):
                        list(selected.iter_groups())
                    raise OSError("consumer output failed")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)

    def test_incomplete_membership_fails_without_receipt_fallback(self):
        index = published.read_index(self.root)
        index["symbols"]["TEST"].pop("group_members")
        with (
            patch.object(published, "read_index", return_value=index),
            patch.object(published, "read_receipt", side_effect=AssertionError("membership rediscovery")),
        ):
            with self.assertRaisesRegex(StandaloneReaderError, "migrate-index"):
                with StandaloneReader(self.store).open_published_evidence():
                    self.fail("incomplete selector accepted")
