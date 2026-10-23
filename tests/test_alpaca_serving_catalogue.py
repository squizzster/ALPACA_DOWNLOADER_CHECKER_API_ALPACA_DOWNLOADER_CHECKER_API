"""Serving policy, incremental replacement and interruption recovery on native stores."""

import io
import json
import sqlite3
import unittest
from contextlib import closing, nullcontext, redirect_stderr
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlite_crash_fixture import leave_hot_journal
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient, snapshot

from alpaca_end_of_day import CatalogChangedError, DataLibrary, DataLibraryError, StandaloneReader
from libs.alpaca_cli import build_parser, dispatch
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import storage, work
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.alpaca_serving_catalogue.producer import maintain, preview, status
from libs.common.sqlite_connections import connect_sqlite


class Interrupted(BaseException):
    pass


class ServingCatalogueTests(NativeCase):
    def test_scoped_maintenance_leaves_unrelated_pending_work_and_decisions_untouched(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        before = storage.controls(self.store)["groups"]["OTHER"]
        for symbol in ("TEST", "OTHER"):
            self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]), symbol)
        pending = work.read(self.store)
        result = maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["pending_symbols"], [])
        self.assertEqual(result["evaluated_groups"], 1)
        self.assertEqual(work.read(self.store), {"OTHER": pending["OTHER"]})
        self.assertEqual(storage.controls(self.store)["groups"]["OTHER"], before)
        self.assertTrue(maintain(self.store)["complete"])
        self.assertEqual(work.read(self.store), {})

    def test_focused_lineage_cli_maintains_only_its_completed_scope(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        for operation in ("update", "refresh"):
            with self.subTest(operation=operation):
                for symbol in ("TEST", "OTHER"):
                    self.publish_change(lambda r, _: r["evidence"].update(warnings=[operation]), symbol)
                args = build_parser().parse_args(
                    [
                        "lineage",
                        operation,
                        "--store",
                        str(self.store),
                        "--symbol",
                        "TEST",
                        "--through",
                        "2026-10-13",
                    ]
                )
                with (
                    patch("libs.alpaca_cli.clients", return_value=(None, None)),
                    patch(
                        "libs.alpaca_lineage_build.registration.standalone_registration",
                        return_value=nullcontext((None, SimpleNamespace(pending_work={}))),
                    ),
                    patch(
                        "libs.alpaca_lineage_build.pipeline.run_lineage",
                        return_value={"results": [{"symbol": "TEST", "processed": ["TEST"]}], "failed": 0},
                    ),
                    redirect_stderr(io.StringIO()),
                ):
                    result = dispatch(args)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["serving_catalogue"]["evaluated_groups"], 1)
                self.assertEqual(set(work.read(self.store)), {"OTHER"})

    def test_completed_group_membership_does_not_expand_through_shared_evidence_ids(self):
        self.add_symbol("OTHER")
        for symbol in ("TEST", "OTHER"):
            self.publish_change(lambda r, _: r["asset_ids"].append("shared-evidence-id"), symbol)
        self.bootstrap()
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        self.assertEqual(preview(self.store)["selected_symbols"], ["TEST"])
        events = []
        result = maintain(self.store, progress=lambda stage, event: events.append(event))
        self.assertTrue(result["complete"], result)
        self.assertEqual([event["symbols"] for event in events], [["TEST"]])

    def test_controls_status_public_snapshot_and_idle_maintenance_never_read_checkpoints(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        connect = storage.connect_sqlite

        def without_checkpoints(*args, **kwargs):
            connection = connect(*args, **kwargs)

            def authorize(action, table, column, database, trigger):
                if action == sqlite3.SQLITE_READ and table == "groups" and column == "body":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
            return connection

        with patch.object(storage, "connect_sqlite", side_effect=without_checkpoints):
            controls = storage.controls(self.store)
            self.assertEqual(status(self.store, controls=controls)["online_identities"], 2)
            self.assertEqual(status(self.store)["groups"], 2)
            self.assertEqual(preview(self.store)["selected_symbols"], [])
            self.assertEqual(DataLibrary(store=self.store).online_count(), 2)
            self.assertTrue(maintain(self.store)["complete"])
            self.assertTrue(maintain(self.store, bootstrap=True)["complete"])

    def test_changed_group_loads_only_its_own_checkpoint(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        decoded = []
        loads = json.loads

        def decode(*args, **kwargs):
            value = loads(*args, **kwargs)
            if isinstance(value, dict) and "history_validation" in value:
                decoded.append(value["locator"])
            return value

        with patch.object(json, "loads", side_effect=decode):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(decoded, ["TEST"])

    def test_changed_selector_rejects_checkpoint_read_from_older_controls(self):
        generation = self.bootstrap().catalog_generation()
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        self.assertTrue(maintain(self.store)["complete"])
        with self.assertRaisesRegex(CatalogueError, "catalogue changed"):
            storage.read_groups(self.store, {"TEST"}, generation=generation)

    def test_serving_trusts_pricing_finiteness_and_still_checks_new_numeric_domains(self):
        class FiniteDecimal(Decimal):
            def is_finite(self):
                raise AssertionError("pricing finiteness guarantee repeated")

        self.add_symbol("BAD", [{**bar, "v": -1} for bar in self.bars])
        with patch("libs.alpaca_serving_catalogue.policy.Decimal", FiniteDecimal):
            data = self.bootstrap()
        self.assertEqual([s.tickers for s in data.list_securities()], [("TEST",)])
        self.assertIn("volume_domain", self.catalogue_groups("BAD")["BAD"]["decisions"][0]["reasons"])

    def test_bootstrap_preview_is_offline_read_only_and_does_not_open_payloads(self):
        before = snapshot(self.store)
        with (
            patch.object(lineage, "read_receipt", side_effect=AssertionError("receipt")),
            patch.object(Path, "iterdir", side_effect=AssertionError("crawl")),
        ):
            report = preview(self.store, bootstrap=True)
        self.assertEqual(report["selected_symbols"], ["TEST"])
        self.assertEqual(report["receipt_reads_upper_bound"], 1)
        self.assertEqual(report["provider_requests"], 0)
        self.assertEqual(report["identity_rule_executions"], 0)
        self.assertEqual(report["pricing_writes"], 0)
        self.assertEqual(before, snapshot(self.store))

    def test_bootstrap_does_not_rewrite_or_authenticate_upstream(self):
        before = snapshot(self.store / "end_of_day/pricing")
        with (
            patch("libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("rules")),
            patch.object(Path, "iterdir", side_effect=AssertionError("crawl")),
        ):
            self.bootstrap()
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing"))

    def test_missing_boundary_requires_explicit_bootstrap(self):
        before = snapshot(self.store)
        with self.assertRaisesRegex(CatalogueError, "bootstrap"):
            maintain(self.store)
        self.assertEqual(before, snapshot(self.store))

    def test_matching_decisions_require_no_receipts_or_payload_reads(self):
        data = self.bootstrap()
        with (
            patch.object(StandaloneReader, "_open_group", side_effect=AssertionError("group opened")),
            patch(
                "libs.alpaca_serving_catalogue.producer.evaluate",
                side_effect=AssertionError("eligibility replay"),
            ),
        ):
            result = maintain(self.store)
            repeated = maintain(self.store, bootstrap=True)
        self.assertEqual(result["generation"], data.catalog_generation())
        self.assertEqual(repeated["evaluated_groups"], 0)
        self.assertEqual(repeated["reused_groups"], 1)

    def test_interruption_before_catalogue_commit_keeps_bootstrap_unselected(self):
        with patch.object(storage, "commit", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store, bootstrap=True)
        self.assertIn("TEST", work.read(self.store))
        with self.assertRaisesRegex(DataLibraryError, "incomplete"):
            DataLibrary(store=self.store)
        self.assertEqual(self.bootstrap().online_count(), 1)

    def test_interruption_inside_catalogue_transaction_rolls_back_selector_and_mappings(self):
        original = self.bootstrap().catalog_generation()
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        with patch.object(storage, "_write_state", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store)
        self.assertEqual(DataLibrary(store=self.store).catalog_generation(), original)
        self.assertIn("TEST", work.read(self.store))
        self.assertTrue(maintain(self.store)["complete"])

    def test_commit_before_acknowledgement_reuses_completed_work_without_histories(self):
        with patch.object(work, "acknowledge", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store, bootstrap=True)
        self.assertIn("TEST", work.read(self.store))
        with (
            patch.object(StandaloneReader, "_open_group", side_effect=AssertionError("history replay")),
            patch(
                "libs.alpaca_serving_catalogue.producer.evaluate", side_effect=AssertionError("decision replay")
            ),
        ):
            result = maintain(self.store, bootstrap=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_groups"], 1)
        self.assertEqual(work.read(self.store), {})

    def test_process_death_in_catalogue_transaction_is_recovered_only_by_maintenance(self):
        data = self.bootstrap()
        path = storage.path(self.store)
        leave_hot_journal(path, "DELETE FROM securities")
        with self.assertRaises(DataLibraryError):
            DataLibrary(store=self.store)
        journal = path.with_name(path.name + "-journal")
        self.assertTrue(journal.exists(), "consumer must not recover the transaction")
        with patch.object(StandaloneReader, "_open_group", side_effect=AssertionError("history replay")):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["generation"], data.catalog_generation())
        self.assertFalse(journal.exists())
        self.assertEqual(DataLibrary(store=self.store).list_securities(), data.list_securities())

    def test_process_death_in_acknowledgement_recovers_and_reuses_committed_decision(self):
        self.bootstrap()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new"]))
        with patch.object(work, "acknowledge", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store)
        generation = DataLibrary(store=self.store).catalog_generation()
        leave_hot_journal(self.root / "index.sqlite3", "DELETE FROM serving_work")
        with self.assertRaises(CatalogueError):
            work.read(self.store)
        with patch.object(StandaloneReader, "_open_group", side_effect=AssertionError("history replay")):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["generation"], generation)
        self.assertEqual(result["reused_groups"], 1)
        self.assertEqual(work.read(self.store), {})

    def test_lineage_selector_records_work_before_ack_and_recovery_preserves_it(self):
        self.bootstrap()
        with patch.object(lineage, "_acknowledge", side_effect=Interrupted()), self.assertRaises(Interrupted):
            self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        index = lineage.read_index(self.root)
        self.assertEqual(work.read(self.store)["TEST"], index["symbols"]["TEST"]["sha256"])
        self.assertTrue((self.root / ".pending.json").exists())
        with lineage.writer_lock(self.store) as root:
            self.assertTrue(lineage.recover(self.store, root))
        self.assertIn("TEST", work.read(self.store))
        self.assertTrue(maintain(self.store)["complete"])

    def test_acknowledgement_never_consumes_newer_obligation(self):
        old = work.read(self.store)
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new"]))
        work.acknowledge(self.store, old)
        self.assertIn("TEST", work.read(self.store))
        self.assertNotEqual(old["TEST"], work.read(self.store)["TEST"])

    def test_old_index_needs_explicit_queue_coverage_seed(self):
        with closing(connect_sqlite(self.root / "index.sqlite3")) as connection, connection:
            connection.execute("DROP TABLE serving_work")
            connection.execute("DROP TABLE serving_work_state")
        with self.assertRaises(CatalogueError):
            work.read(self.store)
        with patch(
            "libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("identity replay")
        ):
            self.assertEqual(self.bootstrap().online_count(), 1)

    def test_bootstrap_projects_old_group_membership_without_republishing_decisions(self):
        self.add_symbol("OTHER")
        original = lineage.read_index(self.root)
        receipts = {
            name: (self.root / name / "lineage.manifest.json").read_bytes() for name in original["symbols"]
        }
        older = deepcopy(original["symbols"])
        for entry in older.values():
            entry.pop("group_members")
        lineage._update_index(self.root, older)
        with patch.object(lineage, "read_receipt", side_effect=AssertionError("preview opened receipt")):
            plan = preview(self.store, bootstrap=True)
        self.assertEqual(plan["membership_projection_receipts"], 2)
        self.assertIsNone(plan["affected_components"])
        with self.assertRaisesRegex(CatalogueError, "coverage incomplete"):
            work.read(self.store)
        with patch(
            "libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("identity replay")
        ):
            result = maintain(self.store, bootstrap=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["membership_projection_receipts"], 2)
        projected = lineage.read_index(self.root)["symbols"]
        for name, entry in projected.items():
            self.assertEqual(entry["sha256"], original["symbols"][name]["sha256"])
            self.assertEqual(entry["group_members"], [name])
            self.assertEqual((self.root / name / "lineage.manifest.json").read_bytes(), receipts[name])
        self.assertEqual(DataLibrary(store=self.store).online_count(), 2)

    def test_policy_change_requires_explicit_rebuild_and_recalculates_decisions(self):
        data = self.bootstrap()
        original_policy = storage.policy()
        changed = {**original_policy, "version": original_policy["version"] + 1}
        with (
            patch("libs.alpaca_serving_catalogue.storage.policy", return_value=changed),
            patch("libs.alpaca_serving_catalogue.producer.policy", return_value=changed),
            patch("libs.alpaca_serving_catalogue.policy.policy", return_value=changed),
        ):
            with self.assertRaisesRegex(DataLibraryError, "policy changed"):
                DataLibrary(store=self.store)
            self.assertTrue(preview(self.store, bootstrap=True)["blockers"])
            self.assertEqual(preview(self.store, rebuild=True)["blockers"], [])
            result = maintain(self.store, rebuild=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["evaluated_groups"], 1)
            with self.assertRaises(CatalogChangedError):
                data.get_history(data.list_securities()[0].public_id)

    def test_unreadable_publication_leaves_work_pending_instead_of_marking_ineligible(self):
        payload = self.store / "end_of_day/pricing/TEST/2026/Oct/month.raw.csv.gz"
        # Select the actual current payload through its completed manifest.
        source = json.loads((payload.parents[2] / "source.manifest.json").read_text())
        month = payload.parents[2] / source["boundaries"]["current_month"]["manifest"]
        manifest = json.loads(month.read_text())
        selected = next(e for e in manifest["adjustments"] if e["adjustment"] == "raw")
        (month.parent / selected["file"]).unlink()
        result = maintain(self.store, bootstrap=True)
        self.assertFalse(result["complete"])
        self.assertIn("TEST", work.read(self.store))
        self.assertEqual(storage.controls(self.store, incomplete=True)["groups"], {})

    def test_negative_eligibility_is_completed_work(self):
        self.add_symbol("BAD", [{**b, "h": Decimal("1")} for b in self.bars])
        self.bootstrap()
        self.assertEqual(work.read(self.store), {})
        decisions = self.catalogue_groups("BAD")["BAD"]["decisions"]
        self.assertFalse(decisions[0]["eligible"])
        self.assertIn("price_bounds", decisions[0]["reasons"])

    def test_policy_rejects_numeric_timestamp_gap_and_view_failures_without_trimming(self):
        cases = {
            "NEGATIVE": [{**b, "v": Decimal("-0.0000000000001")} for b in self.bars],
            "FRACTIONCOUNT": [{**b, "n": Decimal("7.1")} for b in self.bars],
            "BOUNDS": [{**b, "h": Decimal("1")} for b in self.bars],
            "TIMESTAMP": [{**b, "t": b["t"].replace("T04:", "T05:")} for b in self.bars],
            "GAP": self.bars[:2] + self.bars[3:],
            "ZERO": [{**b, "v": 0, "n": 0, "vw": 0, "h": Decimal("1000")} for b in self.bars],
        }
        for name, rows in cases.items():
            self.add_symbol(name, rows)
        data = self.bootstrap()
        self.assertEqual([s.tickers for s in data.list_securities()], [("TEST",)])
        self.assertEqual(status(self.store)["ineligible_identities"], len(cases))
        for name in cases:
            self.assertEqual(len(StandaloneReader(self.store).get_history(name)["bars"]), len(cases[name]))

    def test_flat_zero_activity_is_eligible_and_fractional_volume_is_exact(self):
        self.add_symbol("ZERO", [{**b, "v": 0, "n": 0, "vw": 0} for b in self.bars])
        data = self.bootstrap()
        self.assertEqual(data.online_count(), 2)
        self.assertEqual(
            data.get_history(data.search_securities("ZERO")[0].public_id).bars[0].volume, Decimal(0)
        )

    def test_view_session_mismatch_and_duplicate_daily_observations_are_ineligible(self):
        class DifferentSessions(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                rows = super().fetch(symbol, adjustment, *args, **kwargs)
                return [row for row in rows if row["t"][:10] != "2026-10-06"] if adjustment == "all" else rows

        download_pricing(self.store, "VIEWS", "2026-10-14", client=DifferentSessions(self.bars))
        result = run_lineage(self.store, ["VIEWS"], identity_client=IdentityClient({"VIEWS": asset("VIEWS")}))
        self.assertEqual(result["failed"], 0, result)
        self.add_symbol("DUPLICATE")
        self.add_symbol(
            "SHIFTED",
            [{**b, "t": b["t"].replace("T04:", "T05:")} if i == 2 else b for i, b in enumerate(self.bars)],
        )
        entries = lineage.read_index(self.root)["symbols"]
        sources = {name: entries[name]["source_sha256"] for name in ("DUPLICATE", "SHIFTED")}
        # Each source has unique daily rows; the accepted group selects two
        # distinct timestamps on one day for the same published identity.
        for symbol in sources:

            def connect(receipt, boundaries, symbol=symbol):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {symbol})
                for boundary in boundaries.values():
                    for entry in boundary["adjustments"]:
                        for segment in entry["segments"]:
                            segment["security_identity"]["security_id"] = "e" * 64

            self.publish_change(connect, symbol)
        self.assertEqual(self.bootstrap().online_count(), 1)
        groups = self.catalogue_groups("VIEWS", "DUPLICATE")
        self.assertIn("incompatible_sessions", groups["VIEWS"]["decisions"][0]["reasons"])
        self.assertEqual(groups["DUPLICATE"]["decisions"][0]["reasons"], ["conflicting_daily_history"])

    def test_split_adjustment_may_round_volume_to_zero_without_losing_exact_raw_volume(self):
        class RoundedSplitVolume(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                rows = super().fetch(symbol, adjustment, *args, **kwargs)
                return (
                    [{**row, "v": 0} for row in rows] if adjustment == "all" or "split" in adjustment else rows
                )

        download_pricing(self.store, "SPLIT", "2026-10-14", client=RoundedSplitVolume(self.bars))
        result = run_lineage(self.store, ["SPLIT"], identity_client=IdentityClient({"SPLIT": asset("SPLIT")}))
        self.assertEqual(result["failed"], 0, result)
        data = self.bootstrap()
        public_id = data.search_securities("SPLIT")[0].public_id
        views = data.get_histories(public_id)
        self.assertEqual(views["raw"].bars[0].volume, Decimal("9007199254740993.5"))
        self.assertEqual(views["all"].bars[0].volume, Decimal(0))

    def test_short_rename_gap_requires_complete_unconflicted_evidence(self):
        self.add_symbol("OLD", informative_bars("2023-12-01", "2023-12-13"))
        self.add_symbol("NEW", valid_bars("2023-12-18", "2023-12-22"))
        current = asset("NEW")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2023-12-18",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        result = run_lineage(
            self.store,
            ["OLD", "NEW"],
            refresh=True,
            identity_client=IdentityClient({"OLD": current, "NEW": current, "111111111": current}, [action]),
        )
        self.assertEqual(result["failed"], 0, result)
        data = self.bootstrap()
        security = data.search_securities("NEW")[0]
        self.assertEqual(
            [d.isoformat() for d in security.explained_missing_sessions],
            ["2023-12-13", "2023-12-14", "2023-12-15"],
        )
        self.assertEqual({i.ticker for i in security.ticker_ownership_intervals}, {"OLD", "NEW"})
        for veto in ("incomplete_actions", "conflicting_action", "insufficient_action_coverage"):

            def change(receipt, _):
                evidence = receipt["evidence"]
                evidence["query"]["corporate_actions_data_quality"] = (
                    "incomplete" if veto == "incomplete_actions" else "complete"
                )
                evidence["corporate_actions"] = [
                    a for a in evidence["corporate_actions"] if a["id"] != "conflict"
                ]
                if veto == "conflicting_action":
                    evidence["corporate_actions"].append(
                        {"action_type": "forward_split", "id": "conflict", "process_date": "2023-12-14"}
                    )
                if veto == "insufficient_action_coverage":
                    evidence["query"]["corporate_actions_start"] = "2023-12-18"

            self.publish_change(change, "NEW")
            with (
                patch(
                    "libs.alpaca_lineage_build.transform.correlate",
                    side_effect=AssertionError("ownership replay"),
                ),
                patch(
                    "alpaca_end_of_day.standalone._rows",
                    side_effect=AssertionError("unchanged gap history reopened"),
                ),
            ):
                result = maintain(self.store)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["checked_bars"], 0)
            self.assertEqual(DataLibrary(store=self.store).search_securities("NEW"), ())
            decisions = self.catalogue_groups("NEW")["NEW"]["decisions"]
            self.assertIn("unexplained_missing_sessions", decisions[0]["reasons"])
            self.assertTrue(StandaloneReader(self.store).get_history("NEW")["bars"])

    def test_empty_and_entirely_unresolved_groups_have_no_public_identity(self):
        self.add_symbol("EMPTY", [])

        def unresolved(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["branch"] = "uncorrelated"
                        segment.pop("security_identity", None)

        self.publish_change(unresolved)
        data = self.bootstrap()
        self.assertEqual(data.online_count(), 0)
        self.assertEqual(status(self.store)["groups"], 2)
        self.assertEqual(work.read(self.store), {})

    def test_missing_view_remains_ineligible_but_standalone_history_is_preserved(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    if entry["adjustment"] == "all":
                        for segment in entry["segments"]:
                            segment["branch"] = "uncorrelated"

        self.publish_change(change)
        self.assertEqual(len(StandaloneReader(self.store).get_history("TEST")["bars"]), len(self.bars))
        self.assertEqual(self.bootstrap().online_count(), 0)

    def test_resolved_history_outside_unrelated_unresolved_span_stays_eligible(self):
        self.add_symbol("PART", valid_bars("2026-09-28", "2026-10-14"))

        def change(_, boundaries):
            for entry in boundaries["current_year"]["adjustments"]:
                for segment in entry["segments"]:
                    if segment["to"] < "2026-10-01":
                        segment["branch"] = "uncorrelated"
                        segment.pop("security_identity", None)

        self.publish_change(change, "PART")
        data = self.bootstrap()
        (partial,) = data.search_securities("PART")
        self.assertEqual(partial.first_session.isoformat(), "2026-10-01")
        self.assertEqual(len(data.get_history(partial.public_id).bars), len(self.bars))

    def test_overlapping_unresolved_candidates_distinguish_related_from_unrelated_identity(self):
        self.add_symbol("ALIAS")
        entries = lineage.read_index(self.root)["symbols"]
        sources = {name: entry["source_sha256"] for name, entry in entries.items()}
        for symbol in ("TEST", "ALIAS"):

            def connect(receipt, boundaries, symbol=symbol):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {symbol})
                if symbol == "ALIAS":
                    for boundary in boundaries.values():
                        for entry in boundary["adjustments"]:
                            for segment in entry["segments"]:
                                segment["branch"] = "uncorrelated"
                                segment.pop("security_identity", None)
                                segment["candidate_securities"] = [{"asset_id": "unrelated-id"}]

            self.publish_change(connect, symbol)
        data = self.bootstrap()
        self.assertEqual(data.online_count(), 1)
        public_id = data.search_securities("TEST")[0].public_id

        def related(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["candidate_securities"] = [{"asset_id": "TEST-id"}]

        self.publish_change(related, "ALIAS")
        self.assertTrue(maintain(self.store)["complete"])
        self.assertEqual(DataLibrary(store=self.store).online_count(), 0)
        self.assertTrue(StandaloneReader(self.store).get_history("TEST", public_id=public_id)["bars"])
        self.assertEqual(
            self.catalogue_groups("ALIAS")["ALIAS"]["decisions"][0]["reasons"], ["unresolved_coverage"]
        )

    def test_changed_group_retires_old_ids_and_never_recomputes_published_identity(self):
        old = self.bootstrap().list_securities()[0].public_id
        replacement = "a" * 64

        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["security_identity"]["security_id"] = replacement

        self.publish_change(change)
        self.assertTrue(maintain(self.store)["complete"])
        data = DataLibrary(store=self.store)
        self.assertEqual([s.public_id for s in data.list_securities()], [replacement])
        self.assertEqual([m.public_id for m in data.resolve_ticker("TEST")], [replacement])
        with self.assertRaises(DataLibraryError):
            data.get_security(old)

    def test_reused_ticker_keeps_dated_multiple_matches(self):
        self.add_symbol("REUSED", valid_bars("2026-09-28", "2026-10-14"))
        replacement = "b" * 64

        def change(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = replacement

        self.publish_change(change, "REUSED")
        data = self.bootstrap()
        matches = data.resolve_ticker("REUSED")
        self.assertEqual(len(matches), 2)
        self.assertEqual(len(data.resolve_ticker("REUSED", on="2026-09-29")), 1)
        self.assertEqual(data.resolve_ticker("REUSED", on="2026-10-02")[0].public_id, replacement)

    def test_ambiguous_supported_labels_are_preserved_and_csv_rejects(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    if entry["adjustment"] == "all":
                        for segment in entry["segments"]:
                            segment["historical_symbol"] = "ALIAS"

        self.publish_change(change)
        data = self.bootstrap()
        self.assertEqual(data.list_securities()[0].tickers, ("ALIAS", "TEST"))
        self.assertTrue(data.resolve_ticker("ALIAS", on="2026-10-02"))
        with self.assertRaisesRegex(DataLibraryError, "ambiguous historical ticker"):
            data.write_flat_online_csv(io.StringIO())

    def test_public_id_collision_between_incompatible_groups_is_atomic_failure(self):
        self.bootstrap()
        self.add_symbol("OTHER")
        public_id = DataLibrary(store=self.store).list_securities()[0].public_id

        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["security_identity"]["security_id"] = public_id

        self.publish_change(change, "OTHER")
        generation = DataLibrary(store=self.store).catalog_generation()
        result = maintain(self.store)
        self.assertFalse(result["complete"])
        self.assertIn("conflicting public identity", result["failures"][0]["error"])
        self.assertEqual(DataLibrary(store=self.store).catalog_generation(), generation)
        self.assertIn("OTHER", work.read(self.store))

    def test_rename_group_merges_and_splits_replace_complete_previous_contributions(self):
        # Two independent groups become one published identity and later split.
        self.add_symbol("OLD", informative_bars("2023-12-01", "2023-12-13"))
        self.add_symbol("NEW", informative_bars("2023-12-01", "2023-12-22"))
        old_ids = {s.public_id for s in self.bootstrap().list_securities() if set(s.tickers) & {"OLD", "NEW"}}
        old, new = asset("OLD"), asset("NEW")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2023-12-13",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
        }
        client = IdentityClient({"OLD": old, "NEW": new, "111111111": old, "222222222": new}, [action])
        result = run_lineage(self.store, ["NEW", "OLD"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(maintain(self.store)["complete"])
        data = DataLibrary(store=self.store)
        (merged,) = [s for s in data.list_securities() if "NEW" in s.tickers]
        self.assertEqual(merged.tickers, ("NEW", "OLD"))
        self.assertTrue(old_ids.isdisjoint(s.public_id for s in data.list_securities()))
        rows = io.StringIO()
        data.write_flat_online_csv(rows)
        self.assertIn("111111111", rows.getvalue())
        self.assertIn("222222222", rows.getvalue())
        # Synthetic completed split publications exercise contribution replacement
        # without asking serving to rerun or infer any ownership rule.
        for symbol, identity in (("OLD", "c" * 64), ("NEW", "d" * 64)):

            def split(receipt, boundaries, symbol=symbol, identity=identity):
                receipt["input_sources"] = {symbol: receipt["source_sha256"]}
                receipt["dependencies"] = []
                receipt["asset_ids"] = [symbol + "-id"]
                for boundary in boundaries.values():
                    for entry in boundary["adjustments"]:
                        for segment in entry["segments"]:
                            segment["security_identity"]["security_id"] = identity

            self.publish_change(split, symbol)
        self.assertTrue(maintain(self.store)["complete"])
        data = DataLibrary(store=self.store)
        self.assertEqual(
            {s.public_id for s in data.list_securities() if set(s.tickers) & {"OLD", "NEW"}},
            {"c" * 64, "d" * 64},
        )
        self.assertNotIn(merged.public_id, {s.public_id for s in data.list_securities()})


if __name__ == "__main__":
    unittest.main()
