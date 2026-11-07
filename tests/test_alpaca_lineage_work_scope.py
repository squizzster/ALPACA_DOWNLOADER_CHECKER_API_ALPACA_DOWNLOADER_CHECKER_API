"""Unchanged facts cannot select historical requests, reads or calculations."""

import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_alpaca_lineage_acquisition as auxiliary_fixture
import test_alpaca_lineage_build as fixture
import test_alpaca_serving_suffix as serving_fixture
from test_alpaca_lineage_batching import Provider, event

from libs.alpaca_end_of_day_security_correlation import correlation, terminal_episode
from libs.alpaca_lineage_build import pipeline, policy_engine, storage, universe_state
from libs.alpaca_lineage_build.assignment_handoff import selected
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


class LineageWorkScopeTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_old_price_correction_keeps_actions_mappings_and_closed_assignment_bodies(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        provider = Provider(["GOOG"], [event("GOOG", cusip="111111111", process_date="2025-12-15")])
        provider.assets["111111111"] = provider.assets["GOOG"]

        def build(**kwargs):
            evidence = EvidenceClient(provider.client)
            evidence.observe_catalogue()
            provider.calls.clear()
            return case.run_build(symbols=["GOOG"], client=provider.client, evidence_client=evidence, **kwargs)

        build()
        before = {name: case.assignments(adjustment=name) for name in ADJUSTMENTS}
        publication = case.download(bars=[fixture.bar("2025-12-01", "50"), *case.bars[1:]], refresh_all=True)
        with patch.object(
            storage, "read_boundaries", side_effect=AssertionError("old output reopened for comparison")
        ):
            result = build(pricing_handoff={"GOOG": publication})
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(
            [params["start"] for kind, params in provider.calls if kind == "actions"], ["2026-10-01"]
        )
        self.assertNotIn("111111111", repr(provider.calls))
        self.assertEqual(before, {name: case.assignments(adjustment=name) for name in ADJUSTMENTS})

    def test_backward_price_extension_retains_middle_metadata_and_acquires_prefix(self):
        case = self.case
        recent = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download(bars=recent, from_date="2025-12-01")
        old = event("GOOG", "prefix", process_date="2016-02-01")
        retained = event("GOOG", "middle", process_date="2026-02-01")
        provider = Provider(["GOOG"], [old, retained])
        case.run_build(client=provider.client)
        self.assertNotIn(old, case.receipt()["evidence"]["corporate_actions"])
        publication = case.download(
            bars=[fixture.bar("2016-01-04"), *recent], from_date="2016-01-01", refresh_all=True
        )
        provider.calls.clear()
        result = case.run_build(client=provider.client, pricing_handoff={"GOOG": publication})
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(
            [(q["start"], q["end"]) for kind, q in provider.calls if kind == "actions"],
            [("2016-01-01", "2025-11-30"), ("2026-10-01", "2026-10-14")],
        )
        self.assertEqual(
            {a["id"] for a in case.receipt()["evidence"]["corporate_actions"]}, {"prefix", "middle"}
        )

    def test_descriptive_work_obligation_updates_evidence_without_price_or_assignment_reads(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        provider = Provider(["GOOG"])
        evidence = EvidenceClient(provider.client)
        evidence.observe_catalogue()
        case.run_build(symbols=["GOOG"], client=provider.client, evidence_client=evidence)
        before = case.receipt()
        universe_state.initialize(case.store)
        work = WorkPolicy(case.store, storage.read_index(case.root), {}, [], "2026-10-14")
        work.require("GOOG", "2026-10-14", reason="changed_metadata", refresh=True)
        provider.assets["GOOG"]["name"] = "Updated display name"
        evidence = EvidenceClient(provider.client)
        evidence.observe_catalogue()
        read = Path.read_bytes

        def guarded(path):
            if path.suffix == ".gz":
                self.fail(f"descriptive refresh reopened prices: {path}")
            return read(path)

        with (
            patch.object(Path, "read_bytes", guarded),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("old assignment reopened")),
        ):
            result = case.run_build(
                symbols=["GOOG"], client=provider.client, evidence_client=evidence, work_policy=work
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["pricing_rows_processed"], 0)
        after = case.receipt()
        self.assertEqual(before["boundaries"], after["boundaries"])
        self.assertEqual(before["source_sha256"], after["source_sha256"])
        self.assertIn("Updated display name", repr(after["evidence"]))

    def test_singleton_and_alias_views_complete_one_allocation(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.bars[0] = fixture.bar("2025-12-01", "50")
        case.download()
        with (
            patch.object(
                policy_engine,
                "build_security_correlation_report",
                wraps=policy_engine.build_security_correlation_report,
            ) as reports,
            patch.object(
                correlation, "correlate_download_rows", wraps=correlation.correlate_download_rows
            ) as allocations,
            patch.object(
                correlation, "has_prolonged_carry_forward", wraps=correlation.has_prolonged_carry_forward
            ) as carry,
            patch.object(
                terminal_episode, "_shared_inert_bridges", wraps=terminal_episode._shared_inert_bridges
            ) as bridges,
        ):
            result = case.run_build(symbols=["GOOG"])
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(reports.call_count, 1)
        self.assertEqual(allocations.call_count, 1)
        self.assertEqual(carry.call_count, 1)
        self.assertEqual(bridges.call_count, 1)
        self.assertEqual(case.receipt()["correlated_rows"], len(case.bars) * 8)

    def test_fixed_point_revisits_only_consumers_of_a_changed_counterpart(self):
        calls = Counter()
        view = SimpleNamespace(local={}, reader=None)
        compiled = SimpleNamespace(max_passes=3, ownership_rules=(), row_rules=())

        def loaders(_inputs, previous, _cache):
            return None, lambda name: previous.get(name), None

        def build(report, _store, **kwargs):
            symbol = report["symbol"]
            calls[symbol] += 1
            if symbol == "A":
                return {"owner": (kwargs["correlation_loader"]("B") or {}).get("owner")}
            return {"owner": symbol}

        with (
            patch.object(policy_engine, "rule_loaders", side_effect=loaders),
            patch.object(policy_engine, "build_security_correlation_report", side_effect=build),
        ):
            reports = policy_engine._correlate_until_stable(
                None, dict.fromkeys("ABC", view), dict.fromkeys("ABC", {}), None, {}, policy=compiled
            )
        self.assertEqual(reports, {"A": {"owner": "B"}, "B": {"owner": "B"}, "C": {"owner": "C"}})
        self.assertEqual(calls, {"A": 2, "B": 1, "C": 1})

    def test_group_expansion_retains_decoded_partitions_and_selected_assignment_bodies(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build(symbols=["GOOG"])
        case.download("OLD", end="2026-10-15")
        case.download(end="2026-10-15", bars=fixture.calendar_bars("2025-12-01", "2026-10-15"))
        action = {
            "action_type": "name_change",
            "id": "new-rename",
            "process_date": "2026-10-14",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = fixture.IdentityClient(
            {name: fixture.asset("GOOG") for name in ("GOOG", "OLD", "111111111")}, [action]
        )
        reads = Counter()
        read = Path.read_bytes

        def counted(path):
            if path.is_relative_to(case.store):
                reads[path] += 1
            return read(path)

        with patch.object(Path, "read_bytes", counted):
            result = case.run_build(symbols=["GOOG"], client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(set(result["results"][0]["processed"]), {"GOOG", "OLD"})
        self.assertTrue(any(path.suffix == ".gz" for path in reads))
        self.assertTrue(
            all(
                count == 1
                for path, count in reads.items()
                if path.suffix == ".gz"
                or path.name in {"month.manifest.json", "year.manifest.json", "completed_years.manifest.json"}
            )
        )
        self.assertEqual(reads[case.store / "end_of_day/pricing/GOOG/source.manifest.json"], 2)

    def test_missing_new_assignment_handoff_fails_closed(self):
        self.case.download()
        self.case.run_build()
        receipt = self.case.receipt()
        del receipt["assignment_handoffs"]["completed_years"]
        with self.assertRaisesRegex(LineageError, "missing assignment handoff"):
            selected(receipt, "completed_years")

    def test_partition_generation_change_invalidates_only_the_changed_payload(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        partitions, reads = {}, Counter()
        read = Path.read_bytes

        def counted(path):
            if path.suffix == ".gz":
                reads[path] += 1
            return read(path)

        def load():
            with storage.pricing_locks(case.store, ["GOOG"]), patch.object(Path, "read_bytes", counted):
                return pipeline.load_pricing(case.store, "GOOG", partitions=partitions)

        first = load()
        case.download(refresh_all=True, acquisition_evidence_sha256="a" * 64)
        envelope = load()
        self.assertNotEqual(first.source_sha256, envelope.source_sha256)
        self.assertEqual(first.views, envelope.views)
        self.assertTrue(all(count == 1 for count in reads.values()), reads)
        case.download(bars=[*case.bars[:-1], fixture.bar("2026-10-13", "50")], refresh_all=True)
        changed = load()
        self.assertNotEqual(envelope.views["all"][-1], changed.views["all"][-1])
        self.assertEqual(envelope.views["all"][:-1], changed.views["all"][:-1])
        # Full replacement also changes the current-year snapshot's active-month
        # overlap. Both selected changed partitions reopen; completed years do not.
        self.assertEqual(sorted(reads.values()), [1, 2, 2])

    def test_completed_maintenance_is_not_selected_again_for_discovery(self):
        from test_alpaca_durable_maintenance import fixture as update_fixture

        with update_fixture() as case:
            case.records[:] = [fixture.asset("AAA")]
            case.live["AAA"] = case.old + [fixture.bar("2026-10-19")]
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["stages"]["discovery_lineage"], [])

    def test_projection_reads_enforce_selected_paths_without_authentication(self):
        from libs.common.work_scope import UnexpectedStoreWork, selected_work

        self.case.download()
        self.case.run_build()
        receipt = self.case.receipt()
        kind = "current_month"
        handoff = selected(receipt, kind)
        expected = storage.read_boundaries(self.case.root, receipt, kinds={kind})[kind]
        with (
            selected_work(self.case.store, {"GOOG"}),
            patch.object(Path, "stat", side_effect=AssertionError("published path authenticated")),
        ):
            actual = storage.read_assignment_projection(self.case.root, receipt, kind, handoff)
        self.assertEqual(actual, expected)
        with (
            self.assertRaises(UnexpectedStoreWork),
            selected_work(self.case.store, {"OTHER"}),
            patch.object(storage, "selected_document", side_effect=AssertionError("unselected body read")),
        ):
            storage.read_assignment_projection(self.case.root, receipt, kind, handoff)
        unsafe = {**receipt, "boundaries": {kind: {"manifest": "../escape"}}}
        with (
            self.assertRaisesRegex(LineageError, "unsafe lineage path"),
            patch.object(storage, "selected_document", side_effect=AssertionError("unsafe body read")),
        ):
            storage.read_assignment_projection(self.case.root, unsafe, kind, handoff)

    def test_assignment_projection_recovers_without_reconstructing_completed_output(self):
        from libs.alpaca_lineage_build import assignment_handoff

        class Interrupted(BaseException):
            pass

        self.case.download()
        with (
            patch.object(storage, "finish_publication", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            self.case.run_build()
        with (
            patch.object(
                assignment_handoff, "produce", side_effect=AssertionError("journal output recalculated")
            ),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("journal output reopened")),
        ):
            result = self.case.run_build()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.case.receipt()["assignment_handoffs"]), 3)
        self.assertIn("serving_ownership", self.case.receipt())


class AuxiliaryWorkScopeTests(unittest.TestCase):
    def test_sibling_envelope_does_not_reject_auxiliary_again_and_new_source_invalidates_absence(self):
        case = auxiliary_fixture.AuxiliaryAcquisitionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        identities = case.prepare_entitlement()
        pricing, requests = case.rejected_client()
        first = pipeline.run_lineage(case.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(first["failed"], 0, first)
        publication = case.download(
            "BUYER", end="2026-10-15", bars=case.bars, refresh_all=True, acquisition_evidence_sha256="a" * 64
        )
        with patch.object(
            pipeline, "download_pricing", side_effect=AssertionError("accepted absence reacquired")
        ):
            result = pipeline.run_lineage(
                case.store,
                ["BUYER"],
                identity_client=identities,
                pricing_client=pricing,
                pricing_handoff={"BUYER": publication},
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(requests), 1)
        self.assertEqual(result["results"][0]["pricing_rows_processed"], 0)
        case.download("ENTITLE", end="2026-10-15", bars=case.bars)
        result = pipeline.run_lineage(case.store, ["BUYER"], identity_client=identities)
        self.assertEqual(result["failed"], 0, result)
        self.assertIn("ENTITLE", result["results"][0]["processed"])
        self.assertFalse(result["results"][0]["unavailable_pricing"])


class PartialServingWorkScopeTests(unittest.TestCase):
    def test_legacy_range_change_requires_explicit_date_assembly_without_numeric_replay(self):
        from libs.alpaca_serving_catalogue import storage as serving_storage

        case = serving_fixture.ServingSuffixTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.overlap()
        case.bootstrap()
        previous = case.catalogue_groups("ALIAS")
        for record in previous["ALIAS"]["history_validation"]["prefix_records"]:
            record.pop("session_dates")
            record.pop("session_projection_version")
        serving_storage.commit(case.store, previous, list(previous.values()))
        case.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new metadata"]), "TEST")
        read = Path.read_bytes

        def closed(path):
            if path.suffix == ".gz":
                self.fail("ordinary maintenance reconstructed a missing legacy projection")
            return read(path)

        with (
            patch.object(policy, "_unresolved_ends", return_value=["2026-10-06"]),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed numbers rechecked")),
        ):
            with patch.object(Path, "read_bytes", closed):
                deferred = maintain(case.store)
            self.assertFalse(deferred["complete"])
            self.assertIn("explicit catalogue rebuild", repr(deferred["failures"]))
            rebuilt = maintain(case.store, rebuild=True)
        self.assertTrue(rebuilt["complete"], rebuilt)
        self.assertEqual(rebuilt["checked_bars"], 0)
        current = case.catalogue_groups("ALIAS")["ALIAS"]
        self.assertEqual(current["securities"][0]["first_session"], "2026-10-07")
        self.assertIn("session_dates", current["history_validation"]["prefix_records"][0])

    def test_partial_projection_preserves_numbers_across_metadata_and_uncertainty_changes(self):
        case = serving_fixture.ServingSuffixTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.overlap()
        case.bootstrap()
        prior = case.catalogue_groups("ALIAS")["ALIAS"]
        read = Path.read_bytes

        def guarded(path):
            if path.suffix == ".gz" or path.name in {
                "month.manifest.json",
                "year.manifest.json",
                "completed_years.manifest.json",
            }:
                self.fail(f"serving reopened accepted history: {path}")
            return read(path)

        for change in ("metadata", "uncertainty"):

            def mutate(receipt, boundaries):
                if change == "metadata":
                    receipt["evidence"]["warnings"] = ["new metadata"]
                else:
                    for boundary in boundaries.values():
                        for view in boundary["adjustments"]:
                            for segment in view["segments"]:
                                segment["candidate_securities"] = [{"asset_id": "different-identity"}]

            case.publish_change(mutate, "TEST" if change == "metadata" else "ALIAS")
            with (
                patch.object(Path, "read_bytes", guarded),
                patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted numbers rechecked")),
            ):
                result = maintain(case.store)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["checked_bars"], 0)
            after = case.catalogue_groups("ALIAS")["ALIAS"]
            if change == "metadata":
                self.assertEqual(prior["securities"], after["securities"])
                self.assertEqual(prior["decisions"], after["decisions"])
            else:
                self.assertEqual(after["securities"][0]["first_session"], "2026-10-01")
                self.assertEqual(after["securities"][0]["coverage"]["session_count"], len(case.bars))
