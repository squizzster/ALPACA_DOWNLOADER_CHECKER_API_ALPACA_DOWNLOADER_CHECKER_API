"""Fresh workflow approval must not republish unchanged data decisions."""

import copy
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import pipeline, storage, universe_state, universe_update
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download.download import read_pricing_views
from libs.alpaca_pricing_download.storage import load_source
from libs.alpaca_serving_catalogue import producer
from libs.alpaca_serving_catalogue import storage as serving_storage


class ApprovalHandoffTests(unittest.TestCase):
    @contextmanager
    def ready_case(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            self.assertTrue(case.update()["complete"])
            yield case

    def publications(self, case):
        root = case.store / "end_of_day/lineage"
        index = storage.read_index(root)
        return {
            "entry": index["symbols"]["AAA"],
            "receipt": storage.read_receipt(root, "AAA", index),
            "source": load_source(case.store / "end_of_day/pricing/AAA", "AAA"),
            "serving_generation": serving_storage.controls(case.store)["state"]["generation"],
        }

    def deny_check(self, case):
        with patch.object(
            case.pricing,
            "_get",
            side_effect=HTTPError("https://invalid", 403, "synthetic denied check", {}, None),
        ):
            result = case.update()
        self.assertFalse(result["complete"])
        item = universe_state.read_state(case.store)["work"]["AAA"]
        self.assertEqual(item["status"], "blocked")
        self.assertTrue(item["required_history_check"])

    def assert_no_serving_work(self, result):
        serving = result["stages"]["serving_catalogue"]
        self.assertEqual(serving["evaluated_groups"], 0)
        self.assertEqual(serving["published_components"], 0)
        self.assertEqual(serving["checked_bars"], 0)
        self.assertEqual(serving["full_history_evaluations"], 0)

    def test_same_cutoff_retry_completes_without_reopening_or_republishing_data(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            approved = False
            maintain = universe_update.update_everything
            read_bytes = Path.read_bytes

            def completed_maintenance(*args, **kwargs):
                nonlocal approved
                result = maintain(*args, **kwargs)
                approved = True
                return result

            def read(path):
                if path.is_relative_to(case.store):
                    self.assertNotIn("lineage", path.parts, path)
                    self.assertNotEqual(path.suffix, ".gz", path)
                    if approved:
                        self.assertNotEqual(path.name, "source.manifest.json", path)
                return read_bytes(path)

            with (
                patch.object(universe_update, "update_everything", completed_maintenance),
                patch.object(Path, "read_bytes", read),
                patch.object(storage, "read_receipt", side_effect=AssertionError("receipt reopened")),
                patch.object(storage, "publish", side_effect=AssertionError("unchanged lineage republished")),
                patch.object(pipeline, "correlate", side_effect=AssertionError("identity replay")),
                patch.object(producer, "evaluate", side_effect=AssertionError("serving replay")),
                patch.object(
                    serving_storage, "commit", side_effect=AssertionError("unchanged serving republished")
                ),
            ):
                result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(self.publications(case), before)
            self.assert_no_serving_work(result)
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")
            self.assert_no_serving_work(case.update())

    def test_blocked_check_cannot_complete_from_unchanged_publications(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            with patch.object(case.pricing, "_get", side_effect=AssertionError("restricted request")):
                result = case.update()
            self.assertFalse(result["complete"])
            self.assertEqual(self.publications(case), before)
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "blocked")

    def test_approval_for_another_source_generation_cannot_complete_work(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            maintain = universe_update.update_everything

            def stale_approval(*args, **kwargs):
                result = maintain(*args, **kwargs)
                kwargs["work_policy"].history_approved["AAA"] = "f" * 64
                return result

            with patch.object(universe_update, "update_everything", stale_approval):
                result = case.update(retry_blocked=True)
            self.assertFalse(result["complete"])
            self.assertEqual(self.publications(case), before)
            self.assertNotEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")

    def test_metadata_change_still_requires_new_lineage_publication(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            case.records[:] = [{**asset("AAA"), "name": "Changed company description"}]
            case.identity = CatalogueClient(case.records)
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            after = self.publications(case)
            self.assertNotEqual(after["receipt"]["evidence"], before["receipt"]["evidence"])
            self.assertEqual(after["source"], before["source"])
            self.assertNotEqual(after["entry"]["sha256"], before["entry"]["sha256"])
            self.assertNotEqual(after["serving_generation"], before["serving_generation"])

    def test_corrected_prices_still_require_new_data_publications(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            case.live["AAA"] = [case.old[0], bar("2026-10-16", "55")]
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            after = self.publications(case)
            self.assertNotEqual(after["source"], before["source"])
            self.assertNotEqual(after["entry"]["sha256"], before["entry"]["sha256"])
            self.assertEqual(read_pricing_views(case.store, "AAA")["all"][-1][4], "55")
            self.assertNotEqual(after["serving_generation"], before["serving_generation"])

    def test_interruption_before_acknowledgement_keeps_retry_without_data_churn(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            record = universe_state.record_outcome

            def interrupted(store, symbol, **kwargs):
                if symbol == "AAA" and kwargs["status"] == "complete":
                    raise ProcessInterrupted("before workflow acknowledgement")
                return record(store, symbol, **kwargs)

            with (
                patch.object(universe_state, "record_outcome", interrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update(retry_blocked=True)
            self.assertNotEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")
            self.assertEqual(self.publications(case), before)
            case.pages.clear()
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertTrue(case.pages, "an uncommitted approval must receive a fresh check")
            self.assertEqual(self.publications(case), before)
            self.assert_no_serving_work(result)

    def test_interruption_after_acknowledgement_does_not_require_data_publication(self):
        with self.ready_case() as case:
            before = self.publications(case)
            self.deny_check(case)
            record = universe_state.record_outcome

            def interrupted(store, symbol, **kwargs):
                result = record(store, symbol, **kwargs)
                if symbol == "AAA" and kwargs["status"] == "complete":
                    raise ProcessInterrupted("after workflow acknowledgement")
                return result

            with (
                patch.object(universe_state, "record_outcome", interrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update(retry_blocked=True)
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")
            self.assertEqual(self.publications(case), before)
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(self.publications(case), before)
            self.assert_no_serving_work(result)

    def test_producer_completion_expires_when_any_group_generation_or_obligation_changes(self):
        with self.ready_case() as case:
            entry = self.publications(case)["entry"]
            entries = {
                symbol: {
                    **copy.deepcopy(entry),
                    "sha256": symbol.lower() * 21 + "0",
                    "dependencies": [other],
                    "group_members": ["AAA", "BBB"],
                }
                for symbol, other in (("AAA", "BBB"), ("BBB", "AAA"))
            }
            item = {
                "obligation_id": "checked-obligation",
                "end_exclusive": "2026-10-17",
                "refresh_metadata": True,
                "required_group": ["AAA", "BBB"],
            }
            policy = WorkPolicy(case.store, {"symbols": entries}, {"AAA": item}, [], "2026-10-17")
            policy.accept_completion(
                {
                    "AAA": {"update_obligation_id": item["obligation_id"]},
                    "BBB": {"update_obligation_id": None},
                },
                entries,
                {"AAA": "AAA"},
            )
            self.assertEqual(policy.satisfying_entry("AAA", entries, {}), entries["AAA"])
            for changed in ("AAA", "BBB"):
                with self.subTest(changed=changed):
                    newer = copy.deepcopy(entries)
                    newer[changed]["sha256"] = "f" * 64
                    self.assertIsNone(policy.satisfying_entry("AAA", newer, {}))
            self.assertIsNone(policy.satisfying_entry("AAA", {"AAA": entries["AAA"]}, {}))
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {"BBB": 1}))
            policy.blocked.add("BBB")
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {}))
            policy.blocked.clear()
            policy.work["AAA"] = {**item, "obligation_id": "later-failed-check"}
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {}))

    def test_unavailable_auxiliary_completion_expires_when_pricing_appears(self):
        with self.ready_case() as case:
            entry = self.publications(case)["entry"]
            entry = {**entry, "dependencies": ["AUX"], "group_members": ["AAA", "AUX"]}
            entries = {"AAA": entry}
            work = {
                symbol: {
                    "obligation_id": symbol + "-checked",
                    "end_exclusive": "2026-10-17",
                    "refresh_metadata": True,
                    "required_group": ["AAA", "AUX"],
                    "reasons": ["dependency_reconciliation"],
                }
                for symbol in ("AAA", "AUX")
            }
            policy = WorkPolicy(case.store, {"symbols": entries}, work, [], "2026-10-17")
            policy.accept_completion(
                {
                    "AAA": {
                        "update_obligation_id": work["AAA"]["obligation_id"],
                        "unavailable_pricing": {"AUX": {"outcome": "invalid_symbol"}},
                        "unavailable_obligations": {"AUX": work["AUX"]["obligation_id"]},
                    }
                },
                entries,
                {"AAA": "AAA", "AUX": "AAA"},
            )
            self.assertEqual(policy.satisfying_entry("AUX", entries, {}), entry)
            self.assertIsNone(policy.satisfying_entry("AUX", entries, {"AUX": 1}))
            newer = {**entries, "AUX": {**copy.deepcopy(entry), "sha256": "f" * 64}}
            self.assertIsNone(policy.satisfying_entry("AUX", newer, {}))
            policy.work["AUX"] = {**work["AUX"], "obligation_id": "new-auxiliary-requirement"}
            self.assertIsNone(policy.satisfying_entry("AUX", entries, {}))

    def test_producer_input_acceptance_still_requires_fresh_history_approval(self):
        with self.ready_case() as case:
            before = self.publications(case)
            entries = {"AAA": before["entry"]}
            item = {
                "obligation_id": "fresh-inputs-awaiting-check",
                "end_exclusive": "2026-10-17",
                "refresh_metadata": True,
                "required_history_check": True,
                "required_group": ["AAA"],
            }
            policy = WorkPolicy(case.store, {"symbols": entries}, {"AAA": item}, [], "2026-10-17")
            receipts = {"AAA": copy.deepcopy(before["receipt"])}
            completed = policy.stamp(receipts, {"AAA": before["source"]})
            self.assertEqual(completed, {"AAA": "AAA"})
            self.assertNotEqual(receipts["AAA"].get("update_obligation_id"), item["obligation_id"])
            policy.accept_completion(receipts, entries, completed)
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {}))
            policy.history_approved["AAA"] = "f" * 64
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {}))
            policy.history_approved["AAA"] = before["entry"]["source_sha256"]
            self.assertEqual(policy.satisfying_entry("AAA", entries, {}), before["entry"])
            policy.work["AAA"] = {**item, "obligation_id": "subsequent-check-failure"}
            self.assertIsNone(policy.satisfying_entry("AAA", entries, {}))

    def test_fresh_approval_does_not_erase_substantive_unfinished_requirements(self):
        with self.ready_case() as case:
            entry = self.publications(case)["entry"]
            item = {
                "obligation_id": "fresh-check",
                "end_exclusive": "2026-10-17",
                "required_history_check": True,
                "refresh_metadata": False,
            }
            for name, requirement in (
                ("coverage", {"end_exclusive": "2026-10-20"}),
                ("metadata", {"refresh_metadata": True}),
                ("correction", {"correction_id": "unperformed-correction"}),
                (
                    "mapping",
                    {"invalidate_pricing": True, "acquisition_evidence_fingerprint": "unperformed-replacement"},
                ),
                ("missing group", {"required_group": ["AAA", "MISSING"]}),
            ):
                with self.subTest(requirement=name):
                    policy = WorkPolicy(
                        case.store,
                        {"symbols": {"AAA": entry}},
                        {"AAA": {**item, **requirement}},
                        [],
                        "2026-10-20",
                    )
                    policy.history_approved["AAA"] = entry["source_sha256"]
                    self.assertIsNone(policy.satisfying_entry("AAA", {"AAA": entry}, {}))
            incompatible = copy.deepcopy(entry)
            incompatible["versions"]["correlation"] -= 1
            policy = WorkPolicy(case.store, {"symbols": {"AAA": incompatible}}, {"AAA": item}, [], "2026-10-17")
            policy.history_approved["AAA"] = entry["source_sha256"]
            self.assertIsNone(policy.satisfying_entry("AAA", {"AAA": incompatible}, {}))
