"""Durable dependency work uses the lineage owner's exact-rejection contract."""

import unittest
from unittest.mock import patch

from test_alpaca_durable_maintenance import ActionCatalogueClient, ProcessInterrupted, fixture, rename
from test_alpaca_lineage_build import asset
from test_alpaca_phase4_finalization import invoke
from test_alpaca_pricing_download import snapshot
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.work_contract import obligation
from libs.alpaca_pricing_download.contract import BarsSymbolUnavailable
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_recovery_registry import load_labels


def prepare(case):
    parent = {**asset("AAA"), "exchange": "TXSE"}
    auxiliary = {**asset("DEP"), "status": "inactive", "tradable": False}
    case.identity = ActionCatalogueClient(
        [parent, auxiliary],
        [rename("AAA", "DEP")],
        exact={"AAA": parent, "DEP": auxiliary, "111111111": auxiliary},
    )
    attempts = []

    def page(params):
        if "DEP" in params["symbols"].split(","):
            attempts.append(dict(params))
            assert "DEP" in load_labels(case.registry)
            assert universe_state.read_state(case.store)["work"]["DEP"]["status"] != "complete"
            raise BarsSymbolUnavailable("DEP", 400)
        return case.page(params)

    case.enterContext(patch.object(case.pricing, "_get", side_effect=page))
    return attempts


class AuxiliaryWorkTests(unittest.TestCase):
    def assert_published(self, case):
        root = case.store / "end_of_day/lineage"
        index = storage.read_index(root)
        work = universe_state.read_state(case.store)["work"]
        self.assertTrue(all(item["status"] == "complete" for item in work.values()), work)
        self.assertNotIn("DEP", index["symbols"])
        receipt = storage.read_receipt(root, "AAA", index)
        self.assertEqual(receipt["unavailable_pricing"]["DEP"]["outcome"], "invalid_symbol")
        if receipt["unavailable_obligations"]["DEP"] != work["DEP"]["obligation_id"]:
            # A new workflow obligation may acknowledge the identical published
            # rejection without replacing the owning lineage generation.
            self.assertEqual(
                work["DEP"]["outcome"],
                {
                    "lineage_generation": index["symbols"]["AAA"]["sha256"],
                    "outcome": "unavailable_auxiliary_pricing",
                },
            )
        self.assertFalse((case.store / "end_of_day/pricing/DEP/source.manifest.json").exists())

    def test_daily_and_standalone_publish_before_acknowledging_exact_auxiliary_rejection(self):
        for command in ("daily", "standalone"):
            with self.subTest(command=command), fixture() as case:
                attempts = prepare(case)
                publish = storage.publish

                def checked_publication(*args, **kwargs):
                    work = universe_state.read_state(case.store)["work"]
                    self.assertEqual(work["DEP"]["status"], "pending")
                    return publish(*args, **kwargs)

                with patch.object(storage, "publish", side_effect=checked_publication):
                    if command == "daily":
                        result = case.update()
                        self.assertTrue(result["complete"], result)
                    else:
                        code, result, errors = invoke(
                            case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16"
                        )
                        self.assertEqual(code, 0, (result, errors))
                self.assert_published(case)
                count = len(attempts)
                self.assertGreater(count, 0)
                with patch.object(storage, "read_receipt", side_effect=AssertionError("unselected receipt")):
                    result = case.update()
                self.assertTrue(result["complete"], result)
                self.assertEqual(len(attempts), count)

    def test_interrupted_auxiliary_outcome_recovers_through_the_owning_publication(self):
        for boundary in ("acquisition", "before_publication", "journal", "acknowledgement"):
            with self.subTest(boundary=boundary), fixture() as case:
                attempts = prepare(case)
                target = {
                    "acquisition": "libs.alpaca_lineage_build.pipeline.download_pricing",
                    "before_publication": "libs.alpaca_lineage_build.storage.publish",
                    "journal": "libs.alpaca_lineage_build.storage.finish_publication",
                    "acknowledgement": "libs.alpaca_lineage_build.universe_state.record_outcome",
                }[boundary]
                with patch(target, side_effect=ProcessInterrupted()), self.assertRaises(ProcessInterrupted):
                    case.update()
                self.assertNotEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "complete")
                count = len(attempts)
                result = case.update()
                self.assertTrue(result["complete"], result)
                self.assert_published(case)
                if boundary in {"journal", "acknowledgement"}:
                    self.assertEqual(len(attempts), count)

    def test_changed_metadata_keeps_a_published_auxiliary_in_its_owning_group(self):
        with fixture() as case:
            prepare(case)
            self.assertTrue(case.update()["complete"])
            self.assert_published(case)
            parent = {**asset("AAA"), "exchange": "TXSE"}
            auxiliary = {**asset("DEP"), "status": "inactive", "tradable": False, "exchange": "OTC"}
            case.identity = ActionCatalogueClient(
                [parent, auxiliary],
                [rename("AAA", "DEP")],
                exact={"AAA": parent, "DEP": auxiliary, "111111111": auxiliary},
            )
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assert_published(case)
            item = universe_state.read_state(case.store)["work"]["DEP"]
            self.assertIn("dependency_reconciliation", item["reasons"])
            self.assertEqual(set(item["required_group"]), {"AAA", "DEP"})

    def test_explicit_update_retry_recovers_a_legacy_ungrouped_auxiliary_obligation(self):
        with fixture() as case:
            attempts = prepare(case)
            self.assertTrue(case.update()["complete"])
            before = storage.read_index(case.store / "end_of_day/lineage")
            prior = universe_state.read_state(case.store)["work"]["DEP"]
            item = obligation(
                {
                    **prior,
                    "status": "pending",
                    "reasons": ["changed_metadata"],
                    "required_group": [],
                },
                prior,
            )
            universe_state.put_work(case.store, "DEP", item)
            universe_state.record_outcome(
                case.store,
                "DEP",
                status="blocked",
                outcome={"outcome": "invalid_symbol", "http_status": 400},
            )
            count = len(attempts)
            code, _, _ = invoke(case, "update", "--through", "2026-10-16")
            self.assertEqual(code, 1)
            self.assertEqual(len(attempts), count)
            code, result, errors = invoke(
                case,
                "update",
                "--through",
                "2026-10-16",
                "--retry-blocked",
            )
            self.assertEqual(code, 0, (result, errors))
            self.assert_published(case)
            self.assertEqual(storage.read_index(case.store / "end_of_day/lineage"), before)

    def test_blocked_dependency_requires_explicit_retry_then_finishes_in_group_context(self):
        from urllib.error import HTTPError

        with fixture() as case:
            attempts = prepare(case)
            with patch.object(
                case.pricing, "execute_plan", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
            ):
                code, _, _ = invoke(case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16")
            self.assertEqual(code, 1)
            self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")
            result = case.update()
            self.assertFalse(result["complete"])
            self.assertFalse(attempts)
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assert_published(case)
            before = snapshot(case.store)
            code, result, _ = invoke(case, "status")
            self.assertEqual(code, 0, result)
            self.assertTrue(result["processing_complete"])
            self.assertEqual(snapshot(case.store), before)

    def test_standalone_reuses_publication_after_interrupted_acknowledgement(self):
        with fixture() as case:
            prepare(case)
            with (
                patch.object(universe_state, "record_outcome", side_effect=ProcessInterrupted()),
                self.assertRaises(ProcessInterrupted),
            ):
                invoke(case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16")
            with patch.object(
                case.pricing, "_get", side_effect=AssertionError("completed acquisition repeated")
            ):
                code, result, errors = invoke(
                    case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-16"
                )
            self.assertEqual(code, 0, (result, errors))
            self.assert_published(case)

    def test_rejected_discovered_seed_is_not_completed_by_an_auxiliary_relationship(self):
        with fixture() as case:
            attempts = prepare(case)
            dependency_identity = case.identity
            case.identity = CatalogueClient([asset("AAA"), asset("DEP")])
            result = case.update()
            self.assertFalse(result["complete"], result)
            self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")
            count = len(attempts)
            case.identity = dependency_identity
            result = case.update()
            self.assertFalse(result["complete"], result)
            self.assertEqual(len(attempts), count)
            result = case.update(retry_blocked=True)
            self.assertFalse(result["complete"], result)
            self.assertGreater(len(attempts), count)
            self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")

    def test_expansion_collects_a_new_members_accepted_evidence_before_completion(self):
        from test_alpaca_pricing_download import FakeClient

        with fixture() as case:
            second = asset("BBB")
            download_pricing(case.store, "BBB", "2026-10-17", client=FakeClient(case.old))
            self.assertEqual(
                run_lineage(case.store, ["BBB"], identity_client=CatalogueClient([second]))["failed"], 0
            )
            case.live["BBB"] = list(case.old)
            case.identity = CatalogueClient([asset("AAA", second["id"]), {**second, "exchange": "TXSE"}])
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertTrue(all(group["failed"] == 0 for group in result["stages"]["discovery_lineage"]), result)
            work = universe_state.read_state(case.store)["work"]
            self.assertTrue(all(item["status"] == "complete" for item in work.values()), work)


if __name__ == "__main__":
    unittest.main()
