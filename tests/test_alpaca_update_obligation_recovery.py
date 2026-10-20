"""Evidence supersession, inactive obligations and completed replacement reuse."""

import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.contract import document_digest
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import BarsSymbolUnavailable
from libs.alpaca_pricing_download.download import read_pricing_views


def catalogue(case, *records):
    case.records[:] = records
    case.identity = CatalogueClient(case.records)


def source(case, symbol="AAA"):
    return pricing_storage.load_source(case.store / "end_of_day/pricing" / symbol, symbol)


def full_requests(case, symbol="AAA"):
    return [
        p
        for p in case.pages
        if symbol in p["symbols"].split(",")
        and p["start"][:10] == "2016-01-01"
        and p["end"][:10] >= "2026-10-16"
    ]


class UpdateObligationRecoveryTests(unittest.TestCase):
    def interrupt(self, case, boundary, end="2026-10-17"):
        with (
            patch(boundary, side_effect=ProcessInterrupted("publication checkpoint")),
            self.assertRaises(ProcessInterrupted),
        ):
            case.update(end)

    def test_initial_identity_change_compares_unfinished_work_before_first_lineage(self):
        for boundary in ("acquire_selected", "run_lineage"):
            with self.subTest(boundary=boundary), fixture() as case:
                catalogue(case, asset("AAA"), asset("NEW", "first-id"))
                case.live["NEW"] = list(case.old)
                self.interrupt(case, "libs.alpaca_lineage_build.universe_update." + boundary)
                old_work = universe_state.read_state(case.store)["work"]["NEW"]
                catalogue(case, asset("AAA"), asset("NEW", "second-id"))
                case.live["NEW"] = [bar("2016-01-04", "50"), case.old[-1]]
                result = case.update()
                self.assertTrue(result["complete"], result)
                work = universe_state.read_state(case.store)["work"]["NEW"]
                self.assertNotEqual(work["obligation_id"], old_work["obligation_id"])
                self.assertEqual(
                    work["evidence_fingerprint"], document_digest({"lookups": work["required_lookups"]})
                )
                self.assertEqual(
                    source(case, "NEW")["acquisition_evidence_sha256"], work["acquisition_evidence_fingerprint"]
                )
                self.assertEqual(read_pricing_views(case.store, "NEW")["all"][0][4], "50")
                self.assertTrue(work["invalidate_pricing"])

    def test_identity_reversion_and_return_each_require_compatible_pricing(self):
        for boundary in ("acquire_selected", "run_lineage"):
            with self.subTest(boundary=boundary), fixture() as case:
                catalogue(case, asset("AAA", "replacement-id"))
                self.interrupt(case, "libs.alpaca_lineage_build.universe_update." + boundary)
                catalogue(case, asset("AAA"))
                self.assertTrue(case.update()["complete"])
                first_fingerprint = source(case)["acquisition_evidence_sha256"]
                catalogue(case, asset("AAA", "replacement-id"))
                case.live["AAA"] = [bar("2016-01-04", "50"), case.old[-1]]
                result = case.update()
                self.assertTrue(result["complete"], result)
                self.assertNotEqual(source(case)["acquisition_evidence_sha256"], first_fingerprint)
                self.assertEqual(read_pricing_views(case.store, "AAA")["all"][0][4], "50")

    def test_metadata_change_does_not_repeat_completed_identity_replacement(self):
        with fixture() as case:
            catalogue(case, asset("AAA", "replacement-id"))
            self.interrupt(case, "libs.alpaca_lineage_build.universe_update.run_lineage")
            before = source(case)
            catalogue(case, {**asset("AAA", "replacement-id"), "exchange": "TXSE"})
            case.pages.clear()
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertFalse(full_requests(case))
            work = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertEqual(
                work["evidence_fingerprint"], document_digest({"lookups": work["required_lookups"]})
            )
            self.assertEqual(work["acquisition_evidence_fingerprint"], before["acquisition_evidence_sha256"])
            self.assertEqual(source(case), before)

    def test_later_target_reuses_completed_identity_replacement_and_checks_increment(self):
        for boundary in ("run_lineage", "record_outcome"):
            with self.subTest(boundary=boundary), fixture() as case:
                catalogue(case, asset("AAA", "replacement-id"))
                module = "universe_update" if boundary == "run_lineage" else "universe_state"
                self.interrupt(case, f"libs.alpaca_lineage_build.{module}.{boundary}")
                before = source(case)
                case.live["AAA"] = [*case.old, bar("2026-10-19")]
                case.pages.clear()
                result = case.update("2026-10-20")
                self.assertTrue(result["complete"], result)
                self.assertFalse(full_requests(case))
                self.assertEqual(
                    source(case)["acquisition_evidence_sha256"], before["acquisition_evidence_sha256"]
                )
                self.assertEqual(source(case)["latest_request"]["end_exclusive"][:10], "2026-10-20")
                self.assertEqual(read_pricing_views(case.store, "AAA")["all"][-1][0][:10], "2026-10-19")

    def test_inactive_metadata_completion_does_not_invent_future_pricing_coverage(self):
        for end in ("2026-10-17", "2026-10-20"):
            with self.subTest(end=end), fixture() as case:
                before = source(case)
                catalogue(case, {**asset("AAA"), "status": "inactive"})
                case.pages.clear()
                result = case.update(end)
                self.assertTrue(result["complete"], result)
                work = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(work["pricing_end_exclusive"], "2026-10-17")
                self.assertEqual(work["status"], "complete")
                self.assertEqual(source(case), before)
                self.assertFalse(case.pages)

    def test_inactivity_does_not_cancel_previously_accepted_coverage(self):
        with fixture() as case:
            catalogue(case, asset("AAA"))
            case.live["AAA"] = [*case.old, bar("2026-10-19")]

            def denied(params):
                if params["start"][:10] >= "2026-10-17":
                    raise HTTPError("https://invalid", 403, "denied", {}, None)
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=denied):
                self.assertFalse(case.update("2026-10-20")["complete"])
            catalogue(case, {**asset("AAA"), "status": "inactive"})
            result = case.update("2026-10-20", retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(source(case)["latest_request"]["end_exclusive"][:10], "2026-10-20")
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")

    def test_failed_history_check_remains_executable_after_inactivity(self):
        with fixture() as case:
            catalogue(case, asset("AAA"))
            with patch.object(
                case.pricing, "_get", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
            ):
                self.assertFalse(case.update()["complete"])
            catalogue(case, {**asset("AAA"), "status": "inactive"})
            case.pages.clear()
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertTrue(case.pages)
            self.assertEqual(result["stages"]["required_history"]["checked"], ["AAA"])

    def test_inactive_retry_preserves_correction_before_and_after_pricing_publication(self):
        for boundary in ("replace_prepared", "run_lineage"):
            with self.subTest(boundary=boundary), fixture() as case:
                catalogue(case, asset("AAA"))
                case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
                self.interrupt(case, "libs.alpaca_lineage_build.refresh." + boundary)
                correction = universe_state.read_state(case.store)["work"]["AAA"]["correction_id"]
                catalogue(case, {**asset("AAA"), "status": "inactive"})
                if boundary == "replace_prepared":
                    case.live["AAA"][-1] = case.old[-1]
                case.pages.clear()
                result = case.update()
                self.assertTrue(result["complete"], result)
                self.assertEqual(source(case)["history_correction_id"], correction)
                self.assertEqual(read_pricing_views(case.store, "AAA")["all"][0][4], "50")
                if boundary == "run_lineage":
                    self.assertFalse(full_requests(case))

    def test_wrapped_retained_symbol_rejection_stays_blocked_after_retry_delay(self):
        with fixture() as case:
            catalogue(case, asset("AAA"), asset("PARTIAL"))
            case.live["PARTIAL"] = [bar("2026-10-16")]

            def partial(params):
                response = case.page(params)
                if params["adjustment"] == "all":
                    response["bars"].pop("PARTIAL", None)
                return response

            with patch.object(case.pricing, "_get", side_effect=partial):
                self.assertTrue(case.update()["complete"])
            attempts = []

            def reject(params):
                if "PARTIAL" in params["symbols"].split(",") and params["start"][:10] == "2026-10-01":
                    attempts.append(params)
                    raise BarsSymbolUnavailable("PARTIAL", 400)
                return partial(params)

            with patch.object(case.pricing, "_get", side_effect=reject):
                self.assertFalse(case.update("2026-10-20")["complete"])
                work = universe_state.read_state(case.store)["work"]["PARTIAL"]
                self.assertEqual(work["status"], "blocked")
                self.assertEqual(work["outcome"]["outcome"]["outcome"], "invalid_symbol")
                self.assertIn("established pricing acquisition", work["outcome"]["outcome"]["error"])
                before = len(attempts)
                with patch.object(universe_state, "now_utc", return_value="2099-01-01T00:00:00+00:00"):
                    self.assertFalse(case.update("2026-10-20")["complete"])
                self.assertEqual(len(attempts), before)
            with patch.object(case.pricing, "_get", side_effect=partial):
                result = case.update("2026-10-20", retry_blocked=True)
            self.assertTrue(result["complete"], result)


if __name__ == "__main__":
    unittest.main()
