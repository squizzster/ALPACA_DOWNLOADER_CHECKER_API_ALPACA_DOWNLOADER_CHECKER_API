"""Completed operation facts survive planning, scope expansion and publication."""

import unittest
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import pipeline, pricing, resume
from libs.alpaca_pricing_download import append, check_summary


class OperationHandoffTests(unittest.TestCase):
    def test_daily_append_retains_accepted_facts_through_serving(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.registry.write_text("AAA\n")
            case.live = {"AAA": [*case.old, bar("2026-10-19")]}
            held, validated, compiled, corrections = [], set(), set(), set()
            validate = check_summary.validate_entry
            compile_need = append._append_need
            correction = pipeline.correction_from
            digest = pricing.document_digest

            def once_validate(entry):
                self.assertNotIn(id(entry), validated, "accepted summary validated again")
                validated.add(id(entry))
                held.append(entry)
                return validate(entry)

            def once_plan(entry, target):
                key = (id(entry), target)
                self.assertNotIn(key, compiled, "applicable append plan recomputed")
                compiled.add(key)
                held.append(entry)
                return compile_need(entry, target)

            def once_correction(source, previous):
                key = (id(source), id(previous))
                self.assertNotIn(key, corrections, "completed correction decision repeated")
                corrections.add(key)
                held.extend((source, previous))
                return correction(source, previous)

            def supplied_generation(value):
                self.assertNotIn("latest_request", value, "selected source generation rehashed")
                return digest(value)

            with (
                patch.object(check_summary, "validate_entry", side_effect=once_validate),
                patch.object(append, "validate_entry", side_effect=once_validate),
                patch.object(append, "_append_need", side_effect=once_plan),
                patch.object(pipeline, "correction_from", side_effect=once_correction),
                patch.object(resume, "correction_from", side_effect=once_correction),
                patch.object(pricing, "correction_from", side_effect=once_correction),
                patch.object(pricing, "document_digest", side_effect=supplied_generation),
            ):
                result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(validated and compiled and corrections)

    def test_append_plan_is_invalidated_by_new_target_and_new_accepted_entry(self):
        from libs.alpaca_pricing_download.contract import timestamp

        with fixture() as case:
            entry = check_summary.read_snapshot(case.store)["entries"][0]
            target = timestamp("2026-10-20T00:00:00Z")
            with patch.object(append, "_append_need", wraps=append._append_need) as compile_need:
                first, _ = append.append_needs([entry], target)
                again, _ = append.append_needs([entry], target)
                self.assertIs(first[0], again[0])
                self.assertEqual(compile_need.call_count, 1)
                append.append_needs([entry], timestamp("2026-10-21T00:00:00Z"))
                self.assertEqual(compile_need.call_count, 2)
                replacement = check_summary.AcceptedEntry(dict(entry), entry.serialized)
                append.append_needs([replacement], target)
                self.assertEqual(compile_need.call_count, 3)
            with self.assertRaises(TypeError):
                entry["last_all"][1] = "changed"
            with self.assertRaises(TypeError):
                entry["asof"] = "changed"
