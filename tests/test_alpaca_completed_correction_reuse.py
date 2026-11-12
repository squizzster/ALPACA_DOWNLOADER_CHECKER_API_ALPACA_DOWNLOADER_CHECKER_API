"""Completed unchanged corrections survive workflow renewal and real JSON boundaries."""

import copy
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.completed_corrections import applicable
from libs.alpaca_pricing_download import correction, refresh
from libs.alpaca_pricing_download.contract import VALIDATOR_FILE, timestamp


class CompletedCorrectionReuseTests(unittest.TestCase):
    def test_same_witness_reuses_completed_disposition_after_json_without_history_io(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = case.old
            page = case.pricing._get

            def context_difference(params):
                values = copy.deepcopy(page(params))
                if params["adjustment"] == "all" and timestamp(params["end"]) - timestamp(
                    params["start"]
                ) < timedelta(days=2):
                    for rows in values["bars"].values():
                        for row in rows:
                            if row["t"] == case.old[0]["t"]:
                                row.update({k: bar("2016-01-04", "50")[k] for k in ("o", "h", "l", "c", "vw")})
                return values

            with patch.object(case.pricing, "_get", side_effect=context_difference):
                first = case.update("2026-10-17")
                self.assertTrue(first["complete"], first)
                state = universe_state.read_state(case.store)
                accepted = state["work"]["AAA"]
                self.assertEqual(accepted["status"], "complete")
                self.assertEqual(
                    accepted["pricing_correction"]["source_sha256"],
                    accepted["correction_completion"]["source_sha256"],
                )
                cid = accepted["correction_id"]
                read = Path.read_bytes

                def no_history(path):
                    if path.suffix == ".gz" or path.name == VALIDATOR_FILE:
                        raise AssertionError("accepted history reopened: " + str(path))
                    return read(path)

                with (
                    patch.object(
                        refresh,
                        "acquire_full",
                        side_effect=AssertionError("completed full acquisition repeated"),
                    ),
                    patch.object(Path, "read_bytes", no_history),
                    patch.object(
                        correction,
                        "validate_durable_observations",
                        side_effect=AssertionError("completed evidence revalidated"),
                    ),
                ):
                    second = case.update("2026-10-17")
                self.assertTrue(second["complete"], second)
                self.assertEqual(second["stages"]["maintenance"]["stages"]["refresh"]["pricing"]["results"], [])
                case.live["AAA"] = case.old + [bar("2026-10-19")]
                with patch.object(
                    refresh, "acquire_full", side_effect=AssertionError("accepted prefix reacquired")
                ):

                    class Interrupted(BaseException):
                        pass

                    record = universe_state.record_outcome

                    def interrupt(store, symbol, **kwargs):
                        if kwargs.get("status") == "complete":
                            raise Interrupted()
                        return record(store, symbol, **kwargs)

                    with patch.object(universe_state, "record_outcome", side_effect=interrupt):
                        with self.assertRaises(Interrupted):
                            case.update("2026-10-20")
                    with patch.object(
                        correction,
                        "validate_durable_observations",
                        side_effect=AssertionError("accepted correction replayed after interruption"),
                    ):
                        advanced = case.update("2026-10-20")
                    self.assertTrue(advanced["complete"], advanced)
                    again = case.update("2026-10-20")
                    self.assertTrue(again["complete"], again)
                self.assertEqual(again["stages"]["maintenance"]["stages"]["refresh"]["pricing"]["results"], [])
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["correction_id"], cid)

    def test_actual_observation_generation_mapping_or_scope_change_revokes_disposition(self):
        entry = {"symbol": "AAA", "asof": "2026-10-17", "end_exclusive": "2026-10-17T00:00:00Z"}
        row = ("2026-10-16T04:00:00Z", "1")
        item = {
            "correction_id": "id",
            "pricing_correction": {
                "schema_version": 3,
                "scope": "full_history",
                "cause": "selected_all_rows_changed",
                "store": "/store",
                "symbol": "AAA",
                "source_sha256": "generation",
                "asof": entry["asof"],
                "end_exclusive": entry["end_exclusive"],
                "observations": {"all": [list(row)]},
            },
            "correction_completion": {"correction_id": "id", "source_sha256": "generation"},
        }
        args = dict(store="/store", entry=entry, generation="generation", live_all=(row,))
        self.assertTrue(applicable(item, **args))
        for changed in (
            dict(args, generation="new"),
            dict(args, store="/other"),
            dict(args, live_all=(("different", "1"),)),
            dict(args, entry={**entry, "asof": "2026-10-18"}),
            dict(args, invalidated=True),
        ):
            self.assertFalse(applicable(item, **changed))
        carried = copy.deepcopy(item)
        carried["correction_completion"]["source_sha256"] = "appended"
        extended = {**entry, "asof": "2026-10-20", "end_exclusive": "2026-10-20T00:00:00Z"}
        composed = dict(
            store="/store",
            entry=extended,
            generation="appended",
            live_all=(row, ("2026-10-19T04:00:00Z", "2")),
            changed_sessions=(row[0],),
        )
        self.assertTrue(applicable(carried, **composed))
        self.assertFalse(applicable(carried, **{**composed, "added_sessions": ("2026-10-18T04:00:00Z",)}))
        self.assertFalse(applicable(carried, **{**composed, "changed_sessions": ("2026-10-19T04:00:00Z",)}))
        new = copy.deepcopy(item)
        new["correction_completion"]["source_sha256"] = "changed"
        self.assertFalse(applicable(new, **args))
