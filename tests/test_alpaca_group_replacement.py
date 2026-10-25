"""Complete-group replacement identities survive missing labels and interruptions."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import ActionCatalogueClient, ProcessInterrupted, fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_phase4_finalization import invoke
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import storage, universe_plan, universe_state
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.universe_update import rebuild_universe
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_serving_catalogue.producer import maintain


def seed(case, *, fingerprint="a" * 64, empty=False, missing=False):
    case.store = case.directory / "merger"
    second = {**asset("BBB"), "status": "inactive"}
    exact = {"AAA": asset("AAA"), "BBB": second, "111111111": asset("AAA"), "222222222": second}
    if missing:
        exact.pop("BBB")
    action = {
        "action_type": "stock_merger",
        "id": "merger",
        "process_date": "2026-09-01",
        "effective_date": "2026-09-01",
        "acquirer_symbol": "AAA",
        "acquiree_symbol": "BBB",
        "acquirer_cusip": "111111111",
        "acquiree_cusip": "222222222",
        "old_rate": 1,
        "new_rate": 1,
    }
    for symbol in ("AAA", "BBB"):
        download_pricing(
            case.store,
            symbol,
            "2026-10-17",
            client=FakeClient([] if empty and symbol == "BBB" else case.old),
            acquisition_evidence_sha256=fingerprint,
        )
    case.assertFalse(run_lineage(case.store, identity_client=IdentityClient(exact, [action]))["failed"])
    rebuild(case.store)
    rebuild_universe(case.store)
    case.assertTrue(maintain(case.store, bootstrap=True)["complete"])
    current = asset("AAA", "replacement-id")
    case.identity = ActionCatalogueClient([current], [action], exact={**exact, "AAA": current})
    case.live = {
        "AAA": case.old,
        "BBB": [] if empty else [bar("2016-01-04", "50"), bar("2026-10-16", "100")],
    }
    case.pages.clear()
    return second


def source(case, symbol):
    return storage.read_document(case.store / f"end_of_day/pricing/{symbol}/source.manifest.json")


class GroupReplacementTests(unittest.TestCase):
    def assert_replaced(self, case, result):
        self.assertTrue(result["complete"], result)
        work = universe_state.read_state(case.store)["work"]
        fingerprints = {work[s]["acquisition_evidence_fingerprint"] for s in ("AAA", "BBB")}
        self.assertEqual(len(fingerprints), 1)
        self.assertNotIn(None, fingerprints)
        for symbol in ("AAA", "BBB"):
            self.assertEqual(work[symbol]["status"], "complete")
            self.assertIn(source(case, symbol)["acquisition_evidence_sha256"], fingerprints)
        self.assertEqual(
            [r[4] for r in read_pricing_views(case.store, "BBB")["raw"]],
            [str(r["c"]) for r in case.live["BBB"]],
        )

    def test_omitted_member_is_replaced_once_with_or_without_previous_fingerprint(self):
        for fingerprint, empty, missing in (
            (None, False, False),
            ("a" * 64, False, False),
            ("a" * 64, True, False),
            ("a" * 64, True, True),
        ):
            with self.subTest(fingerprint=fingerprint, empty=empty, missing=missing), fixture() as case:
                seed(case, fingerprint=fingerprint, empty=empty, missing=missing)
                events = []
                iterdir = Path.iterdir

                def selected_directories(path):
                    self.assertNotIn(path, (case.store, case.store / "end_of_day/pricing"))
                    return iterdir(path)

                with patch.object(Path, "iterdir", selected_directories):
                    result = case.update(progress=lambda stage, value: events.append(stage))
                self.assert_replaced(case, result)
                self.assertFalse(any(s.endswith("dependency_acquisition") for s in events))
                self.assertEqual(
                    {v["symbol"]: v["status"] for v in result["stages"]["acquisition"]["results"]},
                    {"AAA": "published", "BBB": "published"},
                )

    def test_another_group_mapping_change_replaces_the_unchanged_member_again(self):
        with fixture() as case:
            second = seed(case)
            case.identity.records.append(second)
            self.assert_replaced(case, case.update())
            before = source(case, "BBB")["acquisition_evidence_sha256"]
            current = asset("AAA", "second-replacement-id")
            case.identity.records[0] = case.identity.assets["AAA"] = current
            case.live["BBB"] = [bar("2016-01-04", "75"), bar("2026-10-16", "100")]
            case.pages.clear()
            result = case.update()
            self.assert_replaced(case, result)
            self.assertNotEqual(source(case, "BBB")["acquisition_evidence_sha256"], before)
            self.assertTrue(any("BBB" in p["symbols"].split(",") for p in case.pages))

    def test_interrupted_replacement_survives_metadata_change_and_catalogue_omission(self):
        for metadata_change in (False, True):
            with self.subTest(metadata_change=metadata_change), fixture() as case:
                seed(case)
                with (
                    patch(
                        "libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=ProcessInterrupted()
                    ),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update()
                before = {s: source(case, s) for s in ("AAA", "BBB")}
                if metadata_change:
                    current = {**case.identity.assets["AAA"], "name": "Updated metadata"}
                    case.identity.records[0] = case.identity.assets["AAA"] = current
                case.pages.clear()

                def no_replacement(params):
                    self.assertGreaterEqual(params["start"][:10], "2026-10-15", params)
                    return case.page(params)

                with patch.object(case.pricing, "_get", side_effect=no_replacement):
                    result = case.update()
                self.assert_replaced(case, result)
                self.assertEqual({s: source(case, s) for s in before}, before)

    def test_legacy_null_requirement_is_bound_without_repeating_completed_sibling(self):
        for entrypoint in ("daily", "lineage"):
            with self.subTest(entrypoint=entrypoint), fixture() as case:
                seed(case)
                select = universe_plan.select_work

                def legacy(*args, **kwargs):
                    selection = select(*args, **kwargs)
                    selection["work"]["BBB"].update(
                        evidence_fingerprint=None, acquisition_evidence_fingerprint=None, required_lookups={}
                    )
                    return selection

                with (
                    patch.object(universe_plan, "select_work", side_effect=legacy),
                    patch(
                        "libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=ProcessInterrupted()
                    ),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update()
                before = copy.deepcopy(source(case, "AAA"))
                self.assertEqual(source(case, "BBB")["acquisition_evidence_sha256"], "a" * 64)
                case.live["BBB"] = [bar("2016-01-04", "75"), bar("2026-10-16", "100")]
                case.pages.clear()

                def only_unbound_replacement(params):
                    if params["start"][:10] < "2026-10-15":
                        self.assertNotIn("AAA", params["symbols"].split(","))
                    return case.page(params)

                with patch.object(case.pricing, "_get", side_effect=only_unbound_replacement):
                    if entrypoint == "daily":
                        result = case.update()
                    else:
                        code, result, errors = invoke(
                            case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-16"
                        )
                        self.assertEqual(code, 0, (result, errors))
                self.assert_replaced(case, result)
                self.assertEqual(source(case, "AAA"), before)

    def test_metadata_only_group_change_preserves_prices(self):
        with fixture() as case:
            seed(case)
            current = {**asset("AAA"), "name": "Updated metadata"}
            case.identity.records[0] = case.identity.assets["AAA"] = current
            before = source(case, "BBB")
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertFalse(result["plan"]["work"]["BBB"]["invalidate_pricing"])
            self.assertEqual(source(case, "BBB"), before)
            self.assertFalse(any("BBB" in p["symbols"].split(",") for p in case.pages))
