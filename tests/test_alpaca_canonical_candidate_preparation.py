"""A scoped canonical comparison owns its proposed production artifacts before review."""

import json
import tempfile
from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_former_ticker_pipeline import traded_history
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_lineage_build import candidate_preparation as preparation
from libs.alpaca_lineage_build import group_engine, pipeline, storage
from libs.alpaca_lineage_build.contract import LineageError, document_digest
from libs.alpaca_lineage_build.prepared_recovery import accept_candidate
from libs.alpaca_lineage_build.retained_recovery import recover
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError


class Market:
    def __init__(self, failed=None):
        self.failed = failed
        self.calls = []
        self.native = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.market_observation_handoff_token = self.native.market_observation_handoff_token

    def get_trade_tape_sample(self, symbol, session, *, feed, asof, **kwargs):
        route = symbol, session, feed, asof
        self.calls.append(route)
        if self.failed == route:
            raise RuntimeError("interrupted request")

        def response(_url):
            if feed == "otc":
                raise IdentityHttpError(403, "subscription does not permit querying OTC market data")
            return {"trades": [{"t": session + "T15:01:00Z", "z": "C", "x": "P"}], "symbol": symbol}

        with patch.object(self.native, "request_json", side_effect=response):
            return self.native.get_trade_tape_sample(symbol, session, feed=feed, asof=asof, **kwargs)


class CanonicalPreparationTests(TestCase):
    def test_real_publication_is_reviewed_and_installation_never_repeats_its_calculation(self):
        with fixture() as case:
            path = case.store.parent / "prepared.json"
            with (
                patch.object(storage, "publish", side_effect=AssertionError("wrote to production")),
                patch.object(group_engine, "make_publication", wraps=group_engine.make_publication) as build,
                patch.object(
                    group_engine, "correlate_components", wraps=group_engine.correlate_components
                ) as evaluate,
            ):
                result = preparation.prepare(case.store, "AAA", path)
            self.assertFalse(result["reused"])
            self.assertEqual((build.call_count, evaluate.call_count), (1, 1))
            saved = json.loads(path.read_bytes())
            self.assertEqual(saved["review"], {"version": 1, "status": "pending"})
            self.assertTrue(saved["comparison"]["comparison"]["candidate_partition_complete"])
            with self.assertRaisesRegex(LineageError, "approved"):
                accept_candidate(saved, case.store)
            with (
                patch.object(preparation, "load_pricing", side_effect=AssertionError("read pricing payload")),
                patch.object(storage, "read_receipt", side_effect=AssertionError("reread receipt")),
                patch.object(storage, "read_boundaries", side_effect=AssertionError("reread assignments")),
            ):
                self.assertTrue(preparation.prepare(case.store, "AAA", path)["reused"])
                with self.assertRaisesRegex(LineageError, "different preparation inputs"):
                    preparation.prepare(
                        case.store,
                        "AAA",
                        path,
                        market_clients=Mock(side_effect=AssertionError("opened provider")),
                    )
            saved["review"] = {"version": 1, "status": "approved", "conclusion": "Verified fixture outcome."}
            selection = accept_candidate(saved, case.store)
            self.assertEqual(saved["output_generation"], document_digest(saved["candidate_receipts"]))
            prior = deepcopy(saved)
            prior.pop("output_generation")
            self.assertEqual(
                accept_candidate(prior, case.store).groups[0].operation_id, document_digest(prior["preparation"])
            )
            distinct = {**saved, "output_generation": "b" * 64}
            self.assertNotEqual(
                accept_candidate(distinct, case.store).groups[0].operation_id, selection.groups[0].operation_id
            )
            with (
                patch.object(group_engine, "make_publication", side_effect=AssertionError("repeated assembly")),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("reloaded prices")),
                patch.object(
                    group_engine, "correlate_components", side_effect=AssertionError("repeated correlation")
                ),
            ):
                self.assertEqual(recover(case.store, None, selection=selection)["published_prepared_groups"], 1)

    def test_pending_lineage_and_pricing_and_changed_source_reject_before_prices_or_observations(self):
        with fixture() as case:
            path = case.store.parent / "proposal.json"
            for root in ("lineage", "pricing/AAA"):
                marker = case.store / "end_of_day" / root / ".pending.json"
                marker.write_text("{}")
                try:
                    with (
                        self.subTest(root=root),
                        patch.object(preparation, "load_pricing", side_effect=AssertionError("bar read")),
                        self.assertRaisesRegex(LineageError, "pending|incomplete"),
                    ):
                        preparation.prepare(
                            case.store,
                            "AAA",
                            path,
                            market_clients=Mock(side_effect=AssertionError("provider request")),
                        )
                finally:
                    marker.unlink()
            preparation.prepare(case.store, "AAA", path)
            actual = preparation._current_sources

            def changed(*args, **kwargs):
                sources = actual(*args, **kwargs)
                return {**sources, "AAA": {**sources["AAA"], "new_source_generation": True}}

            with (
                patch.object(preparation, "_current_sources", side_effect=changed),
                patch.object(preparation, "load_pricing", side_effect=AssertionError("reread prices")),
                self.assertRaisesRegex(LineageError, "changed"),
            ):
                preparation.prepare(case.store, "AAA", path)

    def test_completed_market_observations_survive_interruption_and_actual_endpoint_observations_enter_artifacts(
        self,
    ):
        store = Path(self.enterContext(tempfile.TemporaryDirectory())) / "store"
        output = store.parent / "proposed.json"
        prices = FakeClient(traded_history("2024-02-12", "2024-02-24", 22))
        archive = {**asset("SHELVE"), "exchange": "OTC", "status": "inactive"}
        div = {
            "action_type": "cash_dividend",
            "symbol": "WOOD",
            "cusip": "111111111",
            "id": "native-div",
            "rate": ".30",
            "ex_date": "2024-02-20",
            "process_date": "2024-02-20",
        }
        for symbol in ("SHELVE", "WOOD"):
            download_pricing(store, symbol, "2024-03-01", client=prices)
        result = pipeline.run_lineage(
            store, ["WOOD"], identity_client=IdentityClient({"SHELVE": archive, "111111111": archive}, [div])
        )
        self.assertEqual(result["failed"], 0, result)
        # Archive comes first in compact key order. Its mapped sample and the
        # actual negative OTC response both survive failure on the literal route.
        fail = ("WOOD", "2024-02-20", "sip", "-")
        first = Market(failed=fail)
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            preparation.prepare(store, "WOOD", output, market_clients=lambda: (prices, first))
        self.assertEqual(len(first.calls), 3, first.calls)
        self.assertFalse(output.exists())
        second = Market()
        checkpoint = output.with_name("." + output.name + ".market.json")
        historical = checkpoint.read_bytes()
        tampered = json.loads(historical)
        tampered["objects"].append({"type": "unsolicited_test_observation"})
        checkpoint.write_text(json.dumps(tampered))
        with (
            patch.object(
                preparation, "load_pricing", side_effect=AssertionError("read price before checkpoint binding")
            ),
            self.assertRaisesRegex(LineageError, "different inputs"),
        ):
            preparation.prepare(store, "WOOD", output, market_clients=lambda: (prices, second))
        self.assertEqual(second.calls, [])
        checkpoint.write_bytes(historical)
        prices.calls.clear()
        answer = preparation.prepare(store, "WOOD", output, market_clients=lambda: (prices, second))
        self.assertFalse(answer["reused"])
        self.assertEqual(second.calls, [fail])
        self.assertEqual(
            [(c[0], c[1], c[2].date().isoformat()) for c in prices.calls],
            [("WOOD", "raw", "2024-02-12"), ("WOOD", "raw", "2024-02-23")],
        )
        packet = json.loads(output.read_bytes())
        receipts = packet["candidate_receipts"]
        mapped = receipts["SHELVE"]["evidence"]["mapped_historical_trade_tape"]
        self.assertEqual(
            (mapped["tape_network"], mapped["pricing_route_binding"]["observed_symbol_asset_id"]),
            ("Nasdaq UTP", archive["id"]),
        )
        otc = receipts["SHELVE"]["evidence"]["otc_historical_trade_tape"]
        self.assertEqual((otc["http_status"], otc["outcome"]), (403, "unavailable"))
        literal = receipts["WOOD"]["evidence"]["former_ticker_literal_bars"]["SHELVE"]
        self.assertEqual(literal["source_generations"], packet["pricing_generations"])
        self.assertTrue(all(o["outcome"] == "observed" for o in literal["observations"]))
        self.assertEqual(receipts["WOOD"]["evidence"]["historical_trade_tape"]["session"], "2024-02-20")
        with patch.object(preparation, "load_pricing", side_effect=AssertionError("repeated price read")):
            self.assertTrue(
                preparation.prepare(
                    store, "WOOD", output, market_clients=Mock(side_effect=AssertionError("repeated request"))
                )["reused"]
            )
