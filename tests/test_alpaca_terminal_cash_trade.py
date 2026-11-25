"""A reported terminal SIP print is descriptive evidence for a cash acquiree."""

import tempfile
from copy import deepcopy
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from test_alpaca_former_ticker_pipeline import traded_history
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_otc_status_discovery import price_view
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary, StandaloneReader
from libs.alpaca_lineage_build import metadata, pipeline
from libs.alpaca_lineage_build import terminal_cash_trade as cash
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence
from libs.alpaca_serving_catalogue.producer import maintain


class NativeCash(IdentityClient):
    def __init__(self):
        self.symbol, self.cusip = "HEMLOCK", "777770007"
        self.own = {**asset(self.symbol, "target-equity"), "status": "inactive", "exchange": "FUTUREVENUE"}
        self.merger = {
            "id": "native-cash-terms",
            "action_type": "cash_merger",
            "acquiree_symbol": self.symbol,
            "acquiree_cusip": self.cusip,
            "rate": "38.75",
            "effective_date": "2024-02-22",
            "process_date": "2024-02-22",
        }
        super().__init__({self.symbol: self.own, self.cusip: self.own}, [self.merger])
        self.physical = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.trade = {
            "t": "2024-02-21T23:48:26.870022149Z",
            "z": "C",
            "x": "K",
            "s": 100,
            "p": 36.48,
            "c": ["@", "T"],
        }
        self.response = {"symbol": self.symbol, "trades": [self.trade], "next_page_token": "older"}
        self.urls = []

    def get_trade_tape_sample(self, *args, **kwargs):
        def get(url):
            self.urls.append(url)
            if len(self.urls) > 1:
                raise AssertionError("second physical trades request")
            if isinstance(self.response, Exception):
                raise self.response
            return self.response

        with patch.object(self.physical, "request_json", side_effect=get):
            return self.physical.get_trade_tape_sample(*args, **kwargs)


def row(day, volume, count):
    return [day + "T05:00:00Z", "36", "37", "34", "36", str(volume), str(count), "0"]


class HeldTail(list):
    def __init__(self, values, allowed):
        super().__init__(values)
        self.allowed = list(allowed)

    def __getitem__(self, key):
        if not self.allowed or self.allowed.pop(0) != key:
            raise AssertionError(f"unexpected historical row {key}")
        return super().__getitem__(key)

    def __iter__(self):
        raise AssertionError("scanned a materialized history")


class TerminalCashTradeTests(TestCase):
    def setUp(self):
        self.native = NativeCash()
        self.view = price_view(self.native.symbol)
        self.view.views = {"raw": HeldTail([row("2024-02-21", 400, 9), row("2024-02-22", 0, 0)], [-1, -2])}
        self.client = metadata.EvidenceClient(self.native)

    def acquire(self, before=None, **kwargs):
        return metadata.acquire(self.view, self.client, before, **kwargs)

    def test_fresh_pipeline_publishes_trade_terms_and_preserves_zero_activity_final_bar(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())) / "store"
        tape = traded_history("2024-02-01", "2024-02-23", 10)
        last = tape[-2]["c"]
        tape[-1].update(o=last, h=last, l=last, c=last, v=0, n=0, vw=0)
        prices = FakeClient(tape)
        symbol = self.native.symbol
        download_pricing(root, symbol, "2026-10-14", client=prices)

        def run():
            result = pipeline.run_lineage(root, [symbol], identity_client=self.native, pricing_client=prices)
            self.assertEqual(result["failed"], 0, result)
            return result["results"][0]

        run()
        with StandaloneReader(root).open_group(symbol) as group:
            sample = group.evidence["sources"][symbol]["evidence"][cash.FIELD]
            (security,) = group.summary["securities"]
        self.assertEqual(sample["observed_symbol_asset_id"], self.native.own["id"])
        self.assertEqual(
            sample["cash_merger"],
            {
                "action_id": "native-cash-terms",
                "role": "acquiree",
                "symbol": symbol,
                "cusip": self.native.cusip,
                "effective_date": "2024-02-22",
                "process_date": "2024-02-22",
                "rate": "38.75",
            },
        )
        self.assertTrue(sample["pricing_generation"])
        self.assertEqual(
            (
                sample["symbol"],
                sample["returned_symbol"],
                sample["asof"],
                sample["tape_network"],
                sample["observed_session"],
            ),
            (symbol, symbol, "2026-10-14", "Nasdaq UTP", "2024-02-21"),
        )
        self.assertEqual(sample["trade"], self.native.trade)
        self.assertEqual(
            parse_qs(urlsplit(self.native.urls[0]).query),
            {
                "asof": ["2026-10-14"],
                "feed": ["sip"],
                "sort": ["desc"],
                "limit": ["1"],
                "start": ["2024-02-21T05:00:00Z"],
                "end": ["2024-02-23T04:59:59.999999999Z"],
            },
        )
        self.assertTrue(maintain(root, bootstrap=True)["complete"])
        public = DataLibrary(store=root)
        self.assertEqual(public.get_security(security["public_id"]).last_session, date(2024, 2, 22))
        bars = public.get_history(security["public_id"]).bars
        self.assertEqual((bars[-1].close, bars[-1].volume, bars[-1].trade_count), (last, 0, 0))
        with (
            patch.object(prices, "fetch", side_effect=AssertionError("repeated price acquisition")),
            patch.object(self.native, "get_asset_lookup", side_effect=AssertionError("repeated asset lookup")),
            patch.object(self.native, "get_corporate_actions", side_effect=AssertionError("repeated terms")),
            patch.object(self.native, "get_trade_tape_sample", side_effect=AssertionError("repeated trade")),
            patch(
                "libs.alpaca_lineage_build.policy_engine.build_security_correlation_report",
                side_effect=AssertionError("repeated ownership"),
            ),
        ):
            self.assertEqual(run()["status"], "unchanged")

    def test_longer_halt_uses_only_held_zero_tail_and_one_physical_request(self):
        self.view.views["raw"] = HeldTail(
            [
                row("2024-02-01", 4, 1),
                row("2024-02-12", 200, 8),
                row("2024-02-20", 0, 0),
                row("2024-02-22", 0, 0),
            ],
            [-1, -2, -3],
        )
        sample = self.acquire()[cash.FIELD]
        self.assertEqual((sample["session"], sample["through_session"]), ("2024-02-12", "2024-02-22"))
        self.assertEqual(sample["observed_session"], "2024-02-21")
        self.assertEqual(len(self.native.urls), 1)

    def test_empty_and_denied_are_completed_observations_even_after_evidence_reconstruction(self):
        for response, expected in (
            ({"symbol": self.native.symbol, "trades": [], "next_page_token": None}, ("no_trades", 200)),
            (IdentityHttpError(403, "denied"), ("unavailable", 403)),
            (IdentityHttpError(404, "missing"), ("unavailable", 404)),
        ):
            with self.subTest(expected=expected):
                self.setUp()
                self.native.response = response
                result = self.acquire()
                sample = result[cash.FIELD]
                self.assertEqual((sample["outcome"], sample["http_status"]), expected)
                self.assertNotIn("trade", sample)
                self.client = metadata.EvidenceClient(self.native)
                with patch.object(cash, "_terms", side_effect=AssertionError("repeated native selector")):
                    before = {"evidence": result, "anchor_month": "2026-10"}
                    self.assertIs(self.acquire(before, observe_new_tapes=False)[cash.FIELD], sample)
                self.assertEqual(len(self.native.urls), 1)

    def test_absent_native_operands_and_unselected_inputs_make_no_trade_calls(self):
        for mode in (
            "positive_end",
            "all_zero",
            "mixed_activity",
            "older_mapping",
            "literal",
            "missing_event",
            "wrong_role",
            "stock_consideration",
            "zero_consideration",
            "wrong_day",
            "unresolved",
            "other_uuid",
            "otc",
            "active",
            "foreign_class",
            "partial_day",
            "partial_start",
            "unmaterialized",
            "unknown_generation",
            "accepted",
            "rival_merger",
        ):
            with self.subTest(mode=mode):
                self.setUp()
                native = self.native
                request = self.view.source["latest_request"]
                if mode in {"positive_end", "all_zero", "mixed_activity"}:
                    values = {
                        "positive_end": (400, 9, 10, 2),
                        "all_zero": (0, 0, 0, 0),
                        "mixed_activity": (10, 0, 0, 0),
                    }[mode]
                    self.view.views["raw"] = [row("2024-02-21", *values[:2]), row("2024-02-22", *values[2:])]
                elif mode in {"older_mapping", "literal"}:
                    request["asof"] = "2024-02-21" if mode == "older_mapping" else "-"
                elif mode == "missing_event":
                    native.actions.clear()
                elif mode == "wrong_role":
                    native.merger["acquirer_symbol"] = native.merger.pop("acquiree_symbol")
                elif mode == "stock_consideration":
                    native.merger["action_type"] = "stock_and_cash_merger"
                elif mode == "zero_consideration":
                    native.merger["rate"] = "0"
                elif mode == "wrong_day":
                    native.merger["effective_date"] = "2024-02-21"
                elif mode == "unresolved":
                    del native.assets[native.cusip]
                elif mode == "other_uuid":
                    native.assets[native.cusip] = asset("COUNTERPARTY")
                elif mode == "otc":
                    native.own["exchange"] = "OTC"
                    self.view.local["downloads"][0].pop("to")
                elif mode == "active":
                    native.own["status"] = "active"
                elif mode == "foreign_class":
                    native.own["class"] = "crypto"
                elif mode == "partial_day":
                    request["end_exclusive"] = "2024-02-22T19:00:00Z"
                elif mode == "partial_start":
                    self.view.source["history_start"] = "2024-02-21T19:00:00Z"
                elif mode == "unmaterialized":
                    self.view.materialization_scope = "retained_suffix"
                elif mode == "unknown_generation":
                    self.view.source_sha256 = None
                elif mode == "accepted":
                    self.view.accepted_report = {}
                elif mode == "rival_merger":
                    native.actions.append(
                        {**native.merger, "id": "different-endpoint", "acquiree_cusip": "333334444"}
                    )
                with patch.object(self.client, "get_trade_tape_sample", side_effect=AssertionError(mode)):
                    self.assertNotIn(cash.FIELD, self.acquire())

    def test_only_a_competing_occupant_in_the_window_suppresses_the_sample(self):
        for day, observed in (("2020-08-14", True), ("2024-02-21", False)):
            with self.subTest(day=day):
                self.setUp()
                rival = {
                    **self.native.merger,
                    "id": "another-ending",
                    "effective_date": day,
                    "process_date": "2024-02-10",
                    "acquiree_cusip": "333334444",
                }
                self.native.actions.append(rival)
                self.assertEqual(cash.FIELD in self.acquire(), observed)
                self.assertEqual(len(self.native.urls), int(observed))

    def test_original_scope_survives_transitive_unchanged_pricing_and_an_advancing_current_label(self):
        initial = self.acquire()
        sample = initial[cash.FIELD]
        self.view.source_sha256 = "third-generation"
        self.view.source["latest_request"].update(asof="2026-11-24", end_exclusive="2026-11-24T05:00:00Z")
        self.view.source["history_changes"] = [{"source_sha256": "second-generation", "changed_from": None}]
        with (
            patch.object(cash, "_terms", side_effect=AssertionError("reselected original native terms")),
            patch.object(self.client, "get_trade_tape_sample", side_effect=AssertionError("repeated trade")),
        ):
            result = cash.carry_terminal_cash_trade(self.view, initial, previous_generation="second-generation")
            self.assertIs(result, initial)
            self.assertIs(result[cash.FIELD], sample)
            self.assertEqual(sample["pricing_generation"], "selected-generation")
            self.assertEqual(sample["asof"], "2026-10-14")

    def test_prior_binding_retires_changed_pinned_owner_native_terms_or_source_without_reads(self):
        self.view.source["latest_request"]["asof"] = "2024-03-08"
        initial = ProviderEvidence(self.acquire())
        sample = initial[cash.FIELD]
        original = self.view.source_sha256
        self.view.views = Mock(side_effect=AssertionError("read historical prices"))
        for mode in (
            "same",
            "advance",
            "new_pinned",
            "unknown_generation",
            "correction",
            "new_owner",
            "new_cusip_owner",
            "withdrawn_event",
            "new_competing_event",
        ):
            with self.subTest(mode=mode):
                view = deepcopy(self.view)
                current = ProviderEvidence(initial)
                prior = {"evidence": initial, "source_sha256": original}
                event = self.native.merger
                window = SimpleNamespace(
                    consumed_symbols={view.symbol: {"asset": dict(self.native.own)}},
                    previous=prior,
                    client=SimpleNamespace(assets={}),
                    start="2026-10-01",
                    changed_cusips=set(),
                    completed_actions={("cash_merger", event["id"]): event},
                )
                if mode in ("advance", "unknown_generation", "correction"):
                    view.source_sha256 = "next"
                    changed = "2024-02-23" if mode == "advance" else "2024-02-22"
                    view.source["history_changes"] = (
                        []
                        if mode == "unknown_generation"
                        else [{"source_sha256": original, "changed_from": changed}]
                    )
                elif mode == "new_pinned":
                    view.source["latest_request"]["asof"] = "2024-03-09"
                elif mode == "new_owner":
                    window.consumed_symbols[view.symbol]["asset"]["id"] = "different-equity"
                elif mode == "new_cusip_owner":
                    window.client.assets[self.native.cusip] = {}
                    window.client.observed_uuid_lookup = lambda _: {"asset": {"id": "another-issue"}}
                elif mode == "withdrawn_event":
                    window.start = None
                    current["corporate_actions"] = []
                elif mode == "new_competing_event":
                    rival = {**event, "id": "other-event", "acquiree_cusip": "333334444"}
                    window.changed_cusips.add(rival["acquiree_cusip"])
                    current["corporate_actions"] = [event, rival]
                    current["input_symbol_occupants"] = [{"cusip": rival["acquiree_cusip"]}]
                with patch.object(cash, "_terms", side_effect=AssertionError("repeated acquiree selector")):
                    result = cash.carry_terminal_cash_trade(view, current, window)
                    cash.record_terminal_cash_trade(view, result, window)
                if mode in ("same", "advance"):
                    self.assertIs(result, current)
                    self.assertIs(result[cash.FIELD], sample)
                else:
                    self.assertNotIn(cash.FIELD, result)
                    self.assertIs(type(result), ProviderEvidence)
                    self.assertIs(current[cash.FIELD], sample)
        self.assertEqual(len(self.native.urls), 1)
