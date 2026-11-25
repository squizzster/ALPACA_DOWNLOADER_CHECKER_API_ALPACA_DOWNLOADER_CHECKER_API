"""Fresh OTC metadata can select one native action filter before admitting a ticker."""

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_former_ticker_pipeline import traded_history
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pipeline_evidence import transport
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import metadata, otc_status, pipeline
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.inference_evidence import provider_evidence


class StatusClient(IdentityClient):
    def __init__(self, symbol="WXYZQ"):
        self.archive = {**asset(symbol, "original"), "exchange": "OTC", "status": "inactive"}
        self.root, self.terminal = symbol[:-1], symbol
        self.dividend = {
            "action_type": "cash_dividend",
            "id": "native-distribution",
            "symbol": self.root,
            "rate": "0.35",
            "ex_date": "2024-02-09",
            "process_date": "2024-02-12",
            "cusip": "111112222",
        }
        super().__init__({symbol: self.archive, "111112222": self.archive}, [self.dividend])
        self.events = []
        self.denied = False
        self.tape = "A"

    def get_asset_lookup(self, identifier):
        self.events.append(("asset", identifier))
        result = super().get_asset_lookup(identifier)
        if result["http_status"] == 404:
            result["observed_at_utc"] = "2026-10-14T17:00:00Z"
        return result

    def get_corporate_actions(self, symbol, start, end):
        self.events.append(("actions", symbol))
        if self.denied and symbol == self.root:
            raise IdentityHttpError(403, "action filter forbidden")
        return super().get_corporate_actions(symbol, start, end)

    def get_trade_tape_sample(self, symbol, session, *, asof, feed):
        self.request_count += 1
        self.events.append((feed, symbol))
        outcome = {
            "symbol": symbol,
            "returned_symbol": symbol,
            "session": session,
            "asof": asof,
            "feed": feed,
            "limit": 1,
            "outcome": "no_trades",
            "http_status": 200,
        }
        if feed == "sip":
            outcome.update(outcome="observed", trade={"t": session + "T14:30:00Z", "z": self.tape, "x": "P"})
        else:
            outcome.update(outcome="unavailable", http_status=403, reason="otc_subscription_denied")
        return outcome


def price_view(symbol="WXYZQ"):
    return SimpleNamespace(
        symbol=symbol,
        source_sha256="selected-generation",
        materialization_scope="complete_history",
        local={
            "present": True,
            "counts_known": False,
            "downloads": [
                {"adjustment": "raw", "rows": 15, "from": "2024-02-01T05:00:00Z", "to": "2024-02-22T05:00:00Z"}
            ],
        },
        source={
            "history_start": "2016-01-01",
            "latest_request": {
                "asof": "2026-10-14",
                "end_exclusive": "2026-10-14T04:00:00Z",
                "feed": "sip",
                "timeframe": "1Day",
            },
        },
    )


class OtcStatusDiscoveryTests(TestCase):
    def pipeline_case(self, *, match):
        raw = StatusClient()
        if not match:
            raw.assets["111112222"] = asset("DECOY", "different")
        root = Path(self.enterContext(tempfile.TemporaryDirectory())) / "store"
        tape = traded_history("2024-02-01", "2024-02-23", 20)
        prices = FakeClient(tape)
        download_pricing(root, "WXYZQ", "2026-10-14", client=prices)
        outcome = pipeline.run_lineage(root, ["WXYZQ"], identity_client=raw, pricing_client=prices)
        self.assertEqual(outcome["failed"], 0, outcome)
        names = {"WXYZQ", "WXYZ"} if match else {"WXYZQ"}
        self.assertEqual(set(outcome["results"][0]["processed"]), names)
        self.assertEqual({call[0] for call in prices.calls}, names)
        self.assertEqual(raw.events.count(("actions", "WXYZ")), 1)
        self.assertEqual(raw.events.count(("asset", "111112222")), 1)
        with StandaloneReader(root).open_group("WXYZQ") as group:
            evidence = group.evidence["sources"]["WXYZQ"]["evidence"]
            query = evidence["otc_status_symbol_query"]
            self.assertEqual(
                (query["symbol"], query["filter_symbol"], query["mapped_sip_tape"]), ("WXYZQ", "WXYZ", "A")
            )
            self.assertEqual(query["outcome"], "confirmed" if match else "cusip_mismatch")
            self.assertEqual(query["dividend_lookups"][0]["action_id"], "native-distribution")
            self.assertEqual(query["dividend_lookups"][0]["asset_id"], "original" if match else "different")
            self.assertEqual(set(evidence["queried_symbols"]), names)
            if match:
                self.assertLess(raw.events.index(("asset", "111112222")), raw.events.index(("asset", "WXYZ")))
                earlier = group.evidence["sources"]["WXYZQ"]["assignments"]
                self.assertIn("WXYZ", {a["historical_symbol"] for a in earlier})
            else:
                self.assertNotIn(("asset", "WXYZ"), raw.events)
                self.assertNotIn(("asset", "DECOY"), raw.events)
        with (
            patch.object(prices, "fetch", side_effect=AssertionError("repeated prices")),
            patch.object(raw, "get_corporate_actions", side_effect=AssertionError("repeated action request")),
            patch.object(raw, "get_asset_lookup", side_effect=AssertionError("repeated lookup")),
            patch.object(raw, "get_trade_tape_sample", side_effect=AssertionError("repeated mapped sample")),
        ):
            repeat = pipeline.run_lineage(root, ["WXYZQ"], identity_client=raw, pricing_client=prices)
        self.assertEqual(repeat["results"][0]["status"], "unchanged", repeat)

    def test_archive_alone_discovers_a_confirmed_dividend_spelling_and_owns_only_its_ordinary_group(self):
        self.pipeline_case(match=True)

    def test_an_unrelated_dividend_never_registers_or_prices_its_filtered_symbol(self):
        self.pipeline_case(match=False)

    def test_unusable_filtered_response_is_evidence_without_a_dependency(self):
        for variant in ("denied", "empty", "wrong_symbol", "outside", "zero", "missing_cusip"):
            with self.subTest(variant=variant):
                raw = StatusClient()
                if variant == "denied":
                    raw.denied = True
                elif variant == "empty":
                    raw.actions.clear()
                elif variant == "missing_cusip":
                    del raw.assets["111112222"]
                elif variant == "wrong_symbol":
                    raw.dividend["symbol"] = "OTHER"
                elif variant == "outside":
                    raw.dividend["ex_date"] = "2022-02-09"
                else:
                    raw.dividend["rate"] = "0"
                report = metadata.acquire(price_view(), metadata.EvidenceClient(raw))
                result = report["otc_status_symbol_query"]
                self.assertEqual(report["queried_symbols"], ["WXYZQ"])
                self.assertFalse(metadata.dependencies(report, "WXYZQ"))
                self.assertNotIn(("asset", "WXYZ"), raw.events)
                self.assertEqual(
                    result["outcome"],
                    {
                        "denied": "unavailable",
                        "empty": "empty",
                        "wrong_symbol": "empty",
                        "missing_cusip": "cusip_unavailable",
                    }.get(variant, "no_matching_dividend"),
                )

    def test_absent_positive_tape_or_ineligible_symbol_cannot_select_a_filter(self):
        for symbol, z in (("WXYZQ", "Z"), ("TINYQX", "C"), ("ABCQ", "A"), ("XX1XQ", "B")):
            with self.subTest(symbol=symbol):
                raw = StatusClient(symbol)
                raw.tape = z
                report = metadata.acquire(price_view(symbol), metadata.EvidenceClient(raw))
                self.assertNotIn("otc_status_symbol_query", report)
                self.assertNotIn(("actions", raw.root), raw.events)

    def test_stronger_native_cusip_path_takes_precedence(self):
        for source in ("asset_snapshot", "dated_action_and_exact_cusip"):
            with self.subTest(source=source):
                raw = StatusClient()
                if source == "asset_snapshot":
                    raw.archive["cusip"] = "111112222"
                else:
                    raw.actions.append(
                        {
                            "action_type": "worthless_removal",
                            "id": "native-removal",
                            "symbol": "WXYZQ",
                            "cusip": "111112222",
                            "process_date": "2024-10-01",
                        }
                    )
                raw.get_cusip_actions = lambda *args: {"outcome": "denied", "http_status": 403, "actions": []}
                report = metadata.acquire(price_view(), metadata.EvidenceClient(raw))
                self.assertEqual(report["cusip_action_discovery"]["source"]["kind"], source)
                self.assertNotIn("otc_status_symbol_query", report)
                self.assertNotIn(("actions", "WXYZ"), raw.events)

    def test_completed_or_suffix_only_price_control_cannot_select_a_status_query(self):
        for field, value in (
            ("materialization_scope", "retained_suffix"),
            ("previous", {"accepted": True}),
            ("accepted_report", {"accepted": True}),
            ("retained_split", {"accepted": True}),
        ):
            with self.subTest(field=field):
                raw, view = StatusClient(), price_view()
                setattr(view, field, value)
                report = metadata.acquire(view, metadata.EvidenceClient(raw))
                self.assertNotIn("otc_status_symbol_query", report)
                self.assertNotIn(("actions", "WXYZ"), raw.events)

    def test_completed_action_window_and_exact_lookup_survive_peer_failure_and_transport(self):
        raw, view = StatusClient(), price_view()
        cache = metadata.EvidenceClient(raw)
        cache._remember(("WXYZ", "2016-01-01", "2025-01-01"), [raw.dividend])
        cache.get_asset_lookup("111112222")
        raw.events.clear()
        get_symbol = metadata.WindowClient.get_symbol_lookup

        def interrupt(window, symbol):
            if symbol == "WXYZ":
                raise RuntimeError("interrupted after the confirmed search")
            return get_symbol(window, symbol)

        with (
            patch.object(metadata.WindowClient, "get_symbol_lookup", interrupt),
            self.assertRaisesRegex(RuntimeError, "interrupted"),
        ):
            metadata.acquire(view, cache)
        self.assertEqual([c for c in raw.calls if c[0] == "WXYZ"], [("WXYZ", "2025-01-02", "2026-10-14")])
        self.assertNotIn(("asset", "111112222"), raw.events)
        restored, _ = transport(cache)
        restored.client = raw
        with patch.object(otc_status, "_observe", side_effect=AssertionError("accepted status query replayed")):
            report = metadata.acquire(view, restored)
        self.assertEqual(report["otc_status_symbol_query"]["outcome"], "confirmed")
        self.assertEqual(
            report["otc_status_symbol_query"]["action_ranges"],
            [
                {
                    "start": "2016-01-01",
                    "end": "2025-01-01",
                    "http_status": 200,
                    "source": "completed_symbol_actions",
                }
            ],
        )
        self.assertEqual(raw.events.count(("actions", "WXYZ")), 1)
        self.assertNotIn(("asset", "111112222"), raw.events)

    def test_closed_scope_or_changed_issuer_cannot_authorize_a_new_spelling_query(self):
        raw, view = StatusClient(), price_view()
        observed = metadata.acquire(view, metadata.EvidenceClient(raw))
        old = {"evidence": observed, "anchor_month": "2026-10"}
        with patch.object(otc_status, "_observe", side_effect=AssertionError("reopened old source")):
            same = metadata.acquire(view, metadata.EvidenceClient(raw), old)
            self.assertEqual(same["otc_status_symbol_query"], observed["otc_status_symbol_query"])
            frozen = metadata.acquire(
                view, metadata.EvidenceClient(raw), {**old, "versions": {"correlation": 103}}
            )
            self.assertNotIn("otc_status_symbol_query", frozen)
            self.assertFalse(
                metadata.acquire(
                    view,
                    metadata.EvidenceClient(raw),
                    {
                        "anchor_month": "2026-10",
                        "evidence": {
                            **observed,
                            "otc_status_symbol_query": {
                                **observed["otc_status_symbol_query"],
                                "filter_symbol": "INJECTED",
                            },
                        },
                    },
                ).get("otc_status_symbol_query")
            )
            request = view.source["latest_request"]
            request["asof"] = "2021-02-01"
            pinned = metadata.acquire(view, metadata.EvidenceClient(raw), old, observe_new_tapes=False)
            self.assertNotIn("otc_status_symbol_query", pinned)
            request["asof"] = "2026-10-14"
            view.source_sha256 = "different-episode"
            view.local["downloads"][0]["from"] = "2024-02-15T05:00:00Z"
            cropped = metadata.acquire(view, metadata.EvidenceClient(raw), old, observe_new_tapes=False)
            self.assertNotIn("otc_status_symbol_query", cropped)
            view.source_sha256 = "selected-generation"
            view.local["downloads"][0]["from"] = "2024-02-01T05:00:00Z"
            replaced = metadata.EvidenceClient(raw)
            replaced.assets["111112222"] = {
                "outcome": "found",
                "http_status": 200,
                "asset": asset("OTHER", "wrong"),
            }
            self.assertNotIn("otc_status_symbol_query", metadata.acquire(view, replaced, old))
            raw.assets["WXYZQ"] = {**raw.archive, "id": "new-security"}
            changed = metadata.acquire(view, metadata.EvidenceClient(raw), old)
        self.assertNotIn("otc_status_symbol_query", changed)
        legacy = {
            **observed,
            "provider_corporate_actions": observed["corporate_actions"],
            "otc_status_symbol_query": {**observed["otc_status_symbol_query"], "filter_symbol": "INJECTED"},
        }
        genuine = provider_evidence(legacy, local_report=view.local)
        self.assertNotIn("otc_status_symbol_query", genuine)

    def test_a_dividend_in_a_gap_between_materialized_sessions_does_not_select_an_exact_lookup(self):
        raw, view = StatusClient(), price_view()
        view.views = {"raw": [("2024-02-01T05:00:00Z",), ("2024-02-22T05:00:00Z",)]}
        report = metadata.acquire(view, metadata.EvidenceClient(raw))
        self.assertEqual(report["otc_status_symbol_query"]["outcome"], "no_matching_dividend")
        self.assertNotIn(("asset", "111112222"), raw.events)

    def test_fragmented_coverage_selects_at_most_one_unobserved_range_before_confirmation(self):
        raw, view = StatusClient(), price_view()
        raw.actions.clear()
        cache = metadata.EvidenceClient(raw)
        for a, b in (("2019-01-01", "2024-02-06"), ("2024-02-13", "2024-02-15")):
            cache._remember(("WXYZ", a, b), [])
        report = metadata.acquire(view, cache)
        self.assertEqual(report["otc_status_symbol_query"]["outcome"], "partial_action_coverage")
        self.assertEqual(
            [call for call in raw.calls if call[0] == "WXYZ"], [("WXYZ", "2024-02-07", "2024-02-12")]
        )
        self.assertEqual(report["queried_symbols"], ["WXYZQ"])
