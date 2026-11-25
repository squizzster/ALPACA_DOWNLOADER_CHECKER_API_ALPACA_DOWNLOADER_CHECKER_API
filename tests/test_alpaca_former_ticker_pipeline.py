"""Fresh lineage discovery of an earlier occupant of a reused trading label."""

import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary, StandaloneReader
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient
from libs.alpaca_serving_catalogue.producer import maintain


def traded_history(start, end, initial):
    rows = informative_bars(start, end)
    for i, row in enumerate(rows):
        opening = Decimal(initial + i)
        row.update(
            o=opening,
            h=opening + 1,
            l=opening - 1,
            c=opening + Decimal(".50"),
            v=10000 + 20 * i,
            n=100 + i,
            vw=opening + Decimal(".25"),
        )
    return rows


class HistoricalIdentityClient(IdentityClient):
    def __init__(self, assets, actions):
        super().__init__(assets, actions)
        self.tape_calls = []
        self.http = AlpacaIdentityClient("test", "test")

    def get_asset_lookup(self, identifier):
        result = super().get_asset_lookup(identifier)
        if identifier == "REUSED" and result["http_status"] == 404:
            result["observed_at_utc"] = "2026-10-14T17:00:00Z"
        return result

    def get_trade_tape_sample(self, symbol, session, *, asof="-", feed="sip"):
        routes = {
            ("REUSED", "2024-02-09", "-", "sip"): "P",
            ("ARCHIVE", "2024-02-22", "2026-10-14", "sip"): "K",
            ("ARCHIVE", "2024-02-23", "-", "otc"): "D",
        }
        route = symbol, session, asof, feed
        if route in self.tape_calls or route not in routes:
            raise AssertionError(f"Unexpected trade sample {route}")
        self.tape_calls.append(route)
        self.request_count += 1
        native = {
            "symbol": symbol,
            "trades": [{"t": session + "T14:30:00Z", "z": "C" if feed == "sip" else "", "x": routes[route]}],
        }
        with patch.object(self.http, "request_json", return_value=native) as get:
            sample = self.http.get_trade_tape_sample(symbol, session, asof=asof, feed=feed)
        get.assert_called_once()
        return sample


class FormerTickerPipelineTests(TestCase):
    def setUp(self):
        old = {**asset("ARCHIVE", "first-issue"), "status": "inactive", "tradable": False, "exchange": "OTC"}
        current = asset("CURRENT", "second-issue")
        first_cusip, second_cusip = "111112222", "333334444"
        entry, exit_day, cutoff = "2026-09-02", "2026-09-25", "2026-10-14"
        first_tape = traded_history("2024-02-01", "2024-02-23", 10)
        current_tape = traded_history("2026-08-10", cutoff, 70)
        before_reuse = [row for row in current_tape if row["t"][:10] < entry]
        during_reuse = [row for row in current_tape if entry <= row["t"][:10] < exit_day]
        prices = {
            "ARCHIVE": FakeClient(first_tape),
            "REUSED": FakeClient(first_tape + during_reuse),
            "BASE": FakeClient(before_reuse),
            "CURRENT": FakeClient(current_tape),
        }

        class MarketData:
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                return prices[symbol].fetch(symbol, adjustment, start, end, asof=asof)

        dividend = {
            "action_type": "cash_dividend",
            "id": "dividend",
            "symbol": "REUSED",
            "cusip": first_cusip,
            "ex_date": "2024-02-09",
            "process_date": "2024-02-12",
            "rate": "0.15",
        }
        actions = [dividend] + [
            {
                "action_type": "name_change",
                "id": change,
                "old_symbol": prior,
                "new_symbol": successor,
                "old_cusip": second_cusip,
                "new_cusip": second_cusip,
                "process_date": change,
            }
            for change, prior, successor in ((entry, "BASE", "REUSED"), (exit_day, "REUSED", "CURRENT"))
        ]
        identity = HistoricalIdentityClient(
            {"ARCHIVE": old, "CURRENT": current, first_cusip: old, second_cusip: current}, actions
        )
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        store = Path(temporary) / "fresh"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2026, 10, 15, tzinfo=timezone.utc),
            )
        )
        provider = MarketData()
        self.first_id, self.second_id = (
            security_id_for_alpaca_asset_ids([item["id"]]) for item in (old, current)
        )
        self.first_cusip, self.second_cusip = first_cusip, second_cusip
        self.first_tape, self.current_tape = first_tape, current_tape
        self.prices, self.identity, self.store, self.provider = prices, identity, store, provider
        download_pricing(store, "CURRENT", cutoff, client=provider)

    def run_lineage(self):
        result = pipeline.run_lineage(
            self.store, ["CURRENT"], identity_client=self.identity, pricing_client=self.provider
        )
        self.assertEqual(result["failed"], 0, result)
        return result["results"][0]

    def test_fresh_group_discovers_former_label_and_reuses_published_ownership(self):
        result = self.run_lineage()
        self.assertCountEqual(result["processed"], self.prices)
        self.assertTrue(all(client.calls for client in self.prices.values()))
        self.assertEqual({day for client in self.prices.values() for day in client.asofs}, {"2026-10-14"})
        self.assertCountEqual(
            self.identity.tape_calls,
            [
                ("REUSED", "2024-02-09", "-", "sip"),
                ("ARCHIVE", "2024-02-22", "2026-10-14", "sip"),
                ("ARCHIVE", "2024-02-23", "-", "otc"),
            ],
        )
        standalone = StandaloneReader(self.store)
        with standalone.open_group("CURRENT") as group:
            self.assertEqual(set(group.summary["source_symbols"]), set(self.prices))
            self.assertEqual(
                {s["public_id"] for s in group.summary["securities"]}, {self.first_id, self.second_id}
            )
            evidence = group.evidence["sources"]
            self.assertEqual(
                [symbol for symbol, source in evidence.items() if "historical_trade_tape" in source["evidence"]],
                ["REUSED"],
            )
            reused = evidence["REUSED"]["evidence"]
            self.assertNotIn("former_ticker_literal_bars", reused)
            direct = next(item for item in reused["asset_lookups"] if item["lookup"] == "REUSED")
            self.assertEqual((direct["http_status"], direct["observed_at_utc"]), (404, "2026-10-14T17:00:00Z"))
            sample = reused["historical_trade_tape"]
            self.assertEqual(
                (sample["symbol"], sample["session"], sample["asof"]), ("REUSED", "2024-02-09", "-")
            )
            self.assertEqual((sample["feed"], sample["limit"], sample["tape_network"]), ("sip", 1, "Nasdaq UTP"))
            self.assertEqual(sample["trade"], {"t": "2024-02-09T14:30:00Z", "z": "C", "x": "P"})
            self.assertEqual(
                [
                    symbol
                    for symbol, source in evidence.items()
                    if "mapped_historical_trade_tape" in source["evidence"]
                ],
                ["ARCHIVE"],
            )
            mapped = evidence["ARCHIVE"]["evidence"]["mapped_historical_trade_tape"]
            self.assertEqual(
                (mapped["symbol"], mapped["returned_symbol"], mapped["session"], mapped["asof"]),
                ("ARCHIVE", "ARCHIVE", "2024-02-22", "2026-10-14"),
            )
            self.assertEqual(mapped["trade"], {"t": "2024-02-22T14:30:00Z", "z": "C", "x": "K"})
            otc = evidence["ARCHIVE"]["evidence"]["otc_historical_trade_tape"]
            self.assertEqual(
                (otc["symbol"], otc["session"], otc["asof"], otc["feed"], otc["after_sip_session"]),
                ("ARCHIVE", "2024-02-23", "-", "otc", "2024-02-22"),
            )
            self.assertEqual(otc["trade"], {"t": "2024-02-23T14:30:00Z", "z": "", "x": "D"})
            self.assertEqual(
                [
                    (lookup["asset"]["symbol"], lookup["asset"]["exchange"], lookup["asset"]["status"])
                    for lookup in reused["asset_lookups"]
                    if lookup["lookup_kind"] == "cusip" and lookup["lookup"] == self.first_cusip
                ],
                [("ARCHIVE", "OTC", "inactive")],
            )
            for route in ("ARCHIVE", "REUSED"):
                prefix = [
                    item
                    for item in evidence[route]["assignments"]
                    if item["to"] < "2026-09-02" and "raw" in item["adjustments"]
                ]
                self.assertEqual(
                    [
                        (
                            s["from"],
                            s["to"],
                            s["historical_symbol"],
                            s["canonical_security"]["cusip"],
                            s["security_identity"]["security_id"],
                        )
                        for s in prefix
                    ],
                    [("2024-02-01", "2024-02-22", "REUSED", self.first_cusip, self.first_id)],
                    route,
                )
            views = group.get_histories(public_id=self.first_id)["views"]
            self.assertEqual(set(views), set(ADJUSTMENTS))
            fields = {
                "open": "o",
                "high": "h",
                "low": "l",
                "close": "c",
                "volume": "v",
                "trade_count": "n",
                "vwap": "vw",
            }
            for mode, view in views.items():
                with self.subTest(mode=mode):
                    self.assertEqual(
                        [b["session"] for b in view["bars"]], [r["t"][:10] for r in self.first_tape]
                    )
                    self.assertTrue(all(b["tickers"] == ["REUSED"] for b in view["bars"]))
                    self.assertEqual(
                        [tuple(Decimal(b[k]) for k in fields) for b in view["bars"]],
                        [tuple(r[k] for k in fields.values()) for r in self.first_tape],
                    )

        catalogue = maintain(self.store, bootstrap=True)
        self.assertTrue(catalogue["complete"], catalogue)
        public = DataLibrary(store=self.store)
        self.assertEqual({m.public_id for m in public.resolve_ticker("REUSED")}, {self.first_id, self.second_id})
        self.assertEqual(
            [m.public_id for m in public.resolve_ticker("REUSED", on="2024-02-09")], [self.first_id]
        )
        self.assertEqual(
            [m.public_id for m in public.resolve_ticker("REUSED", on="2026-09-02")], [self.second_id]
        )
        self.assertEqual(public.resolve_ticker("REUSED", on="2025-02-03"), ())
        self.assertEqual(public.resolve_ticker("ARCHIVE", on="2024-02-09"), ())
        security = public.get_security(self.first_id)
        self.assertEqual(security.cusips, (self.first_cusip,))
        self.assertEqual(public.get_security(self.second_id).cusips, (self.second_cusip,))
        self.assertEqual((security.first_session, security.last_session), (date(2024, 2, 1), date(2024, 2, 22)))
        self.assertEqual(
            [b.close for b in public.get_history(self.second_id).bars], [r["c"] for r in self.current_tape]
        )

        original_open = Path.open

        def open_on_retry(path, *args, **kwargs):
            self.assertFalse(path.name.endswith((".gz", ".projection.json")), path)
            self.assertNotIn(
                path.name, {"completed_years.manifest.json", "year.manifest.json", "month.manifest.json"}
            )
            return original_open(path, *args, **kwargs)

        with (
            patch.object(self.identity, "get_asset_lookup", side_effect=AssertionError("repeated asset lookup")),
            patch.object(
                self.identity, "get_corporate_actions", side_effect=AssertionError("repeated action query")
            ),
            patch.object(
                self.identity,
                "get_trade_tape_sample",
                side_effect=AssertionError("repeated SIP trade sample"),
            ),
            patch.object(self.provider, "fetch", side_effect=AssertionError("repeated pricing request")),
            patch.object(Path, "open", open_on_retry),
            patch(
                "libs.alpaca_lineage_build.policy_engine.build_security_correlation_report",
                side_effect=AssertionError("repeated ownership decision"),
            ),
        ):
            retry = self.run_lineage()
        self.assertEqual(retry["status"], "unchanged")

    def test_ambiguous_current_mapping_retains_only_two_literal_sessions_in_the_group(self):
        self.identity.assets["REUSED"] = asset("REUSED", "third-issue")
        self.run_lineage()
        market = self.prices["REUSED"]
        literal = [call for call, asof in zip(market.calls, market.asofs, strict=True) if asof == "-"]
        self.assertEqual(
            [
                (symbol, mode, start.date().isoformat(), end.date().isoformat())
                for symbol, mode, start, end in literal
            ],
            [("REUSED", "raw", "2024-02-01", "2024-02-02"), ("REUSED", "raw", "2024-02-22", "2024-02-23")],
        )
        with StandaloneReader(self.store).open_group("CURRENT") as group:
            evidence = group.evidence["sources"]
            bundle = evidence["REUSED"]["evidence"]["former_ticker_literal_bars"]["ARCHIVE"]
            self.assertEqual(set(bundle["source_generations"]), {"REUSED", "ARCHIVE"})
            self.assertEqual(bundle["source_asofs"], {"REUSED": "2026-10-14", "ARCHIVE": "2026-10-14"})
            self.assertEqual(
                [(item["session"], item["outcome"], item["bar"]["t"]) for item in bundle["observations"]],
                [(r["t"][:10], "observed", r["t"]) for r in (self.first_tape[0], self.first_tape[-1])],
            )
        with patch.object(self.provider, "fetch", side_effect=AssertionError("repeated literal request")):
            self.assertEqual(self.run_lineage()["status"], "unchanged")

    def test_without_market_client_an_ambiguous_route_has_no_literal_bar_observations(self):
        self.identity.assets["REUSED"] = asset("REUSED", "third-issue")
        for symbol in ("ARCHIVE", "BASE", "REUSED"):
            download_pricing(self.store, symbol, "2026-10-14", client=self.provider)
        with patch(
            "libs.alpaca_lineage_build.group_engine.complete_literal_edges",
            side_effect=AssertionError("literal request without a pricing client"),
        ):
            result = pipeline.run_lineage(self.store, ["CURRENT"], identity_client=self.identity)
        self.assertEqual(result["failed"], 0, result)
        with StandaloneReader(self.store).open_group("CURRENT") as group:
            self.assertNotIn("former_ticker_literal_bars", group.evidence["sources"]["REUSED"]["evidence"])

    def test_missing_cusip_lookup_leaves_the_earlier_occupation_unpublished(self):
        del self.identity.assets[self.first_cusip]
        result = self.run_lineage()
        self.assertEqual(set(result["processed"]), {"BASE", "REUSED", "CURRENT"})
        self.assertFalse(self.prices["ARCHIVE"].calls)
        self.assertFalse(self.identity.tape_calls)
        root = self.store / "end_of_day/lineage"
        receipt = storage.read_receipt(root, "REUSED", storage.read_index(root, "REUSED"))
        self.assertEqual(receipt["unresolved_rows"], len(self.first_tape) * len(ADJUSTMENTS))
        self.assertFalse((self.store / "end_of_day/manifests/REUSED/update.json").exists())
        catalogue = maintain(self.store, bootstrap=True)
        self.assertTrue(catalogue["complete"], catalogue)
        public = DataLibrary(store=self.store)
        self.assertEqual(public.resolve_ticker("REUSED", on="2024-02-09"), ())
        self.assertEqual(
            [m.public_id for m in public.resolve_ticker("REUSED", on="2026-09-02")], [self.second_id]
        )
