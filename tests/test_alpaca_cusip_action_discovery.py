import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from libs.alpaca_data_api_solver import ACTION_URL, build_plan
from libs.alpaca_lineage_build.metadata import EvidenceClient, WindowClient, acquire_unpriced, dependencies
from libs.alpaca_lineage_build.resume import evidence_delta
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.evidence import export_evidence, import_evidence
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.alpaca_security_lineage.lineage import build_identity_report

SYMBOL = "SABLE"
FORMER = "JUNIPER"
CUSIP = "319ZZ8124"
OTHER = "411SS1017"
START, END = "2024-01-01", "2024-02-29"
ASSET = {
    "symbol": SYMBOL,
    "id": "sample-archive-uuid",
    "cusip": CUSIP,
    "class": "us_equity",
    "name": "Sable Equity",
    "exchange": "OTC",
    "status": "active",
    "tradable": False,
}
BINDING = {"symbol": SYMBOL, "asset_id": ASSET["id"], "cusip": CUSIP}
DIVIDEND = {
    "action_type": "cash_dividend",
    "id": "dividend-12",
    "process_date": "2024-01-16",
    "symbol": FORMER,
    "cusip": CUSIP,
    "rate": 0.25,
    "ex_date": "2024-01-16",
}
REQUEST = {"start": START, "end_exclusive": "2024-03-01", "asof": END}


class Provider:
    request_count = 0

    def __init__(self, *, asset=ASSET, issued=(), named=(), denied=False):
        self.asset = asset
        self.named = list(named)
        self.issued = list(issued)
        self.denied = denied
        self.filtered = []
        self.symbol_calls = []
        self.lookup_calls = []

    def get_cusip_actions(self, cusip, start, end):
        if cusip != self.asset["cusip"] or (cusip, start, end) in self.filtered:
            raise AssertionError("unsolicited or repeated CUSIP request")
        self.filtered.append((cusip, start, end))
        if self.denied:
            return {"outcome": "denied", "http_status": 403, "actions": []}
        return {"outcome": "complete", "http_status": 200, "actions": self.issued}

    def get_corporate_actions(self, symbol, start, end):
        self.symbol_calls.append(symbol)
        if symbol == SYMBOL:
            return self.named
        if symbol == FORMER:
            return [DIVIDEND]
        raise AssertionError(f"undiscovered ticker requested: {symbol}")

    def get_asset_lookup(self, identifier):
        self.lookup_calls.append(identifier)
        if identifier in {SYMBOL, CUSIP}:
            return {"asset": self.asset, "http_status": 200, "outcome": "found"}
        if identifier == FORMER:
            return {"asset": None, "http_status": 404, "outcome": "not_found"}
        raise AssertionError(f"unsolicited exact mapping: {identifier}")


def restore(evidence, provider):
    encoder = Encoder()
    wire = export_evidence(evidence, encoder.encode)
    state, objects = json.loads(json.dumps([wire, encoder.objects]))
    return import_evidence(state, provider, Decoder(objects).decode)


class CusipActionDiscoveryTests(unittest.TestCase):
    def test_unlicensed_terminal_uses_own_native_removal_and_exact_uuid_to_discover_merger_role(self):
        terminal = {k: value for k, value in ASSET.items() if k != "cusip"}
        removal = {
            "id": "retirement",
            "process_date": "2024-02-09",
            "action_type": "worthless_removal",
            "symbol": SYMBOL,
            "cusip": CUSIP,
        }
        merger = {
            "id": "consideration",
            "action_type": "stock_and_cash_merger",
            "effective_date": "2024-01-22",
            "process_date": "2024-01-22",
            "acquirer_symbol": FORMER,
            "acquirer_cusip": CUSIP,
            "acquirer_rate": 0.1,
            "acquiree_symbol": "HEMLOCK",
            "acquiree_cusip": OTHER,
            "acquiree_rate": 1,
            "cash_rate": 1.2,
        }

        class NativeProvider(Provider):
            def get_corporate_actions(self, symbol, start, end):
                if symbol == FORMER:
                    self.symbol_calls.append(symbol)
                    return [merger]
                return super().get_corporate_actions(symbol, start, end)

            def get_asset_lookup(self, identifier):
                if identifier == OTHER:
                    self.lookup_calls.append(identifier)
                    return {
                        "outcome": "found",
                        "http_status": 200,
                        "asset": {**terminal, "id": "separate-target-uuid", "symbol": "HEMLOCK"},
                    }
                return super().get_asset_lookup(identifier)

        provider = NativeProvider(asset=terminal, named=[removal])
        with patch.object(
            provider,
            "get_cusip_actions",
            return_value={
                "outcome": "complete",
                "http_status": 200,
                "actions": [merger, removal],
            },
        ) as filtered:
            client = EvidenceClient(provider)
            report = acquire_unpriced(SYMBOL, REQUEST, client)
            filtered.assert_called_once_with(CUSIP, START, END)
        self.assertEqual(provider.lookup_calls.count(CUSIP), 1)
        self.assertEqual(provider.symbol_calls, [SYMBOL, FORMER])
        evidence = report["cusip_action_discovery"]
        self.assertEqual(evidence["source"]["kind"], "dated_action_and_exact_cusip")
        self.assertEqual(evidence["source"]["exact_lookup"], CUSIP)
        self.assertEqual(
            evidence["source"]["matches"],
            [
                {
                    "action_id": "retirement",
                    "action_type": "worthless_removal",
                    "process_date": "2024-02-09",
                    "symbol": SYMBOL,
                    "cusip_field": "cusip",
                    "symbol_field": "symbol",
                }
            ],
        )
        (window,) = evidence["windows"]
        self.assertEqual(
            [(m["symbol"], m["cusip_field"]) for m in window["matches"]],
            [(FORMER, "acquirer_cusip"), (SYMBOL, "cusip")],
        )
        self.assertEqual(
            next(x["asset"]["id"] for x in report["asset_lookups"] if x["lookup"] == OTHER),
            "separate-target-uuid",
        )
        self.assertEqual(
            {m["symbol"] for m in report["symbol_occupancy_evidence"] if m["asset_id"] == terminal["id"]},
            {SYMBOL, FORMER},
        )
        self.assertIn(merger, report["corporate_actions"])
        with (
            patch.object(
                provider, "get_corporate_actions", side_effect=AssertionError("repeated native actions")
            ),
            patch.object(provider, "get_cusip_actions", side_effect=AssertionError("repeated native CUSIP")),
            patch.object(provider, "get_asset_lookup", side_effect=AssertionError("repeated exact lookup")),
        ):
            retry = acquire_unpriced(SYMBOL, REQUEST, restore(client, provider), previous=report)
        self.assertEqual(retry["cusip_action_discovery"], evidence)

    def test_competing_own_cusips_require_same_uuid_and_a_unique_most_recent_scope(self):
        from libs.alpaca_security_lineage.cusip_discovery import discover

        terminal = {k: value for k, value in ASSET.items() if k != "cusip"}

        def lookup(label, kind, asset):
            return {
                "lookup_kind": kind,
                "lookup": label,
                "outcome": "found" if asset else "not_found",
                "http_status": 200 if asset else 404,
                "asset": asset,
            }

        own = lookup(SYMBOL, "symbol", terminal)
        current = lookup(CUSIP, "cusip", terminal)
        other = lookup(OTHER, "cusip", terminal)
        actions = [
            {**DIVIDEND, "symbol": SYMBOL},
            {**DIVIDEND, "cusip": OTHER, "id": "other", "symbol": SYMBOL},
        ]
        calls = []

        def filtered(cusip, start, end):
            calls.append(cusip)
            return {"outcome": "complete", "http_status": 200, "actions": []}

        from types import SimpleNamespace

        client = SimpleNamespace(get_cusip_actions=filtered)
        lookups = {("symbol", SYMBOL): own, ("cusip", CUSIP): current, ("cusip", OTHER): other}
        _, tie = discover(SYMBOL, lookups, actions, START, END, client)
        self.assertEqual(tie["source"]["kind"], "ambiguous_dated_actions")
        self.assertEqual({x["cusip"] for x in tie["source"]["candidates"]}, {CUSIP, OTHER})
        self.assertFalse(calls)
        actions[-1] = {**actions[-1], "process_date": "2024-02-01"}
        _, latest = discover(SYMBOL, lookups, actions, START, END, client)
        self.assertEqual((calls, latest["cusip"]), ([OTHER], OTHER))
        for exact in (
            lookup(OTHER, "cusip", None),
            lookup(OTHER, "cusip", {**terminal, "id": "different-uuid"}),
        ):
            lookups["cusip", OTHER] = exact
            _, selected = discover(SYMBOL, lookups, actions, START, END, client)
            self.assertEqual((calls[-1], selected["cusip"]), (CUSIP, CUSIP))
        lookups["cusip", OTHER] = other
        actions = [
            {
                "action_type": "reverse_split",
                "id": "units",
                "process_date": "2024-02-08",
                "symbol": SYMBOL,
                "old_cusip": CUSIP,
                "new_cusip": OTHER,
            }
        ]
        _, transition = discover(SYMBOL, lookups, actions, START, END, client)
        self.assertEqual((calls[-1], transition["cusip"]), (OTHER, OTHER))
        calls.clear()
        wrong_role = {
            "action_type": "cash_merger",
            "id": "separate-parties",
            "process_date": "2024-02-08",
            "acquiree_symbol": SYMBOL,
            "acquiree_cusip": OTHER,
            "acquirer_symbol": "HEMLOCK",
            "acquirer_cusip": CUSIP,
        }
        lookups["cusip", OTHER] = lookup(OTHER, "cusip", None)
        self.assertEqual(discover(SYMBOL, lookups, [wrong_role], START, END, client), (set(), None))
        self.assertEqual(calls, [])

    def test_append_uses_owned_actions_and_mappings_without_copying_discovery_requests(self):
        before = acquire_unpriced(SYMBOL, REQUEST, EvidenceClient(Provider(issued=[DIVIDEND])))

        class CompletedWindow(dict):
            def __deepcopy__(self, memo):
                raise AssertionError("materialized an accepted discovery window for assignment comparison")

        changed_query = CompletedWindow(
            {
                **before["cusip_action_discovery"],
                "windows": [
                    *before["cusip_action_discovery"]["windows"],
                    {
                        "start": "2024-03-01",
                        "end": "2024-03-01",
                        "outcome": "complete",
                        "matches": [],
                        "actions": [],
                    },
                ],
            }
        )
        after = {
            **before,
            "cusip_action_discovery": changed_query,
            "query": {**before["query"], "corporate_actions_end": "2024-03-01"},
            "archive_cusip_action_endpoints": list(reversed(before["archive_cusip_action_endpoints"])),
            "otc_status_symbol_query": CompletedWindow(outcome="empty"),
            "otc_historical_trade_tape": CompletedWindow(outcome="unavailable"),
            "terminal_cash_merger_trade": CompletedWindow(outcome="observed"),
        }
        self.assertEqual(evidence_delta(before, after, append=True)["scope"], "none")
        snapshot = {
            **after,
            "asset_lookups": [
                {**lookup, "asset": {k: v for k, v in lookup["asset"].items() if k != "cusip"}}
                if lookup.get("asset")
                else lookup
                for lookup in after["asset_lookups"]
            ],
        }
        self.assertEqual(evidence_delta(before, snapshot, append=True)["scope"], "none")
        actual = {
            **after,
            "corporate_actions": [
                *before["corporate_actions"],
                {**DIVIDEND, "id": "later", "process_date": "2024-03-01"},
            ],
        }
        from libs.alpaca_lineage_build import resume

        with patch.object(resume, "_evidence_facts", wraps=resume._evidence_facts) as prepare:
            self.assertEqual(evidence_delta(before, actual, append=True)["scope"], "complete_group")
        self.assertEqual(prepare.call_count, 2)
        self.assertIs(prepare.call_args_list[0].args[0], before)
        self.assertIs(prepare.call_args_list[1].args[0], actual)
        changed_uuid = [
            {**row, "asset": {**row["asset"], "id": "different-uuid"}} if row["lookup_kind"] == "cusip" else row
            for row in after["asset_lookups"]
        ]
        self.assertEqual(
            evidence_delta(before, {**after, "asset_lookups": changed_uuid}, append=True)["scope"],
            "complete_group",
        )

    def test_earlier_ordinary_observation_suppresses_reverse_lookup_after_handoff(self):
        class UnechoedCusip(Provider):
            def get_asset_lookup(self, identifier):
                result = super().get_asset_lookup(identifier)
                if identifier == CUSIP:
                    result["asset"] = {k: v for k, v in result["asset"].items() if k != "cusip"}
                return result

        provider = UnechoedCusip()
        original = EvidenceClient(provider)
        former = acquire_unpriced(FORMER, REQUEST, original)
        self.assertEqual(dependencies(former, FORMER), [SYMBOL])
        self.assertFalse(provider.filtered)
        for restored in (restore(original, provider), EvidenceClient(provider)):
            restored.provider_view(former)
            with patch.object(provider, "get_cusip_actions", side_effect=AssertionError("already named")):
                archive = acquire_unpriced(SYMBOL, REQUEST, restored)
            self.assertEqual(set(archive["queried_symbols"]), {FORMER, SYMBOL})
            self.assertEqual(archive["corporate_actions"], [DIVIDEND])

    def test_only_clean_provider_reports_lend_new_filtered_observations(self):
        client = EvidenceClient(Provider(issued=[DIVIDEND]))
        report = acquire_unpriced(SYMBOL, REQUEST, client)
        self.assertIs(provider_evidence(report), report)
        self.assertIn("cusip_action_discovery", provider_evidence(dict(report)))
        research = {**report, "provider_corporate_actions": [], "event_supplements": [{"legacy": True}]}
        projected = provider_evidence(research)
        self.assertNotIn("cusip_action_discovery", projected)
        self.assertNotIn("archive_cusip_action_endpoints", projected)
        self.assertTrue(all("cusip" not in (item.get("asset") or {}) for item in projected["asset_lookups"]))
        self.assertEqual(projected["corporate_actions"], [])
        other = EvidenceClient(Provider())
        other.provider_view(research)
        self.assertEqual(other.cusip_action_index["by_symbol"], {})
        self.assertEqual(other.cusip_action_windows, {})

    def test_fresh_archive_seed_publishes_ordinary_dividend_returned_only_by_cusip(self):
        self._publish_filtered_dividend(licensed=True)

    def test_fresh_archive_without_licensed_asset_fields_publishes_from_native_action_source(self):
        self._publish_filtered_dividend(licensed=False)

    def _publish_filtered_dividend(self, *, licensed):
        import test_alpaca_former_ticker_pipeline as fixture

        from alpaca_end_of_day import DataLibrary, StandaloneReader
        from libs.alpaca_lineage_build import pipeline
        from libs.alpaca_pricing_download.download import download_pricing
        from libs.alpaca_serving_catalogue.producer import maintain

        with patch.object(
            fixture,
            "download_pricing",
            side_effect=lambda store, _, end, **kw: download_pricing(store, "ARCHIVE", end, **kw),
        ):
            fixture.FormerTickerPipelineTests.setUp(self)
        only_filtered = self.identity.actions.pop(0)
        self.assertEqual(only_filtered["symbol"], "REUSED")
        filtered_actions = [only_filtered]
        if licensed:
            self.identity.assets["ARCHIVE"]["cusip"] = self.first_cusip
        else:
            removal = {
                "action_type": "worthless_removal",
                "id": "closed-issue",
                "process_date": "2024-06-21",
                "symbol": "ARCHIVE",
                "cusip": self.first_cusip,
            }
            self.identity.actions.append(removal)
            filtered_actions.append(removal)
        with patch.object(
            self.identity,
            "get_cusip_actions",
            create=True,
            return_value={
                "outcome": "complete",
                "http_status": 200,
                "actions": filtered_actions,
            },
        ) as filtered:
            result = pipeline.run_lineage(
                self.store, ["ARCHIVE"], identity_client=self.identity, pricing_client=self.provider
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertCountEqual(result["results"][0]["processed"], self.prices)
        self.assertEqual(filtered.call_count, 1)
        self.assertEqual(filtered.call_args.args[0], self.first_cusip)
        with StandaloneReader(self.store).open_group("ARCHIVE") as group:
            evidence = group.evidence["sources"]
            archive = evidence["ARCHIVE"]["evidence"]["cusip_action_discovery"]
            self.assertEqual(
                archive["source"]["kind"], "asset_snapshot" if licensed else "dated_action_and_exact_cusip"
            )
            self.assertEqual(
                archive["windows"][0]["actions"],
                [{"action": action, "ticker": action["symbol"]} for action in filtered_actions],
            )
            former = evidence["REUSED"]["evidence"]
            self.assertIn(only_filtered, former["corporate_actions"])
            self.assertEqual(former["historical_trade_tape"]["session"], only_filtered["ex_date"])
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        public = DataLibrary(store=self.store)
        self.assertEqual(
            [m.public_id for m in public.resolve_ticker("REUSED", on="2024-02-09")], [self.first_id]
        )
        self.assertEqual(
            [m.public_id for m in public.resolve_ticker("REUSED", on="2026-09-02")], [self.second_id]
        )
        self.assertEqual(
            [b.close for b in public.get_history(self.first_id).bars], [r["c"] for r in self.first_tape]
        )
        with (
            patch.object(self.identity, "get_asset_lookup", side_effect=AssertionError("repeated asset")),
            patch.object(
                self.identity, "get_corporate_actions", side_effect=AssertionError("repeated native actions")
            ),
            patch.object(
                self.identity,
                "get_cusip_actions",
                create=True,
                side_effect=AssertionError("repeated CUSIP actions"),
            ),
            patch.object(
                self.identity, "get_trade_tape_sample", side_effect=AssertionError("repeated tape sample")
            ),
            patch.object(self.provider, "fetch", side_effect=AssertionError("repeated prices")),
            patch(
                "libs.alpaca_lineage_build.policy_engine.build_security_correlation_report",
                side_effect=AssertionError("repeated ownership"),
            ),
        ):
            retry = pipeline.run_lineage(
                self.store, ["ARCHIVE"], identity_client=self.identity, pricing_client=self.provider
            )
        self.assertEqual(retry["failed"], 0, retry)

    def test_matching_native_legs_enter_normal_traversal_and_fresh_catalogue_replaces_lookup(self):
        merger = {
            "id": "both-sides",
            "action_type": "cash_merger",
            "process_date": "2024-01-24",
            "acquiree_cusip": CUSIP,
            "acquiree_symbol": FORMER,
            "acquirer_cusip": OTHER,
            "acquirer_symbol": "RIVAL",
        }
        foreign = {**DIVIDEND, "id": "foreign-dividend", "symbol": "OUTSIDER", "cusip": OTHER}
        unknown = {**DIVIDEND, "id": "new-event", "action_type": "new_event", "symbol": "STRANGER"}
        fake = {**DIVIDEND, "id": "", "symbol": "GHOST"}
        movement = {
            "id": "reorganization",
            "action_type": "reorganization",
            "process_date": "2024-01-17",
            "symbol": "PARENT",
            "cusip": OTHER,
            "stock_movements": [{"symbol": FORMER, "cusip": CUSIP}, {"symbol": "SIBLING", "cusip": OTHER}],
        }
        provider = Provider(issued=[DIVIDEND, foreign, merger, unknown, fake, movement])
        client = EvidenceClient(provider, observation={"records": [ASSET], "catalogue_observed_at_utc": "now"})
        report = acquire_unpriced(SYMBOL, REQUEST, client)
        self.assertEqual(provider.filtered, [(CUSIP, START, END)])
        self.assertEqual(provider.symbol_calls, [SYMBOL, FORMER])
        self.assertEqual(provider.lookup_calls, [FORMER, CUSIP])
        self.assertEqual(dependencies(report, SYMBOL), [FORMER])
        self.assertEqual(report["queried_symbols"], [SYMBOL, FORMER])
        fact = report["cusip_action_discovery"]
        self.assertEqual({k: fact[k] for k in BINDING}, BINDING)
        self.assertEqual(fact["source"], {"kind": "asset_snapshot"})
        (window,) = fact["windows"]
        self.assertEqual(window["unmatched_action_count"], 3)
        self.assertEqual(
            [(m["symbol"], m["symbol_field"], m["action_id"]) for m in window["matches"]],
            [
                (FORMER, "symbol", DIVIDEND["id"]),
                (FORMER, "acquiree_symbol", "both-sides"),
                (FORMER, "stock_movements[0].symbol", "reorganization"),
            ],
        )
        self.assertEqual(report["corporate_actions"], [DIVIDEND])
        self.assertNotIn("cusip", report["identities"][0])
        self.assertEqual(
            next(x["asset"] for x in report["asset_lookups"] if x["lookup"] == SYMBOL)["cusip"], CUSIP
        )
        with (
            patch.object(provider, "get_asset_lookup", side_effect=AssertionError("repeated exact lookup")),
            patch.object(
                provider, "get_corporate_actions", side_effect=AssertionError("repeated symbol actions")
            ),
            patch.object(provider, "get_cusip_actions", side_effect=AssertionError("repeated filtered actions")),
        ):
            repeated = acquire_unpriced(SYMBOL, REQUEST, client, previous=report)
        self.assertEqual(repeated["cusip_action_discovery"], fact)
        self.assertEqual(repeated["queried_symbols"], report["queried_symbols"])

    def test_no_supplementary_call_without_native_eligibility_or_after_ordinary_discovery(self):
        for asset in (
            {k: v for k, v in ASSET.items() if k != "cusip"},
            {**ASSET, "cusip": "not-a-cusip"},
            {**ASSET, "class": "crypto"},
            {**ASSET, "exchange": "NASDAQ", "status": "active"},
        ):
            provider = Provider(asset=asset)
            with (
                self.subTest(asset=asset),
                patch.object(provider, "get_cusip_actions", side_effect=AssertionError),
            ):
                report = acquire_unpriced(SYMBOL, REQUEST, EvidenceClient(provider))
                self.assertNotIn("cusip_action_discovery", report)
                self.assertEqual(report["queried_symbols"], [SYMBOL])
        provider = Provider(named=[DIVIDEND])
        with patch.object(provider, "get_cusip_actions", side_effect=AssertionError("ordinary actions suffice")):
            report = acquire_unpriced(SYMBOL, REQUEST, EvidenceClient(provider))
        self.assertEqual(set(report["queried_symbols"]), {SYMBOL, FORMER})
        self.assertEqual(report["cusip_action_discovery"]["previously_named_tickers"], [FORMER])
        self.assertEqual(report["cusip_action_discovery"]["windows"], [])

    def test_empty_and_denied_intervals_remain_distinct_and_only_missing_dates_are_requested(self):
        for denied in (False, True):
            provider = Provider(denied=denied, asset={**ASSET, "status": "inactive", "exchange": "NYSE"})
            client = EvidenceClient(provider)
            report = acquire_unpriced(SYMBOL, REQUEST, client)
            (window,) = report["cusip_action_discovery"]["windows"]
            self.assertEqual(window["outcome"], "denied" if denied else "complete")
            self.assertEqual(window["matches"], [])
            self.assertEqual(dependencies(report, SYMBOL), [])
            self.assertEqual(report["identities"][0]["lookup_cusips"], [])
            new = restore(client, provider)
            with patch(
                "libs.alpaca_lineage_build.metadata.accept_window",
                side_effect=AssertionError("reaccepted interval"),
            ):
                self.assertEqual(new.cusip_discovery_windows(BINDING, "2024-01-19", END), [window])
            expanded = new.cusip_discovery_windows(BINDING, "2023-12-30", "2024-03-03")
            self.assertEqual(len(expanded), 3)
            self.assertEqual(
                provider.filtered,
                [
                    (CUSIP, START, END),
                    (CUSIP, "2023-12-30", "2023-12-31"),
                    (CUSIP, "2024-03-01", "2024-03-03"),
                ],
            )
            # Without a live operation cache the selected receipt retains the same boundaries.
            previous = {"evidence": report, "anchor_month": "2024-02"}
            wrapper = WindowClient(EvidenceClient(provider), SYMBOL, previous, False)
            self.assertEqual(
                wrapper.cusip_discovery_windows(BINDING, START, END, covered_by_symbol=False), [window]
            )

    def test_legacy_scope_does_not_backfill_and_changed_cusip_has_its_own_binding(self):
        provider = Provider()
        previous = {
            "evidence": {
                "query": {"corporate_actions_start": START, "corporate_actions_end": END},
                "asset_lookups": [],
                "corporate_actions": [],
            },
            "anchor_month": "2024-02",
        }
        wrapper = WindowClient(EvidenceClient(provider), SYMBOL, previous, False)
        self.assertEqual(wrapper.cusip_discovery_windows(BINDING, START, END, covered_by_symbol=False), [])
        expanded = wrapper.cusip_discovery_windows(BINDING, START, "2024-03-01", covered_by_symbol=False)
        self.assertEqual([(w["start"], w["end"]) for w in expanded], [("2024-03-01", "2024-03-01")])
        previous["evidence"]["query"]["corporate_actions_end"] = "2024-03-01"
        previous["evidence"]["cusip_action_discovery"] = {**BINDING, "windows": expanded}
        second = WindowClient(EvidenceClient(provider), SYMBOL, previous, False)
        second.cusip_discovery_windows(BINDING, START, "2024-03-02", covered_by_symbol=False)
        self.assertEqual(provider.filtered[-1], (CUSIP, "2024-03-02", "2024-03-02"))
        provider.asset = {**ASSET, "cusip": OTHER}
        second.cusip_discovery_windows({**BINDING, "cusip": OTHER}, START, END, covered_by_symbol=False)
        self.assertEqual(provider.filtered[-1], (OTHER, START, END))

    def test_filtered_http_cursor_survives_interrupted_stage_and_rejects_scope_changes(self):
        api = AlpacaIdentityClient("synthetic-key", "synthetic-secret")
        client = EvidenceClient(api)
        raw = {k: v for k, v in DIVIDEND.items() if k != "action_type"}
        page = {"corporate_actions": {"cash_dividends": [raw]}, "next_page_token": "continue"}
        with patch.object(api, "request_json", side_effect=[page, OSError("interrupted")]) as read:
            with self.assertRaisesRegex(OSError, "interrupted"):
                client.cusip_discovery_windows(BINDING, START, END)
        url = read.call_args_list[0].args[0]
        self.assertEqual(url.split("?")[0], ACTION_URL)
        self.assertEqual(
            parse_qs(urlsplit(url).query),
            {
                "cusips": [CUSIP],
                "start": [START],
                "end": [END],
                "region": ["us"],
                "data_quality": ["complete"],
                "limit": ["1000"],
                "sort": ["asc"],
            },
        )
        self.assertFalse(client.cusip_action_windows.get(CUSIP))
        resumed = AlpacaIdentityClient("synthetic-key", "synthetic-secret")
        restored = restore(client, resumed)
        with patch.object(resumed, "request_json", return_value={"corporate_actions": {}}) as read:
            (window,) = restored.cusip_discovery_windows(BINDING, START, END)
        self.assertEqual(parse_qs(urlsplit(read.call_args.args[0]).query)["page_token"], ["continue"])
        self.assertEqual(read.call_count, 1)
        self.assertEqual([x["symbol"] for x in window["matches"]], [FORMER])
        need = {"kind": "corporate_actions_by_cusip", "why": "test", "cusip": CUSIP, "start": START, "end": END}
        plan = build_plan({"steps": [{"needs": [need, need]}]})
        (action,) = plan["steps"][0]["actions"]
        self.assertEqual(len(action["deliver"]), 2)
        action["request"]["params"]["symbols"] = "OUTSIDER"
        with (
            patch.object(api, "request_json", side_effect=AssertionError("escaped scope")),
            self.assertRaises(ValueError),
        ):
            api.execute_plan(plan)
        with self.assertRaises(ValueError):
            build_plan({"steps": [{"needs": [{**need, "cusip": "CUSIP,ANOTHER"}]}]})

    def test_repeated_cursor_and_denied_late_page_cannot_publish_partial_labels(self):
        api = AlpacaIdentityClient("synthetic-key", "synthetic-secret")
        client = EvidenceClient(api)
        first = {"corporate_actions": {"cash_dividends": [DIVIDEND]}, "next_page_token": "same"}
        with (
            patch.object(api, "request_json", side_effect=[first, first]),
            self.assertRaisesRegex(RuntimeError, "repeated"),
        ):
            client.cusip_discovery_windows(BINDING, START, END)
        self.assertFalse(client.cusip_action_windows.get(CUSIP))
        with patch.object(api, "request_json", side_effect=IdentityHttpError(403, "forbidden")) as read:
            (window,) = client.cusip_discovery_windows(BINDING, START, END)
        self.assertEqual(read.call_count, 1)
        self.assertEqual(window["outcome"], "denied")
        self.assertEqual(window["matches"], [])

    def test_conflicting_returned_action_id_cannot_authorize_a_label(self):
        provider = Provider(issued=[DIVIDEND, {**DIVIDEND, "cusip": OTHER, "symbol": "OUTSIDER"}])
        evidence = EvidenceClient(provider)
        with self.assertRaisesRegex(ValueError, "conflicting native CUSIP action identity"):
            acquire_unpriced(SYMBOL, REQUEST, evidence)
        self.assertEqual(provider.symbol_calls, [SYMBOL])
        self.assertEqual(evidence.cusip_action_index["by_symbol"], {})
        with patch.object(provider, "get_cusip_actions", side_effect=AssertionError("repeated failed response")):
            with self.assertRaisesRegex(ValueError, "conflicting native CUSIP action identity"):
                restore(evidence, provider).cusip_discovery_windows(BINDING, START, END)

    def test_direct_report_rejects_conflicts_in_either_query_order(self):
        for named, issued, expected_calls in (
            ([], [{**DIVIDEND, "rate": 0.5}], [SYMBOL, FORMER]),
            ([{**DIVIDEND, "symbol": SYMBOL}], [DIVIDEND], [SYMBOL]),
        ):
            with self.subTest(expected_calls=expected_calls):
                provider = Provider(named=named, issued=issued)
                with self.assertRaisesRegex(ValueError, "conflicting corporate-action identity"):
                    build_identity_report(SYMBOL, {}, provider, query_dates=(START, END))
                self.assertEqual(provider.symbol_calls, expected_calls)
                self.assertEqual(provider.filtered, [(CUSIP, START, END)])


if __name__ == "__main__":
    unittest.main()
