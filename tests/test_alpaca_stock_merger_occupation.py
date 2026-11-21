"""Exact cross-label stock merger ownership without a cross-security join."""

from copy import deepcopy
from datetime import date
from types import SimpleNamespace
from unittest import TestCase

from libs.alpaca_end_of_day_security_correlation import (
    closed_departure,
    ownership_interval_rules,
    stock_merger_occupation,
)
from libs.alpaca_end_of_day_security_correlation.ticker_reuse_gap import adjustment_session_consensus
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def evidence(symbol, *, settlement=True):
    old = {"symbol": "RETIRED", "id": "old-asset", "class": "us_equity", "status": "inactive", "tradable": False}
    new = {
        "symbol": symbol,
        "id": "new-asset",
        "class": "us_equity",
        "status": "active",
        "tradable": True,
        "exchange": "NASDAQ",
    }
    escrow = {**old, "symbol": "ESCROW", "id": "escrow-asset"}
    rename = {
        "id": "historical-rename",
        "action_type": "name_change",
        "old_symbol": "FORMER",
        "new_symbol": symbol,
        "old_cusip": "111111111",
        "new_cusip": "111111111",
        "process_date": "2026-01-02",
    }
    merger = {
        "id": "terminal-merger",
        "action_type": "stock_merger",
        "acquiree_symbol": old["symbol"],
        "acquirer_symbol": symbol,
        "acquiree_cusip": "111111111",
        "acquirer_cusip": "222222222",
        "acquiree_rate": 1,
        "acquirer_rate": "0.10",
        "effective_date": "2026-03-02",
        "payable_date": "2026-03-02",
        "process_date": "2026-03-02",
    }
    later = {
        **merger,
        "id": "settlement",
        "acquiree_symbol": escrow["symbol"],
        "acquiree_cusip": "333333333",
        "effective_date": "2026-05-04",
        "payable_date": "2026-05-04",
        "process_date": "2026-05-04",
    }

    def lookup(kind, value, asset):
        return {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": asset,
        }

    def occupancy(label, cusip, asset, action):
        return {
            "symbol": label,
            "cusip": cusip,
            "asset_id": asset["id"],
            "evidence": [
                {
                    "symbol": label,
                    "cusip": cusip,
                    "action_type": action["action_type"],
                    "action_id": action["id"],
                    "process_date": action["process_date"],
                }
            ],
        }

    return {
        "query": {
            "input_symbol": symbol,
            "corporate_actions_start": "2025-01-01",
            "corporate_actions_end": "2026-10-01",
            "corporate_actions_data_quality": "complete",
        },
        "identities": [old, new, escrow],
        "asset_lookups": [
            lookup("symbol", symbol, new),
            lookup("cusip", "111111111", old),
            lookup("cusip", "222222222", new),
            lookup("cusip", "333333333", escrow),
        ],
        "corporate_actions": [rename, merger, *([later] if settlement else [])],
        "symbol_occupancy_evidence": [
            occupancy("FORMER", "111111111", old, rename),
            occupancy(symbol, "111111111", old, rename),
            occupancy(old["symbol"], "111111111", old, merger),
            occupancy(symbol, "222222222", new, merger),
            *([occupancy(escrow["symbol"], "333333333", escrow, later)] if settlement else []),
        ],
    }


class StockMergerOccupationTests(TestCase):
    @staticmethod
    def intervals(lineage, *, enabled):
        edges, _ = build_uuid_lineage_edges(lineage)
        rows, _ = ownership_interval_rules.build_ticker_ownership_intervals(
            lineage, uuid_lineage_edges=edges, allow_cross_label_stock_merger_occupation=enabled
        )
        return rows, edges

    def test_active_issue_is_assigned_its_dated_own_label_independently(self):
        for symbol in ("ACTIVE", "LATER"):
            with self.subTest(symbol=symbol):
                source = evidence(symbol)
                old, old_edges = self.intervals(source, enabled=False)
                new, edges = self.intervals(source, enabled=True)
                self.assertEqual(edges, old_edges)
                self.assertEqual(new[0], old[0])
                self.assertEqual([e.predecessor_asset_id for e in edges], ["old-asset"])
                self.assertEqual(
                    [
                        (
                            row.asset_id,
                            row.start_date_inclusive,
                            row.end_date_exclusive,
                            row.applied_rules[-1].name,
                            row.applied_rules[-1].supporting_action_ids,
                        )
                        for row in new
                        if row.historical_symbol == symbol
                    ],
                    [
                        (
                            "old-asset",
                            date(2026, 1, 2),
                            date(2026, 3, 2),
                            stock_merger_occupation.NAME,
                            ("terminal-merger",),
                        ),
                        (
                            "new-asset",
                            date(2026, 3, 2),
                            None,
                            stock_merger_occupation.NAME,
                            ("terminal-merger",),
                        ),
                    ],
                )

    def test_conflicting_or_irrelevant_anchors_preserve_previous_intervals(self):
        source = evidence("ACTIVE", settlement=False)
        cases = {}
        for label in ("dates disagree", "retiree still active", "different current lookup", "rival remains"):
            cases[label] = deepcopy(source)
        cases["dates disagree"]["corporate_actions"][-1]["process_date"] = "2026-03-03"
        cases["retiree still active"]["asset_lookups"][1]["asset"]["tradable"] = True
        cases["different current lookup"]["asset_lookups"][0]["asset"] = {
            **source["asset_lookups"][0]["asset"],
            "id": "rival",
        }
        cases["rival remains"]["symbol_occupancy_evidence"][1]["evidence"].append(
            {"process_date": "2026-06-01", "action_type": "cash_dividend"}
        )
        repeated = deepcopy(source)
        repeated["corporate_actions"].append(
            {
                **repeated["corporate_actions"][-1],
                "id": "conflicting-event",
                "effective_date": "2026-03-03",
                "payable_date": "2026-03-03",
                "process_date": "2026-03-03",
            }
        )
        cases["two distinct purported cutovers"] = repeated
        for label, lineage in cases.items():
            with self.subTest(label=label):
                self.assertEqual(self.intervals(lineage, enabled=True), self.intervals(lineage, enabled=False))

    @staticmethod
    def outgoing_evidence(symbol):
        lineage = evidence(symbol, settlement=False)
        old, current, destination = lineage["identities"]
        old["symbol"] = symbol
        old["name"] = current["name"] = "Provider repeats the current display name"
        lineage["asset_lookups"][0]["source"] = "asset_catalogue"
        lineage["catalogue_observed_at_utc"] = "2026-10-01T01:00:00+00:00"
        lineage["unresolved_cusips"] = []
        action = lineage["corporate_actions"][-1]
        action.update(acquiree_symbol=symbol, acquirer_symbol=destination["symbol"], acquirer_cusip="333333333")
        lineage["corporate_actions"].append(
            {
                "id": "same-issue-event",
                "action_type": "spin_off",
                "source_symbol": symbol,
                "new_symbol": symbol,
                "source_cusip": "111111111",
                "new_cusip": "111111111",
                "source_rate": 1,
                "new_rate": 1,
                "ex_date": action["effective_date"],
                "process_date": action["effective_date"],
            }
        )
        return lineage

    def test_cross_label_acquiree_departure_preserves_existing_interval_and_uuid_membership(self):
        for symbol in ("AAA", "ZZZ"):
            with self.subTest(symbol=symbol):
                lineage = self.outgoing_evidence(symbol)
                prior = ownership_interval_rules.build_ticker_ownership_intervals(lineage)[0]
                candidate = ownership_interval_rules.build_ticker_ownership_intervals(
                    lineage, allow_stock_merger_departure=True
                )[0]
                old = next(interval for interval in prior if interval.historical_symbol == symbol)
                new = next(interval for interval in candidate if interval.historical_symbol == symbol)
                self.assertIsNone(old.end_date_exclusive)
                self.assertEqual(new.end_date_exclusive, date(2026, 3, 2))
                self.assertEqual(new.start_date_inclusive, old.start_date_inclusive)
                self.assertEqual(new.identity_key(), old.identity_key())
                self.assertEqual(new.applied_rules[-1].supporting_action_ids, ("terminal-merger",))
                self.assertEqual([row for row in candidate if row != new], [row for row in prior if row != old])

    def test_departure_rejects_competing_traded_security_and_conflicting_dates(self):
        for change in ("distinct distribution", "retained prior action", "conflicting date", "active acquiree"):
            lineage = self.outgoing_evidence("AAA")
            if change == "distinct distribution":
                lineage["corporate_actions"][-1]["new_cusip"] = "333333333"
            elif change == "retained prior action":
                lineage["corporate_actions"].append(
                    {
                        "action_type": "cash_dividend",
                        "id": "survives",
                        "symbol": "AAA",
                        "cusip": "111111111",
                        "process_date": "2026-05-04",
                    }
                )
            elif change == "conflicting date":
                lineage["corporate_actions"][1]["payable_date"] = "2026-03-03"
            else:
                lineage["identities"][0]["status"] = "active"
            with self.subTest(change=change):
                self.assertEqual(
                    ownership_interval_rules.build_ticker_ownership_intervals(
                        lineage, allow_stock_merger_departure=True
                    ),
                    ownership_interval_rules.build_ticker_ownership_intervals(lineage),
                )

    def test_observed_later_current_session_uses_a_hot_suffix_and_rejects_carry_forward(self):
        symbol = "AAA"
        lineage = self.outgoing_evidence(symbol)
        intervals = ownership_interval_rules.build_ticker_ownership_intervals(
            lineage, allow_stock_merger_departure=True
        )[0]
        sessions = (date(2026, 9, 30),)
        consensus = adjustment_session_consensus((name, sessions) for name in ADJUSTMENTS)
        for volume in (5, 0):
            source = {
                "requested_symbol": symbol,
                "request_from": "2026-01-02",
                "request_to": "2026-10-01",
                "asof": "2026-10-01",
                "feed": "sip",
                "timeframe": "1Day",
            }
            row = dict(o=5, h=5, l=5, c=5, v=volume, n=volume, vw=5 if volume else 0)
            work = [
                (SimpleNamespace(rows=(row,)), {**source, "adjustment": name}, sessions) for name in ADJUSTMENTS
            ]
            result = closed_departure.assess_current_source(
                symbol,
                lineage,
                intervals,
                (),
                work,
                consensus,
                complete=False,
                informative=False,
                evidence_issues=(),
                complete_sources={},
                source_consensus=None,
                allow_observed_current_session=True,
                selected_sources={symbol},
            )
            if volume:
                self.assertEqual(
                    (result.asset_id, result.first, result.last), ("new-asset", "2026-09-30", "2026-09-30")
                )
            else:
                self.assertIsNone(result)

    def test_daily_update_recovers_an_older_outgoing_merger_route_before_pricing(self):
        from datetime import datetime, timezone
        from unittest.mock import patch

        from test_alpaca_durable_maintenance import ActionCatalogueClient, ProcessInterrupted, fixture
        from test_alpaca_lineage_build import informative_bars
        from test_alpaca_pricing_download import FakeClient, bar

        from alpaca_end_of_day import DataLibrary
        from libs.alpaca_end_of_day_security_correlation.security_identity import (
            security_id_for_alpaca_asset_ids,
        )
        from libs.alpaca_lineage_build import pipeline, storage, universe_state
        from libs.alpaca_lineage_build.catalogue import observe_catalogue
        from libs.alpaca_lineage_build.check_summary import rebuild
        from libs.alpaca_lineage_build.contract import versions
        from libs.alpaca_lineage_build.metadata import EvidenceClient
        from libs.alpaca_lineage_build.ruleset import load_ruleset
        from libs.alpaca_lineage_build.universe_update import rebuild_universe, run_update
        from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
        from libs.alpaca_serving_catalogue.producer import maintain

        symbol = "RETURN"
        retained = self.outgoing_evidence(symbol)
        old, current, acquirer = retained["identities"]
        old["exchange"] = acquirer["exchange"] = "NASDAQ"
        acquirer.update(name="Acquiring common shares", status="active", tradable=True)
        initial_day, final_day, cutoff = "2026-02-27", "2026-10-16", "2026-10-17"
        old_id, current_id, acquirer_id = (
            security_id_for_alpaca_asset_ids([asset["id"]]) for asset in (old, current, acquirer)
        )
        legacy = load_ruleset("resources/alpaca_security_correlation_v102_legacy_policy.yaml")
        with fixture() as case:
            store = case.directory / "merger"
            old_bars = informative_bars("2026-01-02", "2026-03-02")
            first, latest = old_bars[0]["t"][:10], informative_bars(final_day, cutoff)[0]
            previous = [bar("2025-12-31", "33")]
            peer = informative_bars("2026-03-02", cutoff)
            prices = {symbol: [*old_bars, latest], acquirer["symbol"]: peer, "FORMER": previous}
            assets = {f["lookup"]: f["asset"] for f in retained["asset_lookups"]}
            assets[acquirer["symbol"]] = acquirer
            assets["FORMER"] = {**old, "symbol": "FORMER", "id": "former-label-id"}
            identity = ActionCatalogueClient([current, acquirer], retained["corporate_actions"], exact=assets)
            catalogue_time = datetime(2026, 10, 17, 7, tzinfo=timezone.utc)
            with (
                patch("libs.alpaca_lineage_build.catalogue.datetime") as clock,
                patch("libs.alpaca_lineage_build.ruleset.load_ruleset", return_value=legacy),
                patch(
                    "libs.alpaca_lineage_build.transform.versions",
                    return_value={
                        **versions(),
                        "correlation": 102,
                        "correlation_policy_sha256": legacy.policy.semantic_sha256,
                    },
                ),
            ):
                clock.now.return_value = catalogue_time
                for label, rows in prices.items():
                    download_pricing(store, label, cutoff, client=FakeClient(rows))
                seed = pipeline.run_lineage(
                    store,
                    identity_client=identity,
                    evidence_client=EvidenceClient(identity, observation=observe_catalogue(identity)),
                )
            self.assertFalse(seed["failed"], seed)
            root = store / "end_of_day/lineage"
            before = storage.read_index(root)
            receipt = storage.read_receipt(root, symbol, before)
            self.assertIn(old_id, receipt["observation_routes"])
            self.assertEqual(receipt["observation_routes"][old_id][0][:10], final_day)
            self.assertNotIn(current_id, receipt["observation_routes"])
            self.assertEqual(receipt["versions"]["correlation"], 102)
            rebuild(store)
            rebuild_universe(store)
            self.assertTrue(maintain(store, bootstrap=True)["complete"])

            case.live = {**prices, symbol: [{**latest, "v": 12345}]}
            calls = []

            def dated_page(params):
                calls.append(dict(params))
                result = case.page(params)
                if symbol in params["symbols"].split(",") and params["asof"] == initial_day:
                    result["bars"][symbol] = [
                        row for row in old_bars if params["start"] <= row["t"] <= params["end"]
                    ]
                return result

            def update():
                with (
                    patch("libs.alpaca_lineage_build.catalogue.datetime") as clock,
                    patch.object(universe_state, "now_utc", return_value="2031-01-01T00:00:00+00:00"),
                ):
                    clock.now.return_value = catalogue_time
                    return run_update(
                        store,
                        cutoff,
                        pricing_client=case.pricing,
                        identity_client=identity,
                        registry=case.registry,
                    )

            with patch.object(case.pricing, "_get", side_effect=dated_page):
                failed = update()
            self.assertFalse(failed["complete"], failed)
            self.assertEqual(failed["failures"][0]["reason"], "missing live comparison bar")
            self.assertEqual(failed["failures"][0]["source_sha256"], before["symbols"][symbol]["source_sha256"])
            self.assertNotIn("comparison_route_review", failed["stages"])

            def interrupt_correction(params):
                if symbol in params["symbols"].split(",") and params["adjustment"] != "all":
                    self.assertEqual(params["start"][:10], final_day, params)
                    raise ProcessInterrupted
                return dated_page(params)

            calls.clear()
            with (
                patch.object(case.pricing, "_get", side_effect=interrupt_correction),
                patch.object(storage, "selected_document", wraps=storage.selected_document) as selected_reads,
                self.assertRaises(ProcessInterrupted),
            ):
                update()
            older_receipts_read = [
                call.args[0].parent.name
                for call in selected_reads.call_args_list
                if call.args[0].name == "lineage.manifest.json"
                and call.args[1] == before["symbols"][call.args[0].parent.name]["sha256"]
            ]
            self.assertCountEqual(older_receipts_read, prices)
            selected = storage.read_index(root, symbol)["symbols"][symbol]
            self.assertEqual(selected["source_sha256"], before["symbols"][symbol]["source_sha256"])
            (route,) = selected["projection"]["historical_price_routes"]
            self.assertEqual((route["owner"], route["asof"]), (old_id, initial_day))
            approvals = universe_state.read_state(store)["work"][symbol]["pricing_correction"]
            self.assertEqual(approvals["scope"], "latest_session")
            self.assertEqual(approvals["selection"]["lineage_generation"], selected["sha256"])
            routed = [params for params in calls if symbol in params["symbols"].split(",")]
            self.assertTrue(any(params["start"][:10] == first for params in routed), routed)
            self.assertEqual({p["asof"] for p in routed if p["start"][:10] != final_day}, {initial_day})

            def resume_correction(params):
                if symbol in params["symbols"].split(","):
                    self.assertEqual(params["start"][:10], final_day, params)
                    self.assertNotEqual(params["adjustment"], "all", params)
                return dated_page(params)

            with (
                patch.object(case.pricing, "_get", side_effect=resume_correction),
                patch(
                    "libs.alpaca_lineage_build.universe_update.review_routes",
                    side_effect=AssertionError("completed route review repeated"),
                ),
            ):
                result = update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["pending_work"], 0)
            public = DataLibrary(store=store)
            self.assertEqual({m.public_id for m in public.resolve_ticker(symbol)}, {old_id, current_id})
            (match,) = public.resolve_ticker(acquirer["symbol"])
            self.assertEqual(match.public_id, acquirer_id)
            for adjustment, view in read_pricing_views(store, symbol).items():
                self.assertEqual(len(view), len(old_bars) + 1)
                self.assertEqual(view[-1][5], "12345")
                self.assertEqual(
                    [row.session.isoformat() for row in public.get_history(current_id, adjustment).bars],
                    [final_day],
                )
