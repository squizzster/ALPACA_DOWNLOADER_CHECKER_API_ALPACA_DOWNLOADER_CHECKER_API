"""Provider-only same-label distributions through ownership and serving."""

import json
from copy import deepcopy
from datetime import date
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import same_ticker_distribution as distribution
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_lineage_build.policy_engine import _correlation_cache
from libs.alpaca_lineage_build.retained_recovery import RECOVERY_BOUNDARY, recover
from libs.alpaca_lineage_build.ruleset import load_ruleset
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.lineage import build_identity_report
from libs.alpaca_serving_catalogue.producer import maintain

OLD, NEW, RECIPIENT = "X12345001", "X12345002", "X12345003"
CUTOVER = "2026-10-07"


def transaction(symbol, recipient, *, alternate_symbol=None):
    action = {
        "id": "example-distribution",
        "action_type": "unit_split",
        "old_symbol": symbol,
        "new_symbol": symbol,
        "alternate_symbol": alternate_symbol or recipient,
        "old_cusip": OLD,
        "new_cusip": NEW,
        "alternate_cusip": RECIPIENT,
        "old_rate": 1,
        "new_rate": 1,
        "alternate_rate": "0.25",
        "effective_date": "2026-10-05",
        "process_date": CUTOVER,
        "payable_date": CUTOVER,
    }
    current = asset(symbol, "successor")
    child = asset(recipient, "recipient")
    return action, {
        OLD: {**asset(symbol, "predecessor"), "status": "inactive", "tradable": False},
        NEW: current,
        RECIPIENT: child,
        symbol: current,
        recipient: child,
    }


def provider_report(symbol, recipient, alter=lambda action, assets: None):
    action, assets = transaction(symbol, recipient)
    alter(action, assets)
    return build_identity_report(
        symbol,
        {"downloads": [], "present": False},
        IdentityClient(assets, [action]),
        query_dates=("2026-10-01", "2026-10-14"),
    )


class DistributionEvidenceTests(TestCase):
    def test_fresh_tickers_share_the_same_rule_and_phases_reuse_the_decision(self):
        for symbol, recipient in (("FRST", "CHLD"), ("SECD", "SPIN")):
            with self.subTest(symbol=symbol):
                lineage = provider_report(symbol, recipient)
                original = deepcopy(lineage)
                cache = _correlation_cache(load_ruleset(authorize=True).policy)
                with (
                    patch.object(distribution, "assess", wraps=distribution.assess) as assess,
                    patch("socket.socket.connect", side_effect=AssertionError("inference used the provider")),
                    patch("pathlib.Path.open", side_effect=AssertionError("inference opened a file")),
                ):
                    established = cache.lineage_analysis(lineage)
                    fallback = cache.lineage_analysis(lineage, include_action_anchored_current_suffix=True)
                    self.assertEqual(established, cache.lineage_analysis(lineage))
                self.assertEqual(assess.call_count, 1)
                self.assertEqual(lineage, original)
                for intervals, _, edges, _ in (established, fallback):
                    self.assertEqual(
                        [
                            (item.asset_id, item.start_date_inclusive, item.end_date_exclusive)
                            for item in intervals
                        ],
                        [
                            ("predecessor", None, date.fromisoformat(CUTOVER)),
                            ("successor", date.fromisoformat(CUTOVER), None),
                        ],
                    )
                    self.assertEqual(
                        [(edge.edge_type, edge.predecessor_asset_id, edge.successor_asset_id) for edge in edges],
                        [(distribution.NAME, "predecessor", "successor")],
                    )

    def test_independent_recipient_and_market_dates(self):
        def use_cusip(action, assets):
            action.update(alternate_symbol=RECIPIENT)

        lineage = provider_report("SAME", "OTCC", use_cusip)
        edge, issue = distribution.assess(lineage, lineage["corporate_actions"])
        self.assertIsNone(issue)
        self.assertEqual(edge.effective_date, date.fromisoformat(CUTOVER))
        lineage["corporate_actions"][0]["ex_date"] = "2026-10-06"
        edge, issue = distribution.assess(lineage, lineage["corporate_actions"])
        self.assertIsNone(issue)
        self.assertEqual(edge.effective_date, date(2026, 10, 6))

    def test_unresolved_lookalikes_cannot_install_a_distribution_edge(self):
        cases = {
            "unequal exchange": (
                lambda action, assets: action.update(new_rate="0.01"),
                "invalid_or_unequal_distribution_rates",
            ),
            "undated switch": (
                lambda action, assets: action.pop("payable_date"),
                "distribution_market_date_uncorroborated",
            ),
            "unresolved recipient": (
                lambda action, assets: assets.pop(RECIPIENT),
                "missing_or_conflicting_distribution_lookup",
            ),
            "recipient matches main asset": (
                lambda action, assets: assets.update({RECIPIENT: assets[NEW]}),
                "competing_occupant_or_distribution_recipient",
            ),
            "rival current lookup": (
                lambda action, assets: assets.update(SAME=asset("SAME", "competitor")),
                "competing_occupant_or_distribution_recipient",
            ),
        }
        for label, (alter, reason) in cases.items():
            with self.subTest(label=label):
                lineage = provider_report("SAME", "OTCC", alter)
                intervals, _, edges, issues = _correlation_cache(load_ruleset().policy).lineage_analysis(lineage)
                self.assertFalse(intervals)
                self.assertFalse(edges)
                self.assertIn(reason, [issue["reason_code"] for issue in issues])

    def test_competing_dated_action_and_one_sided_split(self):
        lineage = provider_report("SAME", "OTCC")
        lineage["corporate_actions"].append(
            {
                "id": "rival-claim",
                "action_type": "cash_dividend",
                "symbol": "SAME",
                "cusip": OLD,
                "ex_date": "2026-10-08",
                "rate": 1,
            }
        )
        edge, issue = distribution.assess(lineage, lineage["corporate_actions"])
        self.assertIsNone(edge)
        self.assertEqual(issue["reason_code"], "dated_occupant_conflicts_with_distribution")
        lineage["corporate_actions"] = [
            {
                "id": "one-sided",
                "action_type": "forward_split",
                "symbol": "SAME",
                "cusip": OLD,
                "ex_date": CUTOVER,
                "old_rate": 1,
                "new_rate": 4,
            }
        ]
        self.assertEqual(distribution.assess(lineage, lineage["corporate_actions"]), (None, None))

    def test_frozen_control_keeps_the_original_decision(self):
        lineage = provider_report("SAME", "OTCC")
        policy = load_policy("resources/alpaca_security_correlation_v94_legacy_policy.yaml")
        intervals, _, edges, _ = _correlation_cache(policy).lineage_analysis(lineage)
        self.assertFalse(intervals)
        self.assertFalse(edges)


class DistributionServingTests(NativeCase):
    def test_normal_publication_serves_full_main_history_and_independent_recipient(self):
        symbol, recipient = "SAME", "SPUN"
        action, assets = transaction(symbol, recipient)
        recipient_bars = [row for row in self.bars if row["t"][:10] >= CUTOVER]
        for name, bars in ((symbol, self.bars), (recipient, recipient_bars)):
            download_pricing(self.store, name, "2026-10-14", client=FakeClient(bars))
        result = pipeline.run_lineage(
            self.store, [symbol, recipient], identity_client=IdentityClient(assets, [action])
        )
        self.assertEqual(result["failed"], 0, result)
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, symbol, index)
        self.assertEqual(receipt["unresolved_rows"], 0)
        self.assertEqual(receipt["correlated_rows"], len(self.bars) * len(ADJUSTMENTS))
        self.assertEqual(
            [(edge["edge_type"], edge["effective_date"]) for edge in receipt["uuid_lineage_edges"]],
            [(distribution.NAME, CUTOVER)],
        )
        boundaries = storage.read_boundaries(self.root, receipt)
        raw = next(item for item in boundaries["current_month"]["adjustments"] if item["adjustment"] == "raw")
        self.assertEqual(
            [
                (segment["from"], segment["to"], segment["ticker_ownership_interval"]["asset_id"])
                for segment in raw["segments"]
            ],
            [("2026-10-01", "2026-10-06", "predecessor"), (CUTOVER, "2026-10-13", "successor")],
        )
        data = self.bootstrap()
        (parent_result,) = data.resolve_ticker(symbol)
        (child_result,) = data.resolve_ticker(recipient)
        self.assertEqual(parent_result.public_id, security_id_for_alpaca_asset_ids(("predecessor", "successor")))
        self.assertEqual(child_result.public_id, security_id_for_alpaca_asset_ids(("recipient",)))
        histories = data.get_histories(parent_result.public_id)
        self.assertEqual(set(histories), set(ADJUSTMENTS))
        self.assertEqual({len(history.bars) for history in histories.values()}, {len(self.bars)})
        self.assertEqual(len(data.get_history(child_result.public_id).bars), len(recipient_bars))

        appended = valid_bars("2026-10-14", "2026-10-15")
        for name, bars in ((symbol, self.bars), (recipient, recipient_bars)):
            download_pricing(self.store, name, "2026-10-15", client=FakeClient(bars + appended))
        original_assess = distribution.assess

        def assess_new_only(lineage, actions):
            self.assertNotEqual(lineage["query"]["input_symbol"], symbol, "reassessed an accepted cutover")
            return original_assess(lineage, actions)

        with patch.object(distribution, "assess", side_effect=assess_new_only):
            result = pipeline.run_lineage(
                self.store, [symbol, recipient], identity_client=IdentityClient(assets, [action])
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(maintain(self.store)["complete"])
        data = DataLibrary(store=self.store)
        (advanced,) = data.resolve_ticker(symbol)
        self.assertEqual(advanced.public_id, parent_result.public_id)
        self.assertEqual(len(data.get_history(advanced.public_id).bars), len(self.bars) + len(appended))

    def test_targeted_retained_recovery_installs_the_journal_without_reassessment(self):
        class Interrupted(BaseException):
            pass

        symbol, recipient = "OLDL", "UNIT"
        action, assets = transaction(symbol, recipient)
        for name in (symbol, recipient):
            download_pricing(self.store, name, "2026-10-14", client=FakeClient(self.bars))
        with patch.object(distribution, "assess", return_value=(None, None)):
            result = pipeline.run_lineage(
                self.store, [symbol, recipient], identity_client=IdentityClient(assets, [action])
            )
        self.assertEqual(result["failed"], 0, result)
        data = self.bootstrap()
        self.assertEqual(data.resolve_ticker(symbol), ())
        (old_recipient,) = data.resolve_ticker(recipient)
        universe_state.initialize(self.store)
        entries = storage.read_index(self.root)["symbols"]
        members = entries[symbol]["group_members"]
        self.assertEqual(members, [symbol, recipient])
        path = self.directory / "recovery.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "boundary": RECOVERY_BOUNDARY,
                    "store": str(self.store),
                    "correlation_policy_sha256": load_ruleset().policy.semantic_sha256,
                    "groups": [
                        {
                            "members": members,
                            "pricing_generations": {name: entries[name]["source_sha256"] for name in members},
                            "receipt_generations": {name: entries[name]["sha256"] for name in members},
                        }
                    ],
                }
            )
        )
        with (
            patch.object(pipeline, "acquire", side_effect=AssertionError("provider evidence reloaded")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("prices reacquired")),
            patch.object(storage, "_install", side_effect=Interrupted()),
        ):
            with self.assertRaises(Interrupted):
                recover(self.store, path)
        with (
            patch.object(distribution, "assess", side_effect=AssertionError("recovery repeated the decision")),
            patch.object(
                pipeline, "load_pricing", side_effect=AssertionError("recovery reloaded lineage prices")
            ),
            patch.object(pipeline, "correlate", side_effect=AssertionError("recovery repeated correlation")),
        ):
            result = recover(self.store, path)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["previously_completed_groups"], 1)
        data = self.bootstrap()
        (main,) = data.resolve_ticker(symbol)
        (current_recipient,) = data.resolve_ticker(recipient)
        self.assertEqual(main.public_id, security_id_for_alpaca_asset_ids(("predecessor", "successor")))
        self.assertEqual(current_recipient.public_id, old_recipient.public_id)
