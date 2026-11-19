"""Provider split dates constrain public identifiers independently of ownership and prices."""

import unittest
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import asset

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.cusip_dates import constrain, split_evidence, supported_periods
from libs.alpaca_serving_catalogue.producer import maintain


def split(day, old, new, symbol="NEW"):
    return dict(
        id=day + old + new,
        action_type="reverse_split",
        symbol=symbol,
        ex_date=day,
        old_cusip=old,
        new_cusip=new,
        old_rate=5,
        new_rate=1,
    )


def lookups(symbol, values):
    return [
        dict(
            lookup_kind="cusip", lookup=value, outcome="found", found=True, http_status=200, asset=asset(symbol)
        )
        for value in values
    ]


class DatedCusipTests(unittest.TestCase):
    def periods(self, actions, **other):
        evidence = {"asset_lookups": lookups("NEW", "ABCD"), "corporate_actions": actions, **other}
        return supported_periods(split_evidence({"NEW": {"evidence": evidence}}), {"NEW-id"}, {"OLD", "NEW"})

    def test_recurring_identifier_and_conflicting_branches(self):
        changes = [split("2018-01-10", "A", "B"), split("2020-02-20", "B", "A")]
        windows = self.periods(changes)
        self.assertEqual(windows["A"], [(None, "2018-01-10"), ("2020-02-20", None)])
        self.assertEqual(windows["B"], [("2018-01-10", "2020-02-20")])
        periods = self.periods([*changes, split("2018-01-10", "A", "C")])
        self.assertTrue(all(periods[k] == [] for k in "ABC"))

    def test_other_owners_reused_tickers_missing_input_and_legacy_assertion(self):
        action = split("2018-01-10", "A", "B")
        evidence = {"asset_lookups": lookups("ELSE", "AB"), "corporate_actions": [action]}
        saved = split_evidence({"ELSE": {"evidence": evidence}})
        self.assertEqual(supported_periods(saved, {"NEW-id"}, {"NEW"}), {})
        self.assertEqual(supported_periods(saved, {"ELSE-id"}, {"OLD"}), {})
        self.assertEqual(self.periods([action], provider_corporate_actions=[]), {})
        self.assertEqual(self.periods([action], event_supplements=[{}]), {})
        self.assertEqual(self.periods([{**action, "ex_date": None, "process_date": "2018-01-10"}]), {})

    def test_old_ticker_ending_before_issue_keeps_ownership_without_cusip(self):
        intervals = [{"ticker": "OLD", "start_inclusive": None, "end_exclusive": "2018-01-10", "cusip": "B"}]
        claims = [dict(field="cusip", value="B", effective_from="2015-01-01", effective_through="2018-01-09")]
        owners, dated = constrain(intervals, claims, self.periods([split("2018-01-10", "A", "B")]))
        self.assertEqual(owners, [{**intervals[0], "cusip": None}])
        self.assertEqual(dated, [])

    def test_split_ex_date_uses_raw_provider_endpoint_and_exact_member_lookup(self):
        first = split("2024-09-11", "A", "B")
        raw = {
            **first,
            "action_type": "forward_split",
            "old_rate": 1,
            "new_rate": 5,
            "process_date": "2024-09-09",
        }
        periods = self.periods([split("2023-01-01", "A", "B")], provider_corporate_actions=[raw])
        self.assertEqual(periods["B"], [("2024-09-11", None)])
        self.assertEqual(self.periods([raw], asset_lookups=lookups("NEW", "A")), {})
        self.assertEqual(self.periods([raw], asset_lookups=lookups("NEW", "AB") + lookups("OTHER", "A")), {})
        self.assertEqual(self.periods([{**raw, "new_symbol": "OTHER"}]), {})


class DatedCusipPublicationTests(NativeCase):
    def test_changed_metadata_preserves_prices_and_serves_only_supported_dates(self):
        def previous_projection(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["canonical_security"]["cusip"] = "B"
                        segment["ticker_ownership_interval"] = {
                            "historical_symbol": "TEST",
                            "cusip": "B",
                            "asset_id": "TEST-id",
                            "start_date_inclusive": None,
                            "end_date_exclusive": None,
                        }
            receipt["evidence"]["asset_lookups"].extend(lookups("TEST", "ABC"))

        self.publish_change(previous_projection)
        (old,) = self.bootstrap().list_securities()
        self.assertTrue(all(i.cusip == "B" for i in old.ticker_ownership_intervals))

        def changed_evidence(receipt, _):
            receipt["evidence"]["corporate_actions"] = [
                split("2026-10-05", "A", "B", "TEST"),
                split("2026-10-08", "B", "C", "TEST"),
            ]

        self.publish_change(changed_evidence)
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted rows reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted numbers rechecked")),
        ):
            result = maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        current = DataLibrary(store=self.store).get_security(old.public_id)
        self.assertEqual(current.tickers, old.tickers)
        self.assertEqual(current.stored_session_count, old.stored_session_count)
        self.assertEqual(
            [
                (i.cusip, str(i.start_inclusive), str(i.end_exclusive))
                for i in current.ticker_ownership_intervals
            ],
            [(None, "None", "2026-10-05"), ("B", "2026-10-05", "2026-10-08"), (None, "2026-10-08", "None")],
        )
        self.assertEqual(
            [
                (str(o.effective_from), str(o.effective_through))
                for o in current.observations
                if o.field == "cusip" and o.value == "B" and o.effective_from
            ],
            [("2026-10-05", "2026-10-07")],
        )
        self.assertIn("B", current.cusips)
