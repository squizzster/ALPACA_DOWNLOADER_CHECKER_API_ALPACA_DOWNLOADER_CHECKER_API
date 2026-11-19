"""Discover a prior ticker only from native split terms and its complete literal tape."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_price_basis_limits import CUT, evidence

from libs.alpaca_data_api_solver import ACTION_URL, build_plan
from libs.alpaca_pricing_download import historical_split_review as prior
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient


class HistoricalSplitTests(TestCase):
    def test_date_only_native_split_plan(self):
        need = {"kind": "market_split_session", "why": "historical_price_basis", "session": CUT}
        action = build_plan({"steps": [{"needs": [need]}]})["steps"][0]["actions"][0]
        self.assertEqual(action["request"]["url"], ACTION_URL)
        params = action["request"]["params"]
        self.assertEqual(
            (params["start"], params["end"], params["types"]), (CUT, CUT, "forward_split,reverse_split")
        )
        with self.assertRaises(ValueError):
            build_plan({"steps": [{"needs": [{**need, "symbol": "RESEARCH"}]}]})

    def test_modified_plan_cannot_expand_the_credentialed_market_request(self):
        api = AlpacaIdentityClient("test", "test")
        need = {"kind": "market_split_session", "why": "historical_price_basis", "session": CUT}
        plan = build_plan({"steps": [{"needs": [need]}]})
        plan["steps"][0]["actions"][0]["request"]["params"]["end"] = "2030-01-01"
        with (
            patch.object(api, "request_json", side_effect=AssertionError("unexpected transport")),
            self.assertRaisesRegex(ValueError, "single-session scope"),
        ):
            api.execute_plan(plan)

    def test_unique_literal_winner_and_independent_current_ticker_are_required(self):
        for case in ("unique", "tie", "competing_current", "volume_differs"):
            with self.subTest(case=case):
                inputs = evidence()
                split = inputs["actions"].pop()
                identity = Mock()
                identity.get_market_splits.return_value = [{**split, "symbol": s} for s in ("PREVIOUS", "PEER")]
                for bundle in inputs["views"].values():
                    for bar in bundle["bars"]:
                        bar.update(timestamp=bar["session"] + "T04:00:00Z", vwap=bar["close"])
                raws = {row["session"]: row for row in inputs["views"]["raw"]["bars"]}

                def observe(_, sym, asof, mode, first, last):
                    self.assertEqual((asof, mode), ("-", "raw"))
                    if sym == "FUTURE":
                        return raws if case == "competing_current" else {}
                    rows = deepcopy(raws)
                    if (sym == "PEER" and case != "tie") or case == "volume_differs":
                        rows[CUT]["volume"] = "401"
                    return rows

                stages = {}

                def put(k, v):
                    stages[k] = v
                    return v

                with patch.object(prior, "provider_rows", side_effect=observe):
                    result = prior.select(lambda: inputs, CUT, False, None, identity, stages.get, put)
                if case == "unique":
                    self.assertEqual(
                        (result["outcome"], result["symbol"], result["published_symbol"]),
                        ("check_regular_trades", "PREVIOUS", "FUTURE"),
                    )
                else:
                    self.assertEqual(result["outcome"], "unsupported")
                with patch.object(
                    prior, "provider_rows", side_effect=AssertionError("repeated frozen observations")
                ):
                    self.assertEqual(
                        prior.select(
                            Mock(side_effect=AssertionError("reopened source")),
                            CUT,
                            False,
                            None,
                            None,
                            stages.get,
                            put,
                        ),
                        result,
                    )
