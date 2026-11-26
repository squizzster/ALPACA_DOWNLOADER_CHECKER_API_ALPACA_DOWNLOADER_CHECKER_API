from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_serving_catalogue.metadata_evidence import project_group_evidence


def security(ticker, start, end, *, ownership_end=None):
    return {
        "ticker_ranges": [{"ticker": ticker, "first_session": start, "last_session": end}],
        "ticker_ownership_intervals": [
            {"ticker": ticker, "start_inclusive": start, "end_exclusive": ownership_end}
        ],
    }


def receipt(owner, asset, start, end, **evidence):
    return {
        "identity_memberships": [{"security_id": owner, "member_alpaca_asset_ids": [asset]}],
        "identity_intervals": [{"security_id": owner, "from": start, "to": end}],
        "evidence": evidence,
    }


def lookup(symbol, asset, **details):
    return {
        "lookup_kind": "symbol",
        "lookup": symbol,
        "asset": {"id": asset, "symbol": symbol, "class": "us_equity", **details},
    }


def print_sample(symbol, day, *, asof="-", **additional):
    return {
        "http_status": 200,
        "outcome": "observed",
        "symbol": symbol,
        "returned_symbol": symbol,
        "asof": asof,
        "session": day,
        "feed": "sip",
        "trade": {"t": day + "T15:10:00Z", "z": "C", "x": "P", "p": 22.5, "s": 50},
        "tape_network": "Nasdaq UTP",
        **additional,
    }


class MetadataEvidenceTests(TestCase):
    def setUp(self):
        self.former = security("SHARED", "2019-01-02", "2020-04-01", ownership_end="2020-04-02")
        self.later = security("SHARED", "2024-02-01", "2024-09-06")
        self.records = {"former": self.former, "later": self.later}
        self.sample = print_sample("SHARED", "2020-03-02")
        self.receipts = {
            "ARCHIVED": receipt(
                "former",
                "old_uuid",
                "2019-01-02",
                "2020-04-01",
                asset_lookups=[
                    {
                        **lookup("ARCHIVED", "old_uuid", exchange="OTC", status="inactive", tradable=False),
                        "source": "asset_catalogue",
                    },
                    {**lookup("REUSED", "old_uuid", name="unrelated current name"), "lookup_kind": "cusip"},
                ],
                catalogue_observed_at_utc="2026-01-02T20:00:00Z",
            ),
            "SHARED": receipt(
                "later",
                "new_uuid",
                "2024-02-01",
                "2024-09-06",
                historical_trade_tape=self.sample,
                asset_lookups=[lookup("NEW", "new_uuid", status="active", tradable=True)],
                catalogue_observed_at_utc="2025-05-01T00:00:00Z",
            ),
        }

    def test_literal_trade_uses_dated_owner_and_terminal_snapshot_keeps_observation_scope(self):
        result = project_group_evidence(self.records, self.receipts)
        self.assertEqual(
            result["former"]["asset_snapshots"],
            [
                {
                    "symbol": "ARCHIVED",
                    "asset_class": "us_equity",
                    "exchange": "OTC",
                    "status": "inactive",
                    "tradable": False,
                    "observed_at": "2026-01-02T20:00:00Z",
                }
            ],
        )
        self.assertEqual(
            result["former"]["trade_observations"],
            [
                {
                    "session": "2020-03-02",
                    "trade_timestamp": "2020-03-02T15:10:00Z",
                    "feed": "sip",
                    "request_symbol": "SHARED",
                    "asof": "-",
                    "returned_symbol": "SHARED",
                    "tape_code": "C",
                    "tape_network": "Nasdaq UTP",
                    "execution_venue": "P",
                }
            ],
        )
        self.assertNotIn("trade_observations", result["later"])
        self.assertIsNone(result["later"]["asset_snapshots"][0]["observed_at"])

    def test_overlapping_online_and_offline_claims_and_unknown_bounds(self):
        def observed():
            return "trade_observations" in project_group_evidence(self.records, self.receipts)["former"]

        self.records["later"] = self.former
        self.assertFalse(observed())
        self.records.pop("later")
        self.assertTrue(observed())  # The offline owner's contribution was years later.
        source = self.receipts["SHARED"]
        source["identity_intervals"][0].update({"from": "2018-01-01", "to": "2024-09-06"})
        self.assertFalse(observed())  # Legacy publication cannot exclude the overlapping offline owner.
        run = {"owner": "later", "from": "2018-01-01", "to": "2024-09-06", "tickers": ["OTHER"]}
        source.update(
            boundaries={"completed_years": {"sha256": "accepted"}},
            assignment_handoffs={
                "completed_years": {
                    "observation_choices": {
                        "views": {"raw": 0},
                        "partitions": [{"runs": [run]}],
                    }
                }
            },
        )
        self.assertTrue(observed())
        run["tickers"].append("SHARED")
        self.assertFalse(observed())
        self.records["later"] = self.later
        self.former["ticker_ownership_intervals"][0].update(start_inclusive=None, end_exclusive=None)
        self.assertFalse(observed())

    def literal_price_selection(self):
        symbol, day, owner = "NATIVE", "2021-05-04", "public"
        records = {owner: security(symbol, "2021-05-03", "2021-05-27")}
        records[owner]["ticker_ownership_intervals"][0]["start_inclusive"] = None
        source = receipt(
            owner, "provider", "2021-05-03", "2021-05-27", historical_trade_tape=print_sample(symbol, day)
        )
        request = {
            "symbol": symbol,
            "asof": "-",
            "feed": "sip",
            "timeframe": "1Day",
            "start": "2021-05-04T04:00:00Z",
            "end_exclusive": "2021-05-05T04:00:00Z",
        }
        run = {
            "branch": "correlated",
            "state": "selected",
            "owner": owner,
            "tickers": [symbol],
            "from": day,
            "first_timestamp": "2021-05-04T04:00:00Z",
            "to": "2021-05-27",
            "last_timestamp": "2021-05-27T04:00:00Z",
        }
        source.update(
            pricing_request=request,
            anchor_month="2021-05",
            assignment_handoffs={
                "current_month": {
                    "observation_choices": {
                        "views": {"raw": 1, "all": 0},
                        "partitions": [{"runs": []}, {"runs": [run]}],
                    }
                }
            },
        )
        return records, {symbol: source}, request, run

    def test_unknown_ownership_endpoints_can_use_bounded_literal_raw_route(self):
        records, receipts, request, run = self.literal_price_selection()

        def trades():
            return project_group_evidence(records, receipts)["public"].get("trade_observations", [])

        self.assertEqual([row["session"] for row in trades()], ["2021-05-04"])
        for changes in (
            {"asof": "2021-05-04"},
            {"symbol": "REUSED"},
            {"feed": "otc"},
            {"timeframe": "1Min"},
            {"start": "2021-05-07T04:00:00Z"},  # Later literal suffix, older print.
            {"start": "2021-05-04T05:00:00Z"},  # Trade in request, stored daily bar outside it.
            {"end_exclusive": "2021-05-04T15:10:00Z"},  # Bar in request, trade at its exclusive endpoint.
            {"start": None},
            {"end_exclusive": None},
        ):
            with self.subTest(changes=changes), patch.dict(request, changes):
                self.assertEqual(trades(), [])
        for changes in (
            {"state": "superseded"},
            {"tickers": ["FORMER"]},
            {"owner": "another_owner"},
            {"branch": "uncorrelated", "owner": None},
            {"state": None},
            {"from": "2021-05-03", "first_timestamp": "2021-05-03T04:00:00Z"},
        ):
            with self.subTest(changes=changes), patch.dict(run, changes):
                self.assertEqual(trades(), [])

    def test_literal_raw_uses_owning_partition_and_competing_owners(self):
        records, receipts, _, run = self.literal_price_selection()
        source = receipts["NATIVE"]
        parts = source["assignment_handoffs"]
        own = parts.pop("current_month")
        parts["current_year"] = own
        self.assertEqual(project_group_evidence(records, receipts), {"public": {}})
        source["anchor_month"] = "2021-06"
        self.assertIn("trade_observations", project_group_evidence(records, receipts)["public"])
        source["anchor_month"] = "2022-01"
        self.assertEqual(project_group_evidence(records, receipts), {"public": {}})
        parts["completed_years"] = own
        self.assertIn("trade_observations", project_group_evidence(records, receipts)["public"])
        records["competing"] = records["public"]
        self.assertEqual(project_group_evidence(records, receipts), {"public": {}, "competing": {}})
        del records["competing"]
        own["observation_choices"]["partitions"][1]["runs"].append({**run, "owner": "other"})
        self.assertEqual(project_group_evidence(records, receipts), {"public": {}})

    def test_literal_raw_time_scope_is_not_reused_for_another_print_on_same_day(self):
        records, receipts, request, _ = self.literal_price_selection()
        source = receipts["NATIVE"]
        request["end_exclusive"] = "2021-05-04T16:00:00Z"
        first = source["evidence"]["historical_trade_tape"]
        second = {**first, "trade": {**first["trade"], "t": "2021-05-04T16:30:00Z"}}
        receipts["PEER"] = receipt(
            "public", "provider", "2021-05-04", "2021-05-27", historical_trade_tape=second
        )
        facts = project_group_evidence(records, receipts)["public"]["trade_observations"]
        self.assertEqual([row["trade_timestamp"] for row in facts], ["2021-05-04T15:10:00Z"])

    def test_mapped_route_joins_source_owner_on_actual_observed_session(self):
        point = {
            "session": "2020-04-01",
            "outcome": "no_trades",
            "asof": "2026-01-01",
            "pricing_route_binding": {"observed_symbol_asset_id": "old_uuid"},
        }
        actual = print_sample(
            "ARCHIVED",
            "2018-01-01",
            asof=point["asof"],
            observed_session="2020-03-04",
            trade={"t": "2020-03-05T00:30:00Z", "z": "A", "x": "T"},
            tape_network="CTA network A",
        )
        point["last_trade_search"] = {"outcome": "observed", "last_request": actual}
        evidence = self.receipts["ARCHIVED"]["evidence"]
        evidence["mapped_historical_trade_tape"] = point
        evidence["otc_historical_trade_tape"] = {"outcome": "unavailable", "http_status": 403}
        result = project_group_evidence(self.records, self.receipts)["former"]["trade_observations"]
        self.assertEqual(len(result), 2)
        self.assertEqual(
            (result[1]["session"], result[1]["trade_timestamp"], result[1]["request_symbol"], result[1]["asof"]),
            ("2020-03-04", "2020-03-05T00:30:00Z", "ARCHIVED", "2026-01-01"),
        )
        point["pricing_route_binding"]["observed_symbol_asset_id"] = "new_uuid"
        result = project_group_evidence(self.records, self.receipts)
        self.assertEqual(len(result["former"]["trade_observations"]), 1)
        self.assertNotIn("trade_observations", result["later"])

    def test_cash_terms_bind_to_acquiree_and_are_independent_of_print(self):
        evidence = self.receipts["ARCHIVED"]["evidence"]
        evidence["corporate_actions"] = [{"action_type": "cash_dividend", "id": "group-dividend"}]
        evidence["terminal_cash_merger_trade"] = {
            "outcome": "unavailable",
            "observed_symbol_asset_id": "old_uuid",
            "cash_merger": {
                "action_id": "native",
                "role": "acquiree",
                "symbol": "ARCHIVED",
                "cusip": "123456789",
                "rate": "42.0500",
                "effective_date": "2020-04-02",
            },
        }
        expected = {**evidence["terminal_cash_merger_trade"]["cash_merger"], "action_type": "cash_merger"}
        result = project_group_evidence(self.records, self.receipts)
        self.assertEqual(result["former"]["corporate_actions"], [expected])
        self.assertNotIn("corporate_actions", result["later"])
        self.receipts["SHARED"]["identity_memberships"].append(
            {"security_id": "offline", "member_alpaca_asset_ids": ["old_uuid"]}
        )
        result = project_group_evidence(self.records, self.receipts)
        self.assertNotIn("corporate_actions", result["former"])
        self.assertNotIn("asset_snapshots", result["former"])

    def test_negative_results_and_legacy_additions_never_become_positive_public_facts(self):
        source = self.receipts["SHARED"]["evidence"]
        source["historical_trade_tape"] = {"outcome": "no_trades", "http_status": 200}
        self.assertNotIn("trade_observations", project_group_evidence(self.records, self.receipts)["former"])
        source.update(historical_trade_tape=self.sample, event_supplements=["archived research"])
        self.assertNotIn("trade_observations", project_group_evidence(self.records, self.receipts)["former"])
        del source["event_supplements"]
        source["provider_corporate_actions"] = []
        self.assertNotIn("trade_observations", project_group_evidence(self.records, self.receipts)["former"])

    def test_duplicate_native_facts_have_one_representation_legacy_missing_membership_is_not_guessed(self):
        self.receipts["ALIAS"] = self.receipts["SHARED"]
        result = project_group_evidence(self.records, self.receipts)
        self.assertEqual(len(result["former"]["trade_observations"]), 1)
        self.assertEqual(len(result["later"]["asset_snapshots"]), 1)
        self.receipts["LEGACY"] = {"evidence": {}}
        self.assertEqual(project_group_evidence(self.records, self.receipts), {"former": {}, "later": {}})
        self.receipts["LEGACY"]["correlated_rows"] = 0
        self.assertEqual(project_group_evidence(self.records, self.receipts), result)
        self.receipts["LEGACY"] = {
            "identity_intervals": self.receipts["ARCHIVED"]["identity_intervals"],
            "evidence": {},
        }
        self.assertEqual(project_group_evidence(self.records, self.receipts), result)
        # A positively scoped modern mapped print is still covered by its own
        # source assignment even if a legacy source's UUID roster is unknown.
        self.receipts["LEGACY"] = {"evidence": {}}
        self.receipts["ARCHIVED"]["evidence"]["mapped_historical_trade_tape"] = print_sample(
            "ARCHIVED",
            "2020-03-05",
            asof="2025-04-01",
            pricing_route_binding={"observed_symbol_asset_id": "old_uuid"},
        )
        mapped = project_group_evidence(self.records, self.receipts)["former"]
        self.assertEqual(set(mapped), {"trade_observations"})
        self.assertEqual(mapped["trade_observations"][0]["request_symbol"], "ARCHIVED")

    def test_only_positively_bound_literal_otc_search_emits_observation(self):
        source = self.receipts["SHARED"]["evidence"]
        sample = print_sample(
            "SHARED",
            "2020-02-28",
            observed_session="2020-03-03",
            feed="otc",
            trade={"t": "2020-03-03T14:00:00Z"},
            tape_network="unknown",
        )
        source["otc_historical_trade_tape"] = {
            "outcome": "no_trades",
            "following_trade_search": {"outcome": "observed", "last_request": sample},
        }
        rows = project_group_evidence(self.records, self.receipts)["former"]["trade_observations"]
        self.assertEqual(
            [(r["session"], r["feed"]) for r in rows], [("2020-03-02", "sip"), ("2020-03-03", "otc")]
        )
        self.assertIsNone(rows[1]["tape_code"])
        self.assertIsNone(rows[1]["execution_venue"])
        source["otc_historical_trade_tape"]["following_trade_search"]["outcome"] = "outside_search_bounds"
        rows = project_group_evidence(self.records, self.receipts)["former"]["trade_observations"]
        self.assertEqual(len(rows), 1)

    def test_asset_snapshots_exclude_untrusted_provenance_and_preserve_conflicting_exact_observations(self):
        evidence = self.receipts["SHARED"]["evidence"]
        evidence["asset_lookups"] += [
            {**lookup("LABEL", "new_uuid", name="unaccepted"), "source": "research"},
            {**lookup("NEW", "new_uuid", tradable=False), "observed_at_utc": "2024-01-01T00:00:00Z"},
        ]
        rows = project_group_evidence(self.records, self.receipts)["later"]["asset_snapshots"]
        self.assertEqual([(r["symbol"], r["tradable"]) for r in rows], [("NEW", True), ("NEW", False)])
