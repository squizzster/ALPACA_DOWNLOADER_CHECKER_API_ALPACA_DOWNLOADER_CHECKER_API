"""Dated labels consume accepted owners and choices, independently of prices."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import dated_labels, observations
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def fixture(names=("OLD", "NEW", "ALIAS")):
    old, new, alias = names
    inputs, reports = {}, {}
    for source, days in ((old, (1, 2)), (new, (3, 4)), (alias, (1, 2, 3, 4))):
        rows = [(f"2030-01-{day:02}T05:00:00Z", *(["1"] * 7)) for day in days]
        inputs[source] = SimpleNamespace(
            views=dict.fromkeys(ADJUSTMENTS, rows), source={"activity_consistent": True}
        )
        reports[source] = {
            "correlated": {
                "segments": [
                    {
                        "adjustment": adjustment,
                        "row_start": index,
                        "row_end": index,
                        "from": row[0][:10],
                        "to": row[0][:10],
                        "historical_symbol": source,
                        "security_identity": {"security_id": "owner", "member_alpaca_asset_ids": ["uuid"]},
                        "canonical_security": {"cusip": "accepted"},
                        "observation_selection": "selected" if source == alias else "superseded",
                        "observation_tickers": sorted([alias, old if row[0][:10] < "2030-01-03" else new]),
                    }
                    for adjustment in ADJUSTMENTS
                    for index, row in enumerate(rows, 1)
                ]
            }
        }
    evidence = {
        old: {
            "corporate_actions": [
                {
                    "action_type": "name_change",
                    "old_symbol": old,
                    "new_symbol": new,
                    "process_date": "2030-01-04",
                    "old_cusip": "provider",
                    "new_cusip": "accepted",
                }
            ]
        }
    }
    return inputs, reports, evidence


class DatedLabelTests(TestCase):
    def test_same_result_for_renamed_securities_despite_imperfect_identifiers_and_late_processing(self):
        for names in (("THMA", "PEAR", "PEARQ"), ("CEDAR", "MAPLE", "ARCHIVE")):
            inputs, reports, evidence = fixture(names)
            original = deepcopy(reports)
            with (
                patch("builtins.open", side_effect=AssertionError("no storage or research access")),
                patch.object(
                    observations, "select_observations", side_effect=AssertionError("numerical choice repeated")
                ),
            ):
                selected, trace = dated_labels.project({}, reports, evidence)
            self.assertTrue(trace["changes"])
            self.assertTrue(trace["reasons"][0]["identifier_disagreements"])
            self.assertEqual(original, reports)
            for source, report in selected.items():
                for before, after in zip(
                    reports[source]["correlated"]["segments"], report["correlated"]["segments"]
                ):
                    self.assertEqual(
                        {k: v for k, v in before.items() if k != "observation_tickers"},
                        {k: v for k, v in after.items() if k != "observation_tickers"},
                    )
                    self.assertEqual(
                        [names[0] if after["from"] < "2030-01-03" else names[1]], after["observation_tickers"]
                    )

    def test_missing_identifier_and_processing_date_do_not_require_a_perfect_certificate(self):
        _inputs, reports, evidence = fixture()
        action = evidence["OLD"]["corporate_actions"][0]
        for key in ("old_cusip", "new_cusip", "process_date"):
            action.pop(key)
        self.assertTrue(dated_labels.project({}, reports, evidence)[1]["changes"])

    def test_processing_after_retained_endpoint_does_not_override_observed_periods(self):
        _inputs, reports, evidence = fixture()
        evidence["OLD"]["corporate_actions"][0]["process_date"] = "2030-02-01"
        self.assertTrue(dated_labels.project({}, reports, evidence)[1]["changes"])

    def test_missing_or_unrelated_rename_preserves_ambiguity(self):
        _inputs, reports, evidence = fixture()
        for actions in ([], [{**evidence["OLD"]["corporate_actions"][0], "process_date": "2020-01-01"}]):
            selected, trace = dated_labels.project({}, reports, {"OLD": {"corporate_actions": actions}})
            self.assertIs(selected, reports)
            self.assertFalse(trace["changes"])

    def test_other_owner_and_competing_chain_remain_explicit(self):
        _inputs, reports, evidence = fixture()
        for segment in reports["ALIAS"]["correlated"]["segments"]:
            segment["security_identity"] = {"security_id": "other"}
        self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"])
        _inputs, reports, evidence = fixture()
        evidence["OLD"]["corporate_actions"].append(
            {"action_type": "name_change", "old_symbol": "OLD", "new_symbol": "RIVAL"}
        )
        self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"])

    def test_alias_with_its_own_dated_action_preserves_parallel_market_possibility(self):
        _inputs, reports, evidence = fixture()
        evidence["ALIAS"] = {
            "corporate_actions": [
                {"action_type": "cash_dividend", "symbol": "ALIAS", "process_date": "2030-01-02"}
            ]
        }
        self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"])
        evidence["ALIAS"]["corporate_actions"][0]["process_date"] = "2031-01-02"
        self.assertTrue(dated_labels.project({}, reports, evidence)[1]["changes"])

    def test_legacy_research_annotations_do_not_supply_a_route(self):
        _inputs, reports, _evidence = fixture()
        for report in reports.values():
            report["sourced_facts"] = {"old_symbol": "OLD", "new_symbol": "NEW"}
            for segment in report["correlated"]["segments"]:
                segment["observation_authority"] = {"market": "regular_way", "source": "/research/secret"}
        self.assertIs(reports, dated_labels.project({}, reports, {})[0])

    def test_interior_split_uses_only_held_dates_and_preserves_arbitrary_equal_price_winner(self):
        inputs, reports, evidence = fixture()
        for source, report in reports.items():
            original = report["correlated"]["segments"]
            report["correlated"]["segments"] = [
                {
                    **next(s for s in original if s["adjustment"] == adjustment),
                    "row_end": len(inputs[source].views[adjustment]),
                    "to": inputs[source].views[adjustment][-1][0][:10],
                    "observation_tickers": [source] if source != "ALIAS" else ["ALIAS", "NEW", "OLD"],
                }
                for adjustment in ADJUSTMENTS
            ]
        selected, trace = dated_labels.project(inputs, reports, evidence)
        self.assertTrue(trace["changes"])
        alias = [s for s in selected["ALIAS"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual([(1, 2), (3, 4)], [(s["row_start"], s["row_end"]) for s in alias])
        self.assertTrue(all(s["observation_selection"] == "selected" for s in alias))
        self.assertTrue(all("ALIAS" not in s["observation_tickers"] for s in alias))

    def test_unique_labels_and_unoffered_chain_label_are_never_rewritten(self):
        _inputs, reports, evidence = fixture()
        for report in reports.values():
            for segment in report["correlated"]["segments"]:
                segment["observation_tickers"] = ["ALIAS"]
        self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"])
        self.assertEqual(
            ["ALIAS", "UNRELATED"],
            dated_labels.select_labels(
                "owner",
                "2030-01-01",
                ["ALIAS", "UNRELATED"],
                dated_labels.plan(reports, evidence)[0],
            ),
        )

    def test_no_rename_and_fewer_than_three_sources_do_not_traverse_segments(self):
        class Closed:
            def __iter__(self):
                raise AssertionError("unselected historical metadata traversed")

        reports = {name: {"correlated": {"segments": Closed()}} for name in ("A", "B", "C")}
        self.assertEqual(({}, []), dated_labels.plan(reports, {}))
        self.assertEqual(
            ({}, []),
            dated_labels.plan(
                {name: reports[name] for name in ("A", "B")},
                {
                    "A": {
                        "corporate_actions": [
                            {"action_type": "name_change", "old_symbol": "A", "new_symbol": "B"}
                        ]
                    }
                },
            ),
        )


class ActionSupportedLabelTests(TestCase):
    def pair(self, names=("HISTORIC", "MAPPED")):
        _, reports, _ = fixture()
        base = reports["ALIAS"]
        pair = {}
        for name in names:
            pair[name] = deepcopy(base)
            pair[name]["pricing_request"] = {"start": "2030-02-01T00:00:00Z"}
            for segment in pair[name]["correlated"]["segments"]:
                segment["historical_symbol"] = name
                segment["observation_tickers"] = sorted(names)
        evidence = {
            names[0]: {
                "corporate_actions": [
                    {
                        "action_type": "cash_dividend",
                        "symbol": names[0],
                        "ex_date": "2030-01-02",
                        "process_date": "2031-01-01",
                    }
                ]
            }
        }
        return pair, evidence

    def test_action_supported_same_owner_labels_preserve_every_other_field(self):
        for names in (("FRC", "FRCB"), ("CEDAR", "ARCHIVE")):
            reports, evidence = self.pair(names)
            with patch("builtins.open", side_effect=AssertionError("no payload or research reads")):
                output, trace = dated_labels.project({}, reports, evidence)
            self.assertTrue(trace["changes"])
            for source in reports:
                for before, after in zip(
                    reports[source]["correlated"]["segments"], output[source]["correlated"]["segments"]
                ):
                    self.assertEqual([names[0]], after["observation_tickers"])
                    self.assertEqual(before, {**after, "observation_tickers": sorted(names)})

    def test_competing_actions_owners_routes_or_different_spans_preserve_ambiguity(self):
        for change in ("actions", "owner", "span", "route", "outside", "missing"):
            reports, evidence = self.pair()
            if change == "actions":
                evidence["MAPPED"] = {"corporate_actions": [{"symbol": "MAPPED", "process_date": "2030-01-03"}]}
            elif change == "owner":
                for segment in reports["MAPPED"]["correlated"]["segments"]:
                    segment["security_identity"] = {"security_id": "other"}
            elif change == "span":
                reports["MAPPED"]["correlated"]["segments"] = [
                    s for s in reports["MAPPED"]["correlated"]["segments"] if s["from"] != "2030-01-01"
                ]
            elif change == "route":
                evidence["HISTORIC"]["corporate_actions"].append(
                    {"action_type": "name_change", "old_symbol": "HISTORIC", "new_symbol": "MAPPED"}
                )
            elif change == "outside":
                evidence["HISTORIC"]["corporate_actions"][0]["ex_date"] = "2029-01-01"
            else:
                evidence = {}
            self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"], change)

    def test_action_supported_plan_does_not_extend_into_alias_only_future(self):
        from libs.alpaca_lineage_build.label_updates import continue_plan, encode_plan

        reports, evidence = self.pair()
        decision = dated_labels.plan(reports, evidence)
        actions = {name: list(facts.get("corporate_actions", ())) for name, facts in evidence.items()}
        previous = {
            name: {"dated_label_plan": encode_plan(decision), "dated_label_completion": {"actions": actions}}
            for name in reports
        }
        future = deepcopy(reports["MAPPED"])
        for segment in future["correlated"]["segments"]:
            segment.update({"from": "2030-02-01", "to": "2030-02-01", "observation_tickers": ["MAPPED"]})
        carried = continue_plan(previous, evidence, {"MAPPED": future})
        self.assertFalse(any(carried[0].values()))

    def test_ongoing_or_unknown_request_scope_does_not_extend_old_action_label(self):
        for request in ({}, {"start": "2030-01-04T00:00:00Z"}):
            reports, evidence = self.pair()
            reports["MAPPED"]["pricing_request"] = request
            self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"])


class RoundTripLabelTests(TestCase):
    def fixture(self):
        _inputs, reports, _evidence = fixture()
        reports = {"BROAD": deepcopy(reports["ALIAS"]), "TEMP": deepcopy(reports["OLD"])}
        for source, report in reports.items():
            for segment in report["correlated"]["segments"]:
                segment["historical_symbol"] = source
                if source == "TEMP":
                    segment["from"] = segment["to"] = "2030-01-02" if segment["row_start"] == 1 else "2030-01-03"
                segment["observation_tickers"] = (
                    ["BROAD", "TEMP"] if segment["from"] == "2030-01-02" else [source]
                )
        actions = [
            {
                "action_type": "name_change",
                "old_symbol": "BROAD",
                "new_symbol": "TEMP",
                "process_date": "2030-01-03",
            },
            {
                "action_type": "name_change",
                "old_symbol": "TEMP",
                "new_symbol": "BROAD",
                "process_date": "2030-01-04",
            },
            {"action_type": "reverse_split", "symbol": "BROAD", "ex_date": "2030-01-02"},
        ]
        return reports, {"BROAD": {"corporate_actions": actions}}

    def test_effective_split_and_roundtrip_resolve_only_existing_collision(self):
        reports, evidence = self.fixture()
        output, trace = dated_labels.project({}, reports, evidence)
        self.assertTrue(trace["changes"])
        for source, report in output.items():
            for before, after in zip(
                reports[source]["correlated"]["segments"], report["correlated"]["segments"]
            ):
                self.assertEqual(
                    ["TEMP"] if before["from"] == "2030-01-02" else before["observation_tickers"],
                    after["observation_tickers"],
                )
                self.assertEqual(before, {**after, "observation_tickers": before["observation_tickers"]})

    def test_competing_route_other_owner_missing_effective_start_and_early_return_preserve_collision(self):
        for change in ("route", "owner", "split", "return"):
            reports, evidence = self.fixture()
            actions = evidence["BROAD"]["corporate_actions"]
            if change == "route":
                actions.append({"action_type": "name_change", "old_symbol": "TEMP", "new_symbol": "OTHER"})
            elif change == "owner":
                for segment in reports["TEMP"]["correlated"]["segments"]:
                    segment["security_identity"] = {"security_id": "other"}
            elif change == "split":
                actions[-1]["ex_date"] = "2030-01-01"
            else:
                actions[1]["process_date"] = "2030-01-03"
            self.assertFalse(dated_labels.project({}, reports, evidence)[1]["changes"], change)
