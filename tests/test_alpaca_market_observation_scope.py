"""A temporary market reopens its dates while accepted peers stay closed."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import case

from libs.alpaca_end_of_day_security_correlation.market_occupancy import bind, project
from libs.alpaca_lineage_build import market_observations
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class MarketObservationScopeTests(TestCase):
    def prepared(self, *, hot=False):
        inputs, reports, evidence = case()
        reports = project(inputs, reports, evidence)
        for name, report in reports.items():
            for segment in report["correlated"]["segments"]:
                segment["from"] = segment["first_timestamp"][:10]
                segment["to"] = segment["last_timestamp"][:10]
                segment["row_count"] = segment["row_end"] - segment["row_start"] + 1
                segment["observation_selection"] = "selected" if name == "MAIN" else "superseded"
        original = deepcopy(reports)
        plan = bind(reports, evidence)
        keyed_reports, keyed_inputs, names = {}, {}, {}
        for kind in ("current_year", "current_month") if hot else ("current_year",):
            for name, report in reports.items():
                key = name, kind
                keyed_reports[key] = deepcopy(report)
                keyed_inputs[key] = inputs[name]
                names[key] = name
        prior = {
            name: {
                "anchor_month": "2026-10" if hot else "2026-11",
                "source_sha256": value.source_sha256,
                "observation_routes": {},
            }
            for name, value in inputs.items()
        }
        return inputs, keyed_inputs, keyed_reports, names, plan, prior, original

    def test_only_temporary_dates_enter_the_owning_numerical_selection(self):
        inputs, keyed_inputs, reports, names, plan, prior, original = self.prepared()

        class ActiveRowsOnly:
            def __init__(self, rows):
                self.rows = rows

            def __len__(self):
                return len(self.rows)

            def __getitem__(self, index):
                row = self.rows[index]
                if not "2026-10-02" <= row[0][:10] < "2026-10-07":
                    raise AssertionError("accepted frozen row accessed")
                return row

        physical = {}
        for value in inputs.values():
            value.views = {
                name: physical.setdefault(id(rows), ActiveRowsOnly(rows)) for name, rows in value.views.items()
            }
        select = market_observations.select_observations
        calls = []

        def capture(selected_inputs, selected_reports, **kwargs):
            calls.append(
                {
                    name: {view: len(rows) for view, rows in value.views.items()}
                    for name, value in selected_inputs.items()
                }
            )
            return select(selected_inputs, selected_reports, **kwargs)

        with patch.object(market_observations, "select_observations", side_effect=capture):
            revised, scope, bundles = market_observations.select_temporary(
                reports, keyed_inputs, names, plan, prior, lambda name: inputs[name].source
            )
        self.assertEqual(1, len(calls))
        self.assertEqual({"public": ["2026-10-02", "2026-10-05", "2026-10-06"]}, scope)
        self.assertEqual({3}, {count for views in calls[0].values() for count in views.values()})
        self.assertEqual(set(ADJUSTMENTS), set(bundles["public"]["2026-10-02"]))
        self.assertTrue(
            all(value["source"] == "ALT" for values in bundles["public"].values() for value in values.values())
        )
        for segment in revised["MAIN", "current_year"]["correlated"]["segments"]:
            self.assertEqual(
                "superseded" if segment["historical_symbol"] == "ALT" else "selected",
                segment["observation_selection"],
            )
        self.assertEqual(
            original["MAIN"]["correlated"]["segments"][0],
            revised["MAIN", "current_year"]["correlated"]["segments"][0],
        )

    def test_physical_year_hot_overlap_consumes_one_completed_decision(self):
        inputs, keyed_inputs, reports, names, plan, prior, _original = self.prepared(hot=True)
        select = market_observations.select_observations
        with patch.object(market_observations, "select_observations", wraps=select) as choose:
            revised, scope, _bundles = market_observations.select_temporary(
                reports, keyed_inputs, names, plan, prior, lambda name: inputs[name].source
            )
        self.assertEqual(1, choose.call_count)
        self.assertEqual(3, len(scope["public"]))
        for name in inputs:
            self.assertEqual(revised[name, "current_year"], revised[name, "current_month"])

    def test_sole_temporary_source_keeps_accepted_choice_without_rows_or_selection(self):
        inputs, keyed_inputs, reports, names, plan, prior, _original = self.prepared()
        reports = {key: value for key, value in reports.items() if names[key] == "ALT"}
        with patch.object(
            market_observations, "select_observations", side_effect=AssertionError("choice repeated")
        ):
            revised, scope, bundles = market_observations.select_temporary(
                reports, {}, names, plan, prior, lambda _name: self.fail("source reopened")
            )
        self.assertIs(reports, revised)
        self.assertFalse(scope)
        self.assertFalse(bundles)
