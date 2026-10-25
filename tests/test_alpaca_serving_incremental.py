"""Cold-process serving maintenance consumes the downloader's change decision."""

import unittest
from decimal import Decimal
from itertools import product
from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary, StandaloneReader, standalone
from alpaca_end_of_day._history_coverage import extend_coverage
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import storage as pricing
from libs.alpaca_pricing_download.append import append_entries
from libs.alpaca_pricing_download.changes import MAX_ANCESTORS, change_history, validate_history_changes
from libs.alpaca_pricing_download.contract import digest, json_bytes, timestamp
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_serving_catalogue import policy, work
from libs.alpaca_serving_catalogue.producer import maintain


class ChangeHandoffTests(unittest.TestCase):
    def test_replacement_span_survives_subsequent_append_and_unchanged_publications(self):
        first = {"history_changes": [], "publication": 0}
        token = digest(json_bytes(first))
        replacement = {"history_changes": change_history(first, {"from": "2020-01-02"}), "publication": 1}
        appended = {"history_changes": change_history(replacement, {"from": "2026-10-21"}), "publication": 2}
        retained = change_history(appended, None)
        self.assertEqual(
            next(item["changed_from"] for item in retained if item["source_sha256"] == token), "2020-01-02"
        )
        self.assertIsNone(retained[0]["changed_from"])
        validate_history_changes(retained)

    def test_ancestor_chain_is_bounded_without_claiming_older_unchanged_history(self):
        current = {"history_changes": [], "publication": 0}
        oldest = digest(json_bytes(current))
        for i in range(MAX_ANCESTORS + 1):
            current = {"history_changes": change_history(current, None), "publication": i + 1}
        validate_history_changes(current["history_changes"])
        self.assertEqual(len(current["history_changes"]), MAX_ANCESTORS)
        self.assertNotIn(oldest, {item["source_sha256"] for item in current["history_changes"]})

    def test_incremental_coverage_order_matches_full_coverage_for_disjoint_appends(self):
        subsets = [set(), {0}, {1}, {0, 1}]
        for parts in product(subsets, repeat=6):
            old = {name: values for name, values in zip(("A", "B", "C"), parts[:3]) if values}
            suffix = {
                name: {day + 2 for day in values} for name, values in zip(("A", "B", "C"), parts[3:]) if values
            }
            full = {name: old.get(name, set()) | suffix.get(name, set()) for name in old.keys() | suffix.keys()}
            self.assertEqual(extend_coverage(suffix, extend_coverage(old)), extend_coverage(full))


class IncrementalServingTests(NativeCase):
    def connect_group(self, *symbols):
        entries = lineage_storage.read_index(self.root)["symbols"]
        sources = {name: entries[name]["source_sha256"] for name in symbols}
        for symbol in symbols:

            def connect(receipt, boundaries):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {receipt["symbol"]})

            self.publish_change(connect, symbol)

    def connect_alias(self, *, winner="TEST"):
        entries = lineage_storage.read_index(self.root)["symbols"]
        sources = {name: entries[name]["source_sha256"] for name in ("TEST", "ALIAS")}
        for symbol in sources:

            def connect(receipt, boundaries):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {receipt["symbol"]})
                for boundary in boundaries.values():
                    for entry in boundary["adjustments"]:
                        for segment in entry["segments"]:
                            segment["security_identity"]["security_id"] = "e" * 64
                            segment["historical_symbol"] = "TEST"
                            segment["observation_tickers"] = [segment["historical_symbol"]]
                            segment["observation_selection"] = (
                                "selected" if receipt["symbol"] == winner else "superseded"
                            )

            self.publish_change(connect, symbol)

    def append(self, symbol, end, rows, *, lineage=True):
        root = self.store / "end_of_day/pricing" / symbol
        source = pricing.load_source(root, symbol)
        endpoints = pricing.read_validator(root, source)
        entry = {
            "symbol": symbol,
            "asof": source["latest_request"]["asof"],
            "end_exclusive": source["latest_request"]["end_exclusive"],
            "last_all": list(endpoints["all"][-1]),
        }
        client = BarsClient("test", "test")

        def response(params):
            start, stop = timestamp(params["start"]), timestamp(params["end"])
            return {"bars": {symbol: [row for row in rows if start <= timestamp(row["t"]) <= stop]}}

        with patch.object(client, "_get", side_effect=response):
            result = append_entries(
                self.store,
                [entry],
                end,
                client=client,
                workers=1,
                approved_generations={symbol: digest(json_bytes(source))},
            )
        self.assertEqual(result["failed"], 0, result)
        if lineage:
            result = run_lineage(self.store, [symbol], identity_client=IdentityClient({symbol: asset(symbol)}))
            self.assertEqual(result["failed"], 0, result)

    def assert_full_agreement(self, symbol):
        # A separate full evaluation is the oracle, never part of maintenance.
        with StandaloneReader(self.store)._open_group(symbol, include_unresolved=True) as group:
            expected = policy.evaluate(group)
        actual = self.catalogue_groups(symbol)[symbol]
        self.assertEqual(actual["securities"], expected["securities"])
        self.assertEqual(actual["decisions"], expected["decisions"])

    def rename_client(self):
        current = asset("NEW")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2023-12-18",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        return IdentityClient({"OLD": current, "NEW": current, "111111111": current}, [action])

    def build_rename(self):
        result = run_lineage(self.store, ["OLD", "NEW"], refresh=True, identity_client=self.rename_client())
        self.assertEqual(result["failed"], 0, result)

    def test_adjusted_only_group_observations_veto_previously_explained_gap(self):
        self.add_symbol("OLD", valid_bars("2023-12-01", "2023-12-13"))
        self.add_symbol("NEW", valid_bars("2023-12-18", "2023-12-22"))
        self.build_rename()
        data = self.bootstrap()
        public_id = data.search_securities("NEW")[0].public_id

        class AdjustedOnly(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                return [] if adjustment == "raw" else super().fetch(symbol, adjustment, *args, **kwargs)

        download_pricing(
            self.store, "AUX", "2026-10-14", client=AdjustedOnly(valid_bars("2023-12-14", "2023-12-15"))
        )
        result = run_lineage(self.store, ["AUX"], identity_client=IdentityClient({"AUX": asset("AUX")}))
        self.assertEqual(result["failed"], 0, result)
        self.connect_group("OLD", "NEW", "AUX")
        opened, original = [], Path.read_bytes

        def read(path):
            if path.suffix == ".gz":
                opened.append(path)
                self.assertTrue(path.is_relative_to(self.store / "end_of_day/pricing/AUX"), path)
            return original(path)

        with (
            patch.object(Path, "read_bytes", read),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("unchanged numeric check")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(len(opened), 1, "physical adjustment aliases should share the selected gap payload")
        group = self.catalogue_groups("AUX")["AUX"]
        decision = next(item for item in group["decisions"] if item["public_id"] == public_id)
        self.assertEqual(decision["reasons"], ["unexplained_missing_sessions"])
        record = next(
            item for item in group["history_validation"]["prefix_records"] if item["public_id"] == public_id
        )
        self.assertEqual(record["source_sessions_in_gaps"], ["2023-12-14"])
        self.assert_full_agreement("AUX")

    def test_adjusted_alias_does_not_change_raw_rename_continuity_on_append_or_retry(self):
        self.add_symbol("OLD", valid_bars("2023-12-01", "2023-12-13"))
        self.add_symbol("NEW", [])
        self.build_rename()

        def alias(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if view["adjustment"] == "all":
                        for segment in view["segments"]:
                            segment["historical_symbol"] = "ALIAS"
                            segment["observation_tickers"] = [segment["historical_symbol"]]

        self.publish_change(alias, "OLD")
        self.bootstrap()
        suffix = valid_bars("2023-12-18", "2023-12-22")
        download_pricing(self.store, "NEW", "2026-10-14", client=FakeClient(suffix), refresh_all=True)
        self.build_rename()
        self.publish_change(alias, "OLD")
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], len(suffix) * 8)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assert_full_agreement("NEW")
        group = self.catalogue_groups("NEW")["NEW"]
        self.assertTrue(group["decisions"][0]["eligible"])
        record = group["history_validation"]["prefix_records"][0]
        self.assertEqual(record["gap_bars"][0]["tickers"], ["OLD"])
        self.assertEqual(record["last_raw_bar"]["tickers"], ["NEW"])
        self.assertIn("ALIAS", group["securities"][0]["tickers"])
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new"]), "NEW")
        with patch.object(standalone, "_rows", side_effect=AssertionError("unchanged history reopened")):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        self.assert_full_agreement("NEW")

    def test_one_identity_relabel_preserves_other_identity_checks_in_the_same_group(self):
        self.add_symbol("OTHER")
        self.connect_group("TEST", "OTHER")
        self.bootstrap()

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["historical_symbol"] = "OLD"
                        segment["observation_tickers"] = [segment["historical_symbol"]]

        self.publish_change(relabel)
        rows = standalone._rows

        def selected_rows(root, *args, **kwargs):
            self.assertEqual(root.name, "TEST", "unaffected identity prices reopened")
            return rows(root, *args, **kwargs)

        with patch.object(standalone, "_rows", side_effect=selected_rows):
            result = maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["full_history_evaluations"], 1)
        self.assertEqual(result["checked_bars"], 8 * len(self.bars))
        self.assert_full_agreement("OTHER")

    def test_one_source_replacement_preserves_independent_identity_checks(self):
        self.add_symbol("OTHER")
        self.connect_group("TEST", "OTHER")
        self.bootstrap()
        changed = [{**row, **dict.fromkeys(("o", "h", "l", "c", "vw"), Decimal(50))} for row in self.bars]
        result = download_pricing(self.store, "TEST", "2026-10-15", client=FakeClient(changed))
        self.assertEqual(result["mode"], "full_refresh")
        self.append("OTHER", "2026-10-15", [], lineage=False)
        result = run_lineage(
            self.store,
            ["TEST"],
            identity_client=IdentityClient({name: asset(name) for name in ("TEST", "OTHER")}),
        )
        self.assertEqual(result["failed"], 0, result)
        rows = standalone._rows

        def selected_rows(root, *args, **kwargs):
            self.assertEqual(root.name, "TEST", "unrelated replacement invalidated completed history")
            return rows(root, *args, **kwargs)

        with patch.object(standalone, "_rows", side_effect=selected_rows):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["full_history_evaluations"], 1)
        self.assertEqual(result["checked_bars"], 8 * len(self.bars))
        self.assert_full_agreement("OTHER")

    def test_reuse_is_per_identity_even_when_identities_share_one_pricing_source(self):
        self.add_symbol("REUSED", valid_bars("2026-09-28", "2026-10-14"))

        def split_identity(receipt, boundaries):
            for view in boundaries["current_month"]["adjustments"]:
                for segment in view["segments"]:
                    segment["security_identity"]["security_id"] = "b" * 64

        self.publish_change(split_identity, "REUSED")
        self.bootstrap()

        def relabel(receipt, boundaries):
            for view in boundaries["current_year"]["adjustments"]:
                for segment in view["segments"]:
                    segment["historical_symbol"] = "EARLIER"
                    segment["observation_tickers"] = [segment["historical_symbol"]]

        self.publish_change(relabel, "REUSED")
        with patch.object(policy, "_bar_reasons", wraps=policy._bar_reasons) as checks:
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["full_history_evaluations"], 1)
        self.assertEqual({call.args[0]["session"][:7] for call in checks.call_args_list}, {"2026-09"})
        self.assert_full_agreement("REUSED")

    def test_membership_merge_and_split_reuse_independent_completed_checks(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        self.connect_group("TEST", "OTHER")
        with patch.object(standalone, "_rows", side_effect=AssertionError("unchanged history reopened")):
            result = maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 2)
        self.assertEqual(result["checked_bars"], 0)
        self.assert_full_agreement("OTHER")
        for symbol in ("TEST", "OTHER"):
            self.connect_group(symbol)
        with patch.object(standalone, "_rows", side_effect=AssertionError("unchanged history reopened")):
            result = maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 2)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(work.read(self.store), {})
        self.assert_full_agreement("TEST")
        self.assert_full_agreement("OTHER")

    def test_changed_gap_source_refreshes_presence_without_rechecking_retained_numeric_history(self):
        missing_day = "2026-10-06"
        with_gap = [bar for bar in self.bars if bar["t"][:10] != missing_day]
        self.add_symbol("GAP", with_gap)
        self.add_symbol("OTHER", with_gap)
        self.connect_group("GAP", "OTHER")
        self.bootstrap()
        public_id = StandaloneReader(self.store).get_history("GAP")["security"]["public_id"]
        for filled in (True, False):
            with self.subTest(filled=filled):
                bars = self.bars if filled else with_gap
                result = download_pricing(
                    self.store, "OTHER", "2026-10-14", client=FakeClient(bars), refresh_all=True
                )
                self.assertEqual(result["mode"], "full_refresh")
                result = run_lineage(
                    self.store,
                    ["OTHER"],
                    identity_client=IdentityClient({name: asset(name) for name in ("GAP", "OTHER")}),
                )
                self.assertEqual(result["failed"], 0, result)
                self.connect_group("GAP", "OTHER")
                with patch.object(policy, "_bar_reasons", wraps=policy._bar_reasons) as checks:
                    result = maintain(self.store)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["reused_history_prefixes"], 1)
                self.assertEqual(result["full_history_evaluations"], 1)
                self.assertEqual(
                    {tuple(call.args[0]["tickers"]) for call in checks.call_args_list}, {("OTHER",)}
                )
                current = self.catalogue_groups("GAP")["GAP"]["history_validation"]
                retained = next(row for row in current["prefix_records"] if row["public_id"] == public_id)
                self.assertEqual(retained["source_sessions_in_gaps"], [missing_day] if filled else [])
                self.assert_full_agreement("GAP")

    def test_daily_append_checks_only_new_rows_without_opening_closed_payloads(self):
        self.add_symbol("LONG", valid_bars("2020-01-02", "2026-10-14"))
        self.bootstrap()
        self.append("LONG", "2026-10-15", valid_bars("2026-10-14", "2026-10-15"))
        original = Path.open
        payloads = []

        def opened(path, *args, **kwargs):
            if str(path).endswith(".csv.gz"):
                payloads.append(path)
                self.assertEqual(path.parent.name, "Oct", "closed history reopened")
            return original(path, *args, **kwargs)

        with (
            patch.object(Path, "open", opened),
            patch.object(policy, "_bar_reasons", wraps=policy._bar_reasons) as checks,
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assertEqual(result["checked_bars"], 8)
        self.assertEqual(checks.call_count, 8)
        self.assertEqual({call.args[0]["session"] for call in checks.call_args_list}, {"2026-10-14"})
        self.assertEqual(len(payloads), 1, "physical aliases should share one payload decode")
        self.assert_full_agreement("LONG")

    def test_month_and_year_rollovers_retain_completed_validation(self):
        for symbol, prior_end, end in (
            ("MONTH", "2026-11-01", "2026-11-03"),
            ("YEAR", "2027-01-01", "2027-01-05"),
        ):
            with self.subTest(symbol=symbol):
                self.add_symbol(symbol, valid_bars("2025-01-02", prior_end), end=prior_end)
                self.bootstrap()
                self.append(symbol, end, valid_bars(prior_end, end))
                original = standalone._rows

                def rows(root, source, kind, *args, **kwargs):
                    self.assertEqual(kind, "current_month", "rollover invalidated approved rows")
                    return original(root, source, kind, *args, **kwargs)

                with patch.object(standalone, "_rows", side_effect=rows):
                    result = maintain(self.store)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["full_history_evaluations"], 0)
                self.assertEqual(result["checked_bars"], 8)
                self.assert_full_agreement(symbol)

    def test_two_pricing_publications_before_serving_reuse_original_checkpoint(self):
        self.bootstrap()
        self.append("TEST", "2026-10-15", valid_bars("2026-10-14", "2026-10-15"))
        self.append("TEST", "2026-10-16", valid_bars("2026-10-15", "2026-10-16"))
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 16)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assert_full_agreement("TEST")

    def test_metadata_change_reuses_checks_without_opening_any_price_payload(self):
        self.bootstrap()

        def rename(receipt, boundaries):
            for lookup in receipt["evidence"]["asset_lookups"]:
                if lookup.get("asset"):
                    lookup["asset"]["name"] = "Observed new name"

        self.publish_change(rename)
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("price reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("known bar rechecked")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(DataLibrary(store=self.store).list_securities()[0].names, ("Observed new name",))
        self.assert_full_agreement("TEST")

    def test_downloader_endpoint_replacement_invalidates_history_validation(self):
        self.bootstrap()
        changed = [
            {**row, "o": Decimal(50), "h": Decimal(50), "l": Decimal(50), "c": Decimal(50), "vw": Decimal(50)}
            for row in self.bars
        ]
        result = download_pricing(
            self.store,
            "TEST",
            "2026-10-15",
            client=FakeClient([*changed, *valid_bars("2026-10-14", "2026-10-15")]),
        )
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(
            run_lineage(self.store, ["TEST"], identity_client=IdentityClient({"TEST": asset("TEST")}))["failed"],
            0,
        )
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 0)
        self.assertEqual(result["checked_bars"], 8 * (len(self.bars) + 1))
        self.assert_full_agreement("TEST")

    def test_append_checks_continuity_seam_and_new_numeric_failure(self):
        for symbol, rows, reason in (
            ("GAP", valid_bars("2026-10-15", "2026-10-16"), "unexplained_missing_sessions"),
            ("BAD", [{**row, "v": -1} for row in valid_bars("2026-10-14", "2026-10-16")], "volume_domain"),
        ):
            self.add_symbol(symbol)
            self.bootstrap()
            self.append(symbol, "2026-10-16", rows)
            result = maintain(self.store)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["full_history_evaluations"], 0)
            decision = self.catalogue_groups(symbol)[symbol]["decisions"][0]
            self.assertIn(reason, decision["reasons"])
            self.assert_full_agreement(symbol)

    def test_published_ownership_change_prevents_prefix_reuse(self):
        self.bootstrap()

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["historical_symbol"] = "OLD"
                        segment["observation_tickers"] = [segment["historical_symbol"]]

        self.publish_change(relabel)
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 0)
        self.assert_full_agreement("TEST")

    def test_overlapping_sources_reuse_only_when_historical_selection_is_preserved(self):
        def low(rows):
            return [{**row, **dict.fromkeys(("o", "h", "l", "c", "vw"), Decimal(50))} for row in rows]

        self.add_symbol("ALIAS", low(self.bars))
        self.connect_alias()
        self.bootstrap()
        for symbol in ("TEST", "ALIAS"):
            new = valid_bars("2026-10-14", "2026-10-15")
            self.append(symbol, "2026-10-15", low(new) if symbol == "ALIAS" else new, lineage=False)
        identities = IdentityClient({name: asset(name) for name in ("TEST", "ALIAS")})
        result = run_lineage(self.store, ["TEST", "ALIAS"], identity_client=identities)
        self.assertEqual(result["failed"], 0, result)
        self.connect_alias()
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["checked_bars"], 8)
        self.assert_full_agreement("ALIAS")
        before = DataLibrary(store=self.store).get_history("e" * 64, adjustment="all").bars[0].close

        # A new producer decision selects ALIAS for all eight views. Serving
        # must invalidate the old prefix rather than repeating source selection.
        self.append("ALIAS", "2026-10-16", low(valid_bars("2026-10-15", "2026-10-16")), lineage=False)
        self.append("TEST", "2026-10-16", [], lineage=False)
        result = run_lineage(self.store, ["TEST", "ALIAS"], identity_client=identities)
        self.assertEqual(result["failed"], 0, result)
        self.connect_alias(winner="ALIAS")
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 0)
        self.assertEqual(result["full_history_evaluations"], 1)
        after = DataLibrary(store=self.store).get_history("e" * 64, adjustment="all").bars[0].close
        self.assertNotEqual(before, after)
        self.assertEqual(after, Decimal(50))
        self.assert_full_agreement("ALIAS")

    def test_completed_negative_price_checks_are_also_reused(self):
        self.add_symbol("BAD", [{**row, "v": -1} for row in self.bars])
        self.bootstrap()
        self.append("BAD", "2026-10-15", valid_bars("2026-10-14", "2026-10-15"))
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 8)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assertFalse(DataLibrary(store=self.store).search_securities("BAD"))
        self.assert_full_agreement("BAD")

    def test_completed_incompatible_view_decision_survives_append_without_rechecking(self):
        class MissingSession(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                rows = super().fetch(symbol, adjustment, *args, **kwargs)
                return [row for row in rows if row["t"][:10] != "2026-10-06"] if adjustment == "all" else rows

        download_pricing(self.store, "VIEWS", "2026-10-14", client=MissingSession(self.bars))
        self.assertEqual(
            run_lineage(self.store, ["VIEWS"], identity_client=IdentityClient({"VIEWS": asset("VIEWS")}))[
                "failed"
            ],
            0,
        )
        self.bootstrap()
        self.append("VIEWS", "2026-10-15", valid_bars("2026-10-14", "2026-10-15"))
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 8)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assert_full_agreement("VIEWS")

    def test_filling_a_short_view_reconsiders_compatibility_over_all_validated_sessions(self):
        for short_view, symbol in (("raw", "RAWVIEWS"), ("all", "ALLVIEWS")):
            with self.subTest(short_view=short_view):

                class ShortView(FakeClient):
                    def fetch(self, symbol, adjustment, *args, **kwargs):
                        rows = super().fetch(symbol, adjustment, *args, **kwargs)
                        return (
                            [row for row in rows if row["t"][:10] != "2026-10-13"]
                            if adjustment == short_view
                            else rows
                        )

                download_pricing(self.store, symbol, "2026-10-14", client=ShortView(self.bars))
                result = run_lineage(
                    self.store, [symbol], identity_client=IdentityClient({symbol: asset(symbol)})
                )
                self.assertEqual(result["failed"], 0, result)
                self.bootstrap()
                prior = self.catalogue_groups(symbol)[symbol]
                self.assertEqual(prior["decisions"][0]["reasons"], ["incompatible_sessions"])
                self.assertEqual(
                    prior["history_validation"]["prefix_records"][0]["validated_through"], "2026-10-13"
                )
                independent = self.catalogue_groups("TEST")["TEST"]

                published = download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.bars))
                self.assertEqual(published["mode"], "update")
                result = run_lineage(
                    self.store, [symbol], identity_client=IdentityClient({symbol: asset(symbol)})
                )
                self.assertEqual(result["failed"], 0, result)
                with patch.object(policy, "_bar_reasons", wraps=policy._bar_reasons) as checks:
                    result = maintain(self.store)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["full_history_evaluations"], 1)
                self.assertEqual({tuple(call.args[0]["tickers"]) for call in checks.call_args_list}, {(symbol,)})
                self.assertEqual(self.catalogue_groups("TEST")["TEST"], independent)
                self.assertTrue(self.catalogue_groups(symbol)[symbol]["decisions"][0]["eligible"])
                self.assert_full_agreement(symbol)

    def test_interrupted_acknowledgement_reuses_committed_append_checks(self):
        from test_alpaca_serving_catalogue import Interrupted

        from libs.alpaca_serving_catalogue import work

        self.bootstrap()
        self.append("TEST", "2026-10-15", valid_bars("2026-10-14", "2026-10-15"))
        with patch.object(work, "acknowledge", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store)
        with (
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed append rechecked")),
            patch.object(
                StandaloneReader, "_open_group", side_effect=AssertionError("completed inputs reopened")
            ),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_groups"], 1)
