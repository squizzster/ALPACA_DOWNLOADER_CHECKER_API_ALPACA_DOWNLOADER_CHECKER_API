"""The doorman routes input changes; only the existing pipeline assigns lineage."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import metadata, pipeline, pricing
from libs.alpaca_lineage_build.resume import requires_lineage, stable_evidence
from libs.alpaca_lineage_build.transform import ASSIGNMENT_FIELDS
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def dividend(identifier, symbol, cusip, day, *, processed=None):
    return {
        "action_type": "cash_dividend",
        "id": identifier,
        "symbol": symbol,
        "cusip": cusip,
        "process_date": processed or day,
        "ex_date": day,
        "record_date": day,
        "payable_date": day,
        "rate": "0.1",
    }


class LineageRequiredTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def decisions(self, symbol="GOOG"):
        return {
            mode: [
                {key: value for key, value in segment.items() if key in ASSIGNMENT_FIELDS}
                for segment in self.case.assignments(symbol, adjustment=mode)
                for _ in range(segment["row_count"])
            ]
            for mode in ADJUSTMENTS
        }

    def assert_full_parity(self, client, symbols=("GOOG",)):
        accepted = {symbol: self.decisions(symbol) for symbol in symbols}
        self.case.run_build(symbols=list(symbols), client=client, refresh=True)
        self.assertEqual({symbol: self.decisions(symbol) for symbol in symbols}, accepted)

    def test_distribution_and_split_append_keeps_old_payloads_closed_and_matches_full_owner(self):
        case = self.case
        identity = fixture.asset("GOOG")
        client = fixture.IdentityClient(
            {"GOOG": identity, "111111111": identity},
            [
                dividend("distribution", "GOOG", "111111111", "2026-10-01"),
                {
                    "action_type": "forward_split",
                    "id": "split",
                    "symbol": "GOOG",
                    "cusip": "111111111",
                    "process_date": "2020-08-31",
                    "ex_date": "2020-08-31",
                    "record_date": "2020-08-24",
                    "payable_date": "2020-08-28",
                    "due_bill_redemption_date": "2020-09-01",
                    "new_rate": 4,
                    "old_rate": 1,
                },
            ],
        )
        bars = fixture.calendar_bars("2016-01-01", "2026-10-14")
        case.download(bars=bars)
        case.run_build(client=client)
        prior = case.receipt()
        case.download(end="2026-10-16", bars=bars + fixture.calendar_bars("2026-10-14", "2026-10-16"))
        read = Path.read_bytes

        def selected(path):
            if path.name in {"completed_years.manifest.json", "year.manifest.json"}:
                self.fail(f"closed assignments reopened: {path}")
            if path.suffix == ".gz" and path.parent.name != "Oct":
                self.fail(f"closed prices reopened: {path}")
            return read(path)

        with (
            patch.object(Path, "read_bytes", selected),
            patch.object(pricing, "load_published_views", side_effect=AssertionError("full price history read")),
            patch.object(pipeline, "correlate", wraps=pipeline.correlate) as owner,
            patch.object(pipeline, "event", wraps=pipeline.event) as events,
        ):
            result = case.run_build(client=client)["results"][0]
        self.assertEqual(owner.call_count, 1)
        self.assertEqual(result["pricing_rows_processed"], 16)
        self.assertEqual(result["historical_rows_reused"], prior["source_rows"])
        self.assertTrue(result["reused_closed_boundaries"])
        gate = [call.kwargs for call in events.call_args_list if call.args == ("lineage_required",)][0]
        self.assertTrue(gate["required"])
        self.assertFalse(gate["retained_history_required"])
        self.assertEqual(gate["scope"], "added_sessions")
        self.assert_full_parity(client)

    def test_material_action_change_selects_full_history_before_owner_execution(self):
        case = self.case
        identity = fixture.asset("GOOG")
        action = dividend("distribution", "GOOG", "111111111", "2026-10-01")
        client = fixture.IdentityClient({"GOOG": identity, "111111111": identity}, [action])
        bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download(bars=bars)
        case.run_build(client=client)
        case.download(end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        client.actions[0]["rate"] = "0.2"
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as owner:
            result = case.run_build(client=client)["results"][0]
        self.assertEqual(owner.call_count, 1)
        self.assertFalse(result["reused_closed_boundaries"])
        self.assertEqual(result["historical_rows_reused"], 0)
        self.assertEqual(result["pricing_rows_processed"], (len(bars) + 1) * 8)
        self.assert_full_parity(client)

    def test_changed_exact_endpoint_invalidates_the_dependent_cusip_mapping(self):
        case = self.case
        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        action = dividend("same-action", "GOOG", "111111111", "2026-10-01")
        old, new = fixture.asset("GOOG", "old"), fixture.asset("GOOG", "new")
        case.download(bars=bars)
        case.run_build(client=fixture.IdentityClient({"GOOG": old, "111111111": old}, [action]))
        case.download(end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        client = fixture.IdentityClient({"GOOG": new, "111111111": new}, [action])
        with patch.object(client, "get_asset_lookup", wraps=client.get_asset_lookup) as lookups:
            case.run_build(client=client)
        self.assertEqual([call.args[0] for call in lookups.call_args_list], ["GOOG", "111111111"])
        self.assertEqual(case.receipt()["unresolved_rows"], 0)
        self.assertEqual(case.receipt()["correlated_rows"], 80)
        self.assert_full_parity(client)

    def _assert_late_canonical_endpoint_refresh(self, *, include_dividend):
        case = self.case
        rename = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2025-12-01",
            "old_symbol": "A",
            "new_symbol": "B",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
        }
        actions = [rename]
        if include_dividend:
            actions.append(dividend("dividend", "C", "111111111", "2026-10-01"))

        def client(identity):
            canonical = fixture.asset("C", identity)
            return fixture.IdentityClient(
                {
                    "A": fixture.asset("A"),
                    "B": fixture.asset("B"),
                    "C": canonical,
                    "111111111": canonical,
                    "222222222": canonical,
                },
                actions,
            )

        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        for symbol in ("A", "B", "C"):
            case.download(symbol=symbol, bars=bars)
        case.run_build(symbols=["A"], client=client("old"))
        self.assertIn("C", case.receipt("A")["evidence"]["queried_symbols"])
        for symbol in ("A", "B", "C"):
            case.download(symbol=symbol, end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        current = client("new")
        with patch.object(current, "get_asset_lookup", wraps=current.get_asset_lookup) as lookups:
            case.run_build(symbols=["A"], client=current)
        acquired = [call.args[0] for call in lookups.call_args_list]
        self.assertEqual(sorted(acquired), ["111111111", "222222222", "A", "B", "C"])
        receipts = {symbol: case.receipt(symbol) for symbol in ("A", "B", "C")}
        for symbol in ("A", "B"):
            receipt = receipts[symbol]
            mapping = {
                item["lookup"]: (item.get("asset") or {}).get("id")
                for item in receipt["evidence"]["asset_lookups"]
            }
            self.assertEqual([mapping[key] for key in ("C", "111111111", "222222222")], ["new"] * 3)
            self.assertIn("C", receipt["evidence"]["queried_symbols"])
            self.assertNotIn("old", receipt["asset_ids"])
        self.assert_full_parity(current, ("A", "B", "C"))
        for symbol, prior in receipts.items():
            self.assertTrue(stable_evidence(prior["evidence"], case.receipt(symbol)["evidence"]))
            self.assertEqual(prior["asset_ids"], case.receipt(symbol)["asset_ids"])

    def test_late_canonical_endpoint_refreshes_both_retained_cusips(self):
        self._assert_late_canonical_endpoint_refresh(include_dividend=False)

    def test_late_canonical_endpoint_restart_keeps_traversal_and_mappings_coherent(self):
        self._assert_late_canonical_endpoint_refresh(include_dividend=True)

    def test_first_rows_reach_existing_ownership_fallback(self):
        case = self.case
        current = fixture.asset("GOOG", "current")
        former = {
            **fixture.asset("GOOG", "former"),
            "status": "inactive",
            "tradable": False,
            "name": "Former company",
        }
        client = fixture.IdentityClient(
            {"GOOG": current, "111111111": former, "222222222": current},
            [
                dividend("former", "GOOG", "111111111", "2020-01-02"),
                dividend("current", "GOOG", "222222222", "2026-10-13"),
            ],
        )
        case.download(bars=[])
        case.run_build(client=client)
        case.download(end="2026-10-15", bars=[fixture.bar("2026-10-14")])
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as owner:
            case.run_build(client=client)
        self.assertEqual(owner.call_count, 1)
        self.assertEqual(case.receipt()["correlated_rows"], 8)
        self.assertEqual(case.receipt()["unresolved_rows"], 0)
        self.assert_full_parity(client)

    def test_later_member_lookup_refreshes_earlier_group_evidence(self):
        case = self.case
        goog, old, new = (
            fixture.asset("GOOG", "go"),
            fixture.asset("OTHER", "old"),
            fixture.asset("OTHER", "new"),
        )
        action = {
            "action_type": "cash_merger",
            "id": "same",
            "process_date": "2026-10-01",
            "effective_date": "2026-10-01",
            "acquiree_symbol": "GOOG",
            "acquiree_cusip": "111111111",
            "acquirer_symbol": "OTHER",
            "acquirer_cusip": "222222222",
            "cash": "1",
        }
        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        before = fixture.IdentityClient(
            {"GOOG": goog, "111111111": goog, "OTHER": old, "222222222": old}, [action]
        )
        for symbol in ("GOOG", "OTHER"):
            case.download(symbol=symbol, bars=bars)
        case.run_build(symbols=["GOOG"], client=before)
        for symbol in ("GOOG", "OTHER"):
            case.download(symbol=symbol, end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        current = fixture.IdentityClient(
            {"GOOG": goog, "111111111": goog, "OTHER": new, "222222222": new}, [action]
        )
        with patch.object(current, "get_asset_lookup", wraps=current.get_asset_lookup) as lookups:
            case.run_build(symbols=["GOOG"], client=current)
        self.assertEqual([call.args[0] for call in lookups.call_args_list], ["GOOG", "OTHER", "222222222"])
        receipts = {symbol: case.receipt(symbol) for symbol in ("GOOG", "OTHER")}
        for receipt in receipts.values():
            mapping = {
                lookup["lookup"]: (lookup.get("asset") or {}).get("id")
                for lookup in receipt["evidence"]["asset_lookups"]
            }
            self.assertEqual(mapping["222222222"], "new")
            self.assertNotIn("old", receipt["asset_ids"])
        self.assert_full_parity(current, ("GOOG", "OTHER"))
        for symbol, prior in receipts.items():
            self.assertEqual(prior["evidence"], case.receipt(symbol)["evidence"])

    def test_new_donor_rows_reach_existing_foreign_occupancy_reconciliation(self):
        case = self.case
        current, foreign = fixture.asset("GOOG", "current"), fixture.asset("OTHER", "foreign")
        client = fixture.IdentityClient(
            {"GOOG": current, "111111111": current, "OTHER": foreign, "222222222": foreign},
            [
                dividend("before", "GOOG", "111111111", "2026-10-01", processed="2026-10-01"),
                dividend("foreign-copy", "GOOG", "222222222", "2026-10-14", processed="2026-10-01"),
                dividend("after", "GOOG", "111111111", "2026-10-15", processed="2026-10-01"),
                dividend("owner", "OTHER", "222222222", "2026-10-14", processed="2026-10-01"),
            ],
        )
        bars = {
            symbol: [
                row
                for row in fixture.calendar_bars("2026-10-01", "2026-10-14")
                if symbol != "GOOG" or row["t"][:10] != "2026-10-07"
            ]
            for symbol in ("GOOG", "OTHER")
        }
        for symbol in bars:
            case.download(symbol=symbol, bars=bars[symbol], refresh_all=True, mapping_asof="2026-10-13")
        case.run_build(symbols=["GOOG"], client=client)
        self.assertEqual(case.receipt()["unresolved_rows"], 64)
        for symbol in bars:
            case.download(
                symbol=symbol,
                end="2026-10-16",
                bars=bars[symbol] + fixture.calendar_bars("2026-10-14", "2026-10-16"),
            )
        case.run_build(symbols=["GOOG"], client=client)
        self.assertEqual(case.receipt()["correlated_rows"], 80)
        self.assertEqual(case.receipt()["unresolved_rows"], 0)
        self.assert_full_parity(client, ("GOOG", "OTHER"))

    def test_normal_cutoff_advance_preserves_accepted_structural_history(self):
        case = self.case
        old, other = fixture.asset("GOOG", "old"), fixture.asset("OTHER", "other")
        action = {
            "action_type": "cash_merger",
            "id": "terminal",
            "process_date": "2026-10-13",
            "effective_date": "2026-10-15",
            "acquiree_symbol": "GOOG",
            "acquiree_cusip": "111111111",
            "acquirer_symbol": "OTHER",
            "acquirer_cusip": "222222222",
            "cash": "1",
        }
        client = fixture.IdentityClient(
            {"GOOG": old, "111111111": old, "OTHER": other, "222222222": other}, [action]
        )
        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        case.download(bars=bars)
        case.download(symbol="OTHER", bars=[])
        case.run_build(client=client)
        accepted = {s: self.decisions(s) for s in ("GOOG", "OTHER")}
        case.download(end="2026-10-16", bars=bars)
        case.download(symbol="OTHER", end="2026-10-16", bars=[])
        with patch.object(pipeline, "event", wraps=pipeline.event) as events:
            case.run_build(client=client)
        self.assertEqual(
            [call.kwargs["required"] for call in events.call_args_list if call.args == ("lineage_required",)],
            [False],
        )
        self.assertEqual({s: self.decisions(s) for s in accepted}, accepted)

        def ownership():
            # Full replay can choose another equally valid rule after the known
            # terminal date. That does not invalidate the completed historical
            # owner or permit replay merely to replace its original provenance.
            return {
                s: {
                    a: [
                        {k: v for k, v in row.items() if k not in {"correlation_rule", "correlation_method"}}
                        for row in rows
                    ]
                    for a, rows in self.decisions(s).items()
                }
                for s in accepted
            }

        before = ownership()
        case.run_build(symbols=list(accepted), client=client, refresh=True)
        self.assertEqual(ownership(), before)

    def test_new_historical_rows_get_the_owning_series_identity(self):
        case = self.case
        client = fixture.IdentityClient(assets={})
        case.download(bars=[])
        case.run_build(client=client)
        case.download(end="2026-10-15", bars=[fixture.bar("2026-10-14")])
        case.run_build(client=client)
        segment = case.assignments()[0]
        self.assertEqual(segment["reason_codes"], ["historical_market_data_without_alpaca_asset"])
        self.assertIsNotNone(segment["market_data_series_identity"])
        self.assert_full_parity(client)

    def test_unchanged_accepted_inputs_skip_every_lineage_stage_and_payload(self):
        case = self.case
        client = fixture.IdentityClient(assets={})
        case.download()
        case.run_build(client=client)
        previous = self.decisions()
        requests = client.request_count
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("lineage on unchanged inputs")),
            patch.object(pricing, "load_published_views", side_effect=AssertionError("price payload read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price payload read")),
            patch.object(pipeline, "event", wraps=pipeline.event) as events,
        ):
            case.run_build(symbols=["GOOG"], client=client)
        self.assertEqual(
            [call.kwargs["required"] for call in events.call_args_list if call.args == ("lineage_required",)],
            [False],
        )
        self.assertEqual(client.request_count, requests)
        self.assertEqual(self.decisions(), previous)

    def test_new_evidence_fields_participate_without_a_second_rule_map(self):
        evidence = {"asset_lookups": [], "symbol_disposition": {"kind": "no_market_data"}}
        current = copy.deepcopy(evidence)
        current["symbol_disposition"]["kind"] = "historical_market_data_only"
        self.assertFalse(stable_evidence(evidence, current))
        current = {**evidence, "new_owner_fact": True}
        self.assertFalse(stable_evidence(evidence, current))
        self.assertTrue(requires_lineage({}, {}, pricing_unchanged=False))

    def test_dependency_bookkeeping_changes_do_not_select_lineage_or_mutate_evidence(self):
        case = self.case
        case.download()
        case.run_build()
        receipt = case.receipt()
        for before, after in (
            (None, {"111111111": {"GOOG": "GOOG-id"}}),
            ({"111111111": {"GOOG": "old-observation"}}, {"111111111": {"GOOG": "GOOG-id"}}),
            ({"111111111": {"GOOG": "GOOG-id"}}, None),
        ):
            prior, evidence = copy.deepcopy(receipt), copy.deepcopy(receipt["evidence"])
            for document, value in ((prior["evidence"], before), (evidence, after)):
                document.pop("mapping_endpoint_observations", None)
                if value is not None:
                    document["mapping_endpoint_observations"] = value
            original = copy.deepcopy((prior, evidence))
            for append in (False, True):
                with self.subTest(before=before, after=after, append=append):
                    self.assertFalse(
                        requires_lineage(
                            {"GOOG": prior}, {"GOOG": evidence}, pricing_unchanged=True, append=append
                        )
                    )
            self.assertEqual((prior, evidence), original)
            self.assertTrue(requires_lineage({"GOOG": prior}, {"GOOG": evidence}, pricing_unchanged=False))
            self.assertTrue(
                requires_lineage({"GOOG": prior}, {"GOOG": evidence}, pricing_unchanged=True, refresh=True)
            )

    def test_dependency_bookkeeping_does_not_hide_changed_or_new_evidence(self):
        case = self.case
        identity = fixture.asset("GOOG")
        action = dividend("distribution", "GOOG", "111111111", "2026-10-01")
        case.download()
        case.run_build(client=fixture.IdentityClient({"GOOG": identity, "111111111": identity}, [action]))
        prior = case.receipt()
        prior["evidence"].pop("mapping_endpoint_observations", None)
        evidence = copy.deepcopy(prior["evidence"])
        evidence["mapping_endpoint_observations"] = {"111111111": {"GOOG": "GOOG-id"}}
        for change in ("lookup_uuid", "action", "occupant", "new_fact"):
            current = copy.deepcopy(evidence)
            if change == "lookup_uuid":
                lookup = next(row for row in current["asset_lookups"] if row["lookup_kind"] == "cusip")
                lookup["asset"]["id"] = "new-owner"
            elif change == "action":
                current["corporate_actions"][0]["rate"] = "0.2"
            elif change == "occupant":
                current.setdefault("input_symbol_occupants", []).append(
                    {"symbol": "GOOG", "asset_id": "competing-owner"}
                )
            else:
                current["new_owner_fact"] = True
            with self.subTest(change=change):
                self.assertTrue(requires_lineage({"GOOG": prior}, {"GOOG": current}, pricing_unchanged=True))

    def test_identical_refresh_adds_dependency_bookkeeping_without_group_recalculation(self):
        case = self.case
        names = ("AAA", "BBB")
        identity = fixture.asset("BBB", "shared-uuid")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-10-01",
            "old_symbol": "AAA",
            "new_symbol": "BBB",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = fixture.IdentityClient(
            {"AAA": fixture.asset("AAA", "shared-uuid"), "BBB": identity, "111111111": identity}, [action]
        )
        bars = case.bars + [fixture.bar("2026-10-14")]
        for name in names:
            case.download(symbol=name)
            case.download(symbol=name, end="2026-10-15", bars=bars)
        finish = metadata._finish_evidence

        def completed_without_bookkeeping(evidence, window):
            result = finish(evidence, window)
            result.pop("mapping_endpoint_observations", None)
            return result

        # Model an accepted complete group from before this optional producer
        # bookkeeping existed, using the normal normalizer and publication path.
        with patch.object(metadata, "_finish_evidence", side_effect=completed_without_bookkeeping):
            case.run_build(symbols=["AAA"], client=client)
        previous = {name: case.receipt(name) for name in names}
        self.assertEqual(set(previous["AAA"]["input_sources"]), set(names))
        refreshed = case.download(symbol="AAA", end="2026-10-15", bars=bars, refresh_all=True)
        self.assertFalse(refreshed["changed"])
        self.assertIsNone(refreshed["pending_update"])
        self.assertNotEqual(refreshed["source_sha256"], previous["AAA"]["source_sha256"])
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("bookkeeping reran lineage")),
            patch.object(pricing, "load_published_views", side_effect=AssertionError("historical prices read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price boundary read")),
            patch.object(pipeline, "event", wraps=pipeline.event) as events,
        ):
            result = case.run_build(symbols=["AAA"], client=client)["results"][0]
        self.assertEqual(result["processed"], list(names))
        self.assertEqual(result["pricing_rows_processed"], 0)
        self.assertEqual(result["historical_rows_reused"], sum(r["source_rows"] for r in previous.values()))
        self.assertTrue(result["reused_decision"])
        self.assertEqual(
            [call.kwargs["required"] for call in events.call_args_list if call.args == ("lineage_required",)],
            [False],
        )
        for name in names:
            current = case.receipt(name)
            self.assertTrue(current["evidence"]["mapping_endpoint_observations"])
            self.assertEqual(current["boundaries"], previous[name]["boundaries"])
            self.assertEqual(current["asset_ids"], previous[name]["asset_ids"])
