"""Continuation consumes accepted membership and every selected-date contributor."""

import unittest
from unittest.mock import patch

import test_alpaca_end_of_day_security_correlation as correlation_fixture
import test_alpaca_lineage_build as fixture
from test_alpaca_lineage_batching import Provider
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_lineage_build import pipeline, pricing, storage
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_pricing_download import refresh
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.producer import maintain


class IncrementalIdentityContractTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_append_preserves_accepted_multiple_uuid_membership_without_rebuilding_it(self):
        case = self.case
        facts = correlation_fixture.fun_like_lineage()
        assets = {item["lookup"]: item["asset"] for item in facts["asset_lookups"]}
        for identity in facts["identities"]:
            for cusip in identity.get("lookup_cusips", ()):
                assets[cusip] = identity
        assets["SIX"] = assets["83001A102"] = None
        facts["corporate_actions"].append(
            {
                "action_type": "cash_dividend",
                "id": "current-dividend",
                "process_date": "2024-07-02",
                "ex_date": "2024-07-02",
                "symbol": "FUN",
                "cusip": "83001C108",
                "rate": "0.1",
            }
        )
        client = fixture.IdentityClient(assets, facts["corporate_actions"])
        bars = fixture.informative_bars("2024-06-01", "2024-07-03")
        for name in ("FUN", "SIX"):
            case.download(name, end="2024-07-03", bars=bars if name == "FUN" else [], from_date="2024-06-01")
        case.run_build(symbols=["FUN"], client=client)
        accepted = case.assignments("FUN")[0]["security_identity"]
        self.assertEqual(len(accepted["member_alpaca_asset_ids"]), 2)
        publications = {
            name: case.download(
                name,
                end="2024-07-04",
                bars=[*bars, *fixture.informative_bars("2024-07-03", "2024-07-04")] if name == "FUN" else [],
            )
            for name in ("FUN", "SIX")
        }
        original = row_correlation._decide_row

        def decide(context, **kwargs):
            self.assertEqual(context.session_date.isoformat(), "2024-07-03", "old ownership recalculated")
            return original(context, **kwargs)

        with (
            patch.object(row_correlation, "_decide_row", side_effect=decide),
            patch.object(
                row_correlation,
                "security_identity_for_alpaca_assets",
                side_effect=AssertionError("accepted membership rebuilt"),
            ),
        ):
            result = case.run_build(symbols=["FUN"], client=client, pricing_handoff=publications)
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        latest = case.assignments("FUN")[-1]
        self.assertEqual(latest["correlation_method"], "asof_selected_uuid_lineage_interval")
        self.assertEqual(latest["security_identity"], accepted)

    def test_corrected_date_consumes_unchanged_competitor_and_preserves_labels(self):
        case = self.case
        names = ("AAA", "BBB")
        identity = fixture.IdentityClient({name: fixture.asset(name, "shared-id") for name in names})
        bars = fixture.informative_bars("2026-10-01", "2026-10-14")
        for name in names:
            case.download(name, bars=bars)
        case.run_build(symbols=names, client=identity)
        self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
        changed = FakeClient([*bars[:-1], {**bars[-1], "v": 12345}])
        selection = refresh.check_retained_source(case.store, "AAA", "2026-10-14", client=changed)
        publication = download_pricing(
            case.store,
            "AAA",
            "2026-10-14",
            client=changed,
            endpoint_selection=selection["endpoint_selection"],
        )
        original = pipeline.correlate
        original_decide = row_correlation._decide_row
        observed = []

        def correlate(store, inputs, evidence, **kwargs):
            observed.append({name: len(view.views["raw"]) for name, view in inputs.items()})
            return original(store, inputs, evidence, **kwargs)

        def decide(context, **kwargs):
            self.assertEqual(context.requested_symbol, "AAA", "accepted peer ownership recalculated")
            self.assertEqual(context.session_date.isoformat(), "2026-10-13", "earlier owner recalculated")
            return original_decide(context, **kwargs)

        with (
            patch.object(pipeline, "correlate", side_effect=correlate),
            patch.object(row_correlation, "_decide_row", side_effect=decide),
            patch.object(pricing, "load_published_boundary", wraps=pricing.load_published_boundary) as reads,
        ):
            result = case.run_build(symbols=names, client=identity, pricing_handoff={"AAA": publication})
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual(observed, [{"AAA": 1, "BBB": 1}])
        self.assertEqual(
            [(call.args[0].name, call.args[2]) for call in reads.call_args_list],
            [("AAA", "current_month"), ("BBB", "current_month")],
        )
        self.assertTrue(maintain(case.store)["complete"])
        group = serving_storage.read_groups(
            case.store, ["AAA"], generation=serving_storage.selector_state(case.store)["generation"]
        )["AAA"]
        self.assertEqual(group["decisions"][0]["reasons"], ["conflicting_daily_history"])
        labels = next(s for s in case.assignments("AAA") if s["to"] == "2026-10-13")["observation_tickers"]
        self.assertEqual(labels, ["AAA", "BBB"])

    def test_structurally_related_distinct_identity_is_not_reopened_for_correction(self):
        case = self.case
        names = ("AAA", "BBB")
        assets = {name: fixture.asset(name) for name in names}
        assets.update({"111111111": assets["AAA"], "222222222": assets["BBB"]})
        action = {
            "id": "acquisition",
            "action_type": "stock_merger",
            "process_date": "2026-10-05",
            "effective_date": "2026-10-05",
            "acquirer_symbol": "AAA",
            "acquirer_cusip": "111111111",
            "acquiree_symbol": "BBB",
            "acquiree_cusip": "222222222",
        }
        identity = fixture.IdentityClient(assets, [action])
        bars = fixture.informative_bars("2026-10-01", "2026-10-14")
        for name in names:
            case.download(name, bars=bars)
        case.run_build(symbols=names, client=identity)
        self.assertNotEqual(
            case.assignments("AAA")[0]["security_identity"]["security_id"],
            case.assignments("BBB")[0]["security_identity"]["security_id"],
        )
        changed = FakeClient([*bars[:-1], {**bars[-1], "v": 12345}])
        selection = refresh.check_retained_source(case.store, "AAA", "2026-10-14", client=changed)
        publication = download_pricing(
            case.store, "AAA", "2026-10-14", client=changed, endpoint_selection=selection["endpoint_selection"]
        )
        with patch.object(pricing, "load_published_boundary", wraps=pricing.load_published_boundary) as reads:
            case.run_build(symbols=names, client=identity, pricing_handoff={"AAA": publication})
        self.assertEqual(
            [(call.args[0].name, call.args[2]) for call in reads.call_args_list], [("AAA", "current_month")]
        )

    def test_legacy_empty_hot_month_projects_last_accepted_membership_once(self):
        case = self.case
        facts = correlation_fixture.fun_like_lineage()
        for action in facts["corporate_actions"]:
            for field in ("process_date", "effective_date"):
                if field in action:
                    action[field] = "2026-10-20" if action["action_type"] == "cash_dividend" else "2026-10-28"
        facts["corporate_actions"].append(
            {
                "action_type": "cash_dividend",
                "id": "current-dividend",
                "process_date": "2026-10-29",
                "ex_date": "2026-10-29",
                "symbol": "FUN",
                "cusip": "83001C108",
                "rate": "0.1",
            }
        )
        assets = {item["lookup"]: item["asset"] for item in facts["asset_lookups"]}
        for identity in facts["identities"]:
            for cusip in identity.get("lookup_cusips", ()):
                assets[cusip] = identity
        assets["SIX"] = assets["83001A102"] = None
        client = fixture.IdentityClient(assets, facts["corporate_actions"])
        bars = fixture.informative_bars("2026-10-01", "2026-10-31")
        for name in ("FUN", "SIX"):
            case.download(name, end="2026-10-31", bars=bars if name == "FUN" else [], from_date="2026-10-01")
        case.run_build(symbols=["FUN"], client=client)
        accepted = case.assignments("FUN")[0]["security_identity"]
        self.assertEqual(len(accepted["member_alpaca_asset_ids"]), 2)
        empty = {
            name: case.download(name, end="2026-11-02", bars=bars if name == "FUN" else [])
            for name in ("FUN", "SIX")
        }
        case.run_build(symbols=["FUN"], client=client, pricing_handoff=empty)
        index = storage.read_index(case.root)
        with storage.publication_lock(case.root, exclusive=True):
            entries = {}
            for name in ("FUN", "SIX"):
                receipt = case.receipt(name)
                for field in (
                    "identity_memberships",
                    "identity_intervals",
                    "observation_routes",
                    "continuation_handoff_version",
                ):
                    receipt.pop(field, None)
                storage.atomic_json(case.root / name / "lineage.manifest.json", receipt)
                entries[name] = {**index["symbols"][name], "sha256": storage.document_digest(receipt)}
            storage._update_index(case.root, entries)
        publications = {
            name: case.download(
                name,
                end="2026-11-03",
                bars=[*bars, *fixture.informative_bars("2026-11-02", "2026-11-03")] if name == "FUN" else [],
            )
            for name in ("FUN", "SIX")
        }
        with patch.object(storage, "read_boundaries", wraps=storage.read_boundaries) as reads:
            result = case.run_build(symbols=["FUN"], client=client, pricing_handoff=publications)
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual(case.assignments("FUN")[-1]["security_identity"], accepted)
        selected = [(call.args[1]["symbol"], call.kwargs.get("kinds")) for call in reads.call_args_list]
        self.assertEqual(selected.count(("FUN", {"current_year"})), 1)
        self.assertFalse(any(kinds and "completed_years" in kinds for _, kinds in selected))

    def test_missing_new_continuation_handoff_fails_before_legacy_or_pricing_reads(self):
        case = self.case
        case.download()
        case.run_build(symbols=["GOOG"])
        receipt = case.receipt()
        self.assertEqual(receipt["continuation_handoff_version"], 1)
        receipt.pop("identity_memberships")
        index = storage.read_index(case.root)
        storage.atomic_json(case.root / "GOOG/lineage.manifest.json", receipt)
        with storage.publication_lock(case.root, exclusive=True):
            storage._update_index(
                case.root, {"GOOG": {**index["symbols"]["GOOG"], "sha256": storage.document_digest(receipt)}}
            )
        with (
            patch.object(
                pipeline, "load_pricing", side_effect=AssertionError("pricing opened for incomplete handoff")
            ),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("new handoff reconstructed as legacy")
            ),
        ):
            result = pipeline.run_lineage(case.store, ["GOOG"], identity_client=fixture.IdentityClient())
        self.assertEqual(result["failed"], 1)
        self.assertIn("finish owning lineage recovery", result["results"][0]["error"])

    def test_unchanged_peer_later_suffix_keeps_accepted_owner_and_numerical_priority(self):
        case = self.case
        names = ("AAA", "BBB")
        identity = fixture.IdentityClient({name: fixture.asset(name, "shared-id") for name in names})
        short = fixture.informative_bars("2026-10-01", "2026-10-14")
        long = fixture.informative_bars("2026-10-01", "2026-10-15")
        for name in names:
            case.download(name, end="2026-10-15", bars=short if name == "AAA" else long)
        case.run_build(symbols=names, client=identity)
        counts = {name: case.receipt(name)["source_rows"] for name in names}
        changed = FakeClient([*short[:-1], {**short[-1], "v": 12345}])
        selection = refresh.check_retained_source(case.store, "AAA", "2026-10-15", client=changed)
        publication = download_pricing(
            case.store, "AAA", "2026-10-15", client=changed, endpoint_selection=selection["endpoint_selection"]
        )
        original = row_correlation._decide_row

        def decide(context, **kwargs):
            self.assertEqual((context.requested_symbol, context.session_date.isoformat()), ("AAA", "2026-10-13"))
            return original(context, **kwargs)

        with patch.object(row_correlation, "_decide_row", side_effect=decide):
            result = case.run_build(symbols=names, client=identity, pricing_handoff={"AAA": publication})
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual({name: case.receipt(name)["source_rows"] for name in names}, counts)
        a = next(s for s in case.assignments("AAA") if s["to"] == "2026-10-13")
        b = next(s for s in case.assignments("BBB") if s["from"] == "2026-10-13")
        self.assertEqual(a["observation_selection"], "superseded")
        self.assertEqual(b["observation_selection"], "selected")
        self.assertEqual(b["observation_tickers"], ["AAA", "BBB"])

    def test_corrected_competitors_recover_publication_without_reopening_inputs(self):
        class Interrupted(BaseException):
            pass

        case = self.case
        names = ("AAA", "BBB")
        identity = fixture.IdentityClient({name: fixture.asset(name, "shared-id") for name in names})
        bars = fixture.informative_bars("2026-10-01", "2026-10-14")
        for name in names:
            case.download(name, bars=bars)
        case.run_build(symbols=names, client=identity)
        changed = FakeClient([*bars[:-1], {**bars[-1], "v": 12345}])
        selection = refresh.check_retained_source(case.store, "AAA", "2026-10-14", client=changed)
        publication = download_pricing(
            case.store, "AAA", "2026-10-14", client=changed, endpoint_selection=selection["endpoint_selection"]
        )
        with (
            patch.object(storage, "finish_publication", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            case.run_build(symbols=names, client=identity, pricing_handoff={"AAA": publication})
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed row decisions repeated")),
            patch.object(
                pipeline, "load_pricing", side_effect=AssertionError("completed input assembly repeated")
            ),
        ):
            result = case.run_build(symbols=names, client=identity)
        self.assertTrue(result["recovered"])
        self.assertEqual(case.assignments("AAA")[-1]["observation_selection"], "conflict")
        self.assertEqual(case.assignments("BBB")[-1]["observation_selection"], "conflict")

    def test_changed_pinned_mapping_reconciles_despite_equal_prices(self):
        case = self.case
        new, old = fixture.asset("CURRENT", "selected-id"), fixture.asset("CURRENT", "other-id")
        identity = fixture.IdentityClient(
            {**dict.fromkeys(("EARLY", "PRIOR", "CURRENT", "111111111"), new), "222222222": old},
            correlation_fixture.RowCorrelationTests._overlapping_asof_lineage()["corporate_actions"],
        )
        names = ("CURRENT", "EARLY", "PRIOR")
        bars = fixture.informative_bars("2024-02-26", "2024-03-04")
        for name in names:
            case.download(
                name,
                bars=bars if name == "CURRENT" else [],
                refresh_all=True,
                mapping_asof="2024-02-29" if name == "CURRENT" else None,
            )
        case.run_build(symbols=["CURRENT"], client=identity)
        publications = {
            name: case.download(name, end="2026-10-15", bars=bars if name == "CURRENT" else []) for name in names
        }
        case.run_build(symbols=["CURRENT"], client=identity, pricing_handoff=publications)
        publication = case.download(
            "CURRENT", end="2026-10-15", bars=bars, mapping_asof="2026-10-13", refresh_all=True
        )
        result = case.run_build(symbols=["CURRENT"], client=identity, pricing_handoff={"CURRENT": publication})
        self.assertFalse(result["results"][0]["reused_decision"])
        self.assertEqual(case.receipt("CURRENT")["unresolved_rows"], 40)

    def test_selected_fresh_endpoint_invalidates_unchanged_fast_path(self):
        case = self.case
        case.download()

        def build(identifier):
            provider = Provider(["GOOG"])
            provider.assets["GOOG"] = fixture.asset("GOOG", identifier)
            evidence = EvidenceClient(
                provider.client,
                observation={
                    "records": [provider.assets["GOOG"]],
                    "catalogue_observed_at_utc": "2026-10-14T00:00:00Z",
                },
            )
            return case.run_build(symbols=["GOOG"], client=provider.client, evidence_client=evidence)

        build("old")
        result = build("new")
        self.assertFalse(result["results"][0]["reused_decision"])
        self.assertEqual(case.receipt()["asset_ids"], ["new"])
        with patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")):
            self.assertTrue(build("new")["results"][0]["reused_decision"])
