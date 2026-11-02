"""Changed canonical endpoints invalidate only their retained exact mappings."""

import unittest
from unittest.mock import patch

import test_alpaca_lineage_acquisition as acquisition
import test_alpaca_lineage_build as fixture
from test_alpaca_lineage_batching import Provider

from libs.alpaca_lineage_build import metadata, pipeline, pricing
from libs.alpaca_lineage_build.metadata import EvidenceClient, WindowClient, _finish_evidence


class MetadataDependencyTests(unittest.TestCase):
    def test_complete_group_continuation_keeps_mapping_baseline_and_refreshes_changed_endpoint(self):
        for changed_endpoint in (False, True):
            with self.subTest(changed_endpoint=changed_endpoint):
                case = fixture.LineageBuildTests()
                case.setUp()
                self.addCleanup(case.doCleanups)
                action = {
                    "action_type": "cash_dividend",
                    "id": "distribution",
                    "symbol": "AAA",
                    "cusip": "111111111",
                    "process_date": "2026-10-01",
                    "ex_date": "2026-10-01",
                    "record_date": "2026-10-01",
                    "payable_date": "2026-10-01",
                    "rate": "0.1",
                }

                def provider(endpoint="current", mapping="historical"):
                    value = Provider(["AAA", "ZZZ"], [action])
                    value.assets["ZZZ"] = fixture.asset("ZZZ", endpoint)
                    value.assets["111111111"] = fixture.asset("ZZZ", mapping)
                    value.catalogue = [value.assets["AAA"], value.assets["ZZZ"]]
                    return value

                def run(value):
                    return case.run_build(
                        symbols=["AAA"],
                        client=value.client,
                        evidence_client=EvidenceClient(
                            value.client,
                            observation={
                                "records": value.catalogue,
                                "catalogue_observed_at_utc": "2026-10-14T12:00:00Z",
                            },
                        ),
                    )

                bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
                for name in ("AAA", "ZZZ"):
                    case.download(symbol=name, bars=bars)
                run(provider())
                prior = case.receipt("AAA")
                self.assertEqual(set(prior["input_sources"]), {"AAA", "ZZZ"})
                self.assertEqual(
                    prior["evidence"]["mapping_endpoint_observations"]["111111111"]["ZZZ"], "current"
                )
                case.download(symbol="AAA", bars=[*bars[:-1], fixture.bar("2026-10-13", "12")], refresh_all=True)
                endpoint = "next" if changed_endpoint else "current"
                mapping = "next-historical" if changed_endpoint else "historical"
                current = provider(endpoint, mapping)
                with (
                    patch.object(
                        metadata, "build_identity_report", wraps=metadata.build_identity_report
                    ) as build,
                    patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate,
                ):
                    result = run(current)
                self.assertEqual(result["results"][0]["processed"], ["AAA", "ZZZ"])
                self.assertEqual(build.call_count, 2)
                self.assertEqual(correlate.call_count, 1)
                self.assertEqual(
                    [call for call in current.calls if call == ("asset", "111111111")],
                    [("asset", "111111111")] if changed_endpoint else [],
                )
                evidence = case.receipt("AAA")["evidence"]
                lookup = next(row for row in evidence["asset_lookups"] if row["lookup"] == "111111111")
                self.assertEqual(lookup["asset"]["id"], mapping)
                self.assertEqual(evidence["mapping_endpoint_observations"]["111111111"]["ZZZ"], endpoint)
                with (
                    patch.object(pipeline, "correlate", side_effect=AssertionError("unchanged group replayed")),
                    patch.object(pricing, "load_published_views", side_effect=AssertionError("old prices read")),
                    patch.object(
                        pricing, "load_published_boundary", side_effect=AssertionError("old prices read")
                    ),
                ):
                    repeated = provider(endpoint, mapping)
                    run(repeated)
                self.assertEqual(repeated.calls, [])

    def test_published_pre77_stale_mapping_repairs_without_price_notice_or_action_requery(self):
        from test_alpaca_certificate_migration import install_v76

        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        action = {
            "action_type": "cash_dividend",
            "id": "distribution",
            "symbol": "AAA",
            "cusip": "111111111",
            "process_date": "2026-10-01",
            "ex_date": "2026-10-01",
            "record_date": "2026-10-01",
            "payable_date": "2026-10-01",
            "rate": "0.1",
        }

        def provider(identity):
            value = Provider(["AAA", "ZZZ"], [action], catalogue=[])
            value.assets = {"ZZZ": fixture.asset("ZZZ", identity), "111111111": fixture.asset("ZZZ", identity)}
            return value

        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        case.download(symbol="AAA", bars=bars)
        rejection, _ = acquisition.AuxiliaryAcquisitionTests().rejected_client()
        case.run_build(symbols=["AAA"], client=provider("old").client, pricing_client=rejection)
        case.download(symbol="AAA", end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        with patch.object(EvidenceClient, "mapping_current", return_value=True):
            case.run_build(symbols=["AAA"], client=provider("new").client, pricing_client=rejection)
        self.assertEqual(case.receipt("AAA")["asset_ids"], ["old"])
        install_v76(case, serving=False)
        current = provider("new")
        case.run_build(symbols=["AAA"], client=current.client, pricing_client=rejection)
        self.assertEqual(case.receipt("AAA")["asset_ids"], ["new"])
        self.assertEqual(current.calls, [("asset", "111111111")])
        repeated = provider("new")
        case.run_build(symbols=["AAA"], client=repeated.client, pricing_client=rejection)
        self.assertEqual(repeated.calls, [])

    def evidence(self, *, canonical="old", explicit=None):
        lookups = [
            {
                "lookup_kind": "cusip",
                "lookup": "111111111",
                "outcome": "found",
                "http_status": 200,
                "asset": fixture.asset("ZZZ", canonical),
            }
        ]
        if explicit is not None:
            lookups.append(
                {
                    "lookup_kind": "symbol",
                    "lookup": "ZZZ",
                    "outcome": "found",
                    "http_status": 200,
                    "asset": fixture.asset("ZZZ", explicit),
                }
            )
        return {"asset_lookups": lookups, "corporate_actions": [], "warnings": []}

    def test_explicit_baseline_preserves_legitimate_ticker_reuse(self):
        client = EvidenceClient(fixture.IdentityClient({"ZZZ": fixture.asset("ZZZ", "new")}))
        client.get_symbol_lookup("ZZZ")
        self.assertTrue(client.evidence_current(self.evidence(explicit="new")))
        self.assertFalse(client.evidence_current(self.evidence()))
        self.assertNotIn("111111111", client.assets)

    def test_published_group_baseline_selects_only_dependent_mapping(self):
        client = EvidenceClient(fixture.IdentityClient({"111111111": fixture.asset("ZZZ", "new")}))
        stale = self.evidence()
        current = self.evidence(explicit="new")
        client.prepare_retained([stale, {"asset_lookups": [current["asset_lookups"][-1]]}])
        self.assertFalse(client.evidence_current(stale))
        self.assertEqual(client.request_count, 0)
        window = WindowClient(client, "AAA", {"anchor_month": "2026-10", "evidence": stale}, False)
        self.assertEqual(window.get_asset_lookup("111111111")["asset"]["id"], "new")
        self.assertEqual(client.request_count, 1)

    def test_fresh_old_mapping_is_trusted_and_retains_its_new_endpoint_baseline(self):
        client = EvidenceClient(
            fixture.IdentityClient(
                {"ZZZ": fixture.asset("ZZZ", "new"), "111111111": fixture.asset("ZZZ", "old")}
            )
        )
        client.get_symbol_lookup("ZZZ")
        prior = self.evidence()
        window = WindowClient(client, "AAA", {"anchor_month": "2026-10", "evidence": prior}, False)
        window.get_asset_lookup("111111111")
        completed = _finish_evidence(self.evidence(), window)
        self.assertTrue(client.evidence_current(completed))
        self.assertEqual(completed["mapping_endpoint_observations"], {"111111111": {"ZZZ": "new"}})
        following = EvidenceClient(fixture.IdentityClient({"ZZZ": fixture.asset("ZZZ", "new")}))
        following.get_symbol_lookup("ZZZ")
        self.assertTrue(following.evidence_current(completed))
        reused = WindowClient(following, "AAA", {"anchor_month": "2026-10", "evidence": completed}, False)
        self.assertEqual(reused.get_asset_lookup("111111111")["asset"]["id"], "old")
        self.assertEqual(following.request_count, 1)
        following.assets["ZZZ"] = {
            "asset": fixture.asset("ZZZ", "third"),
            "outcome": "found",
            "http_status": 200,
        }
        self.assertFalse(following.evidence_current(completed))

    def test_later_unavailable_canonical_lookup_reassembles_earlier_priced_member(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        action = {
            "action_type": "cash_dividend",
            "id": "distribution",
            "symbol": "AAA",
            "cusip": "111111111",
            "process_date": "2026-10-01",
            "ex_date": "2026-10-01",
            "record_date": "2026-10-01",
            "payable_date": "2026-10-01",
            "rate": "0.1",
        }

        def provider(identity):
            value = Provider(["AAA", "ZZZ"], [action], catalogue=[])
            value.assets = {"ZZZ": fixture.asset("ZZZ", identity), "111111111": fixture.asset("ZZZ", identity)}
            return value

        bars = fixture.calendar_bars("2026-10-01", "2026-10-14")
        case.download(symbol="AAA", bars=bars)
        rejection, _ = acquisition.AuxiliaryAcquisitionTests().rejected_client()
        case.run_build(symbols=["AAA"], client=provider("old").client, pricing_client=rejection)
        case.download(symbol="AAA", end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        current = provider("new")
        case.run_build(symbols=["AAA"], client=current.client, pricing_client=rejection)
        continued = case.assignments("AAA")
        self.assertEqual(case.receipt("AAA")["asset_ids"], ["new"])
        self.assertEqual([v for k, v in current.calls if k == "asset"].count("111111111"), 1)
        case.run_build(symbols=["AAA"], client=provider("new").client, pricing_client=rejection, refresh=True)
        self.assertEqual(continued, case.assignments("AAA"))
