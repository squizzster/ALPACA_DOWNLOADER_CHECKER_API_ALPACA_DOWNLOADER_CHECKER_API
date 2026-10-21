"""Accepted prefixes survive arbitrary append ranges and publication rollover."""

import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import pipeline, storage, transform
from libs.alpaca_lineage_build.resume import eligible_source


class LineageContinuationTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_normal_handoff_and_restart_select_only_their_required_pricing_receipts(self):
        from test_alpaca_lineage_batching import Provider

        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        for target, has_handoff in (("2026-10-15", True), ("2026-10-16", False)):
            publication = case.download(end=target, bars=fixture.calendar_bars("2025-12-01", target))
            client = Provider(["GOOG"])
            with patch.object(pipeline, "_current_sources", wraps=pipeline._current_sources) as reads:
                case.run_build(
                    client=client.client, pricing_handoff={"GOOG": publication} if has_handoff else None
                )
            self.assertEqual(reads.call_count, 1 if has_handoff else 2)
            self.assertEqual(len([call for call in client.calls if call[0] == "actions"]), 1)

    def test_handoff_preserves_an_unconsumed_historical_correction_before_append(self):
        from test_alpaca_lineage_batching import Provider

        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        corrected = [fixture.bar("2025-12-01", "50"), *case.bars[1:]]
        changed = case.download(bars=corrected, refresh_all=True)
        publication = case.download(end="2026-10-15", bars=corrected + [fixture.bar("2026-10-14")])
        self.assertEqual(publication["pending_update"]["from"], changed["pending_update"]["from"])
        client = Provider(["GOOG"])
        with patch.object(pipeline, "_current_sources", wraps=pipeline._current_sources) as reads:
            result = case.run_build(client=client.client, pricing_handoff={"GOOG": publication})
        self.assertEqual(reads.call_count, 1)
        self.assertFalse(result["results"][0]["reused_closed_boundaries"])
        self.assertEqual([params["start"] for kind, params in client.calls if kind == "actions"], ["2016-01-01"])

    def test_only_added_sessions_are_resolved_across_days_months_and_years(self):
        case = self.case
        start, end = "2025-12-01", "2026-09-29"
        case.download(end=end, bars=fixture.calendar_bars(start, end))
        case.run_build()
        build = transform.build_security_correlation_report
        for target in ("2026-09-30", "2026-10-15", "2026-12-31", "2027-01-06", "2028-02-03"):
            with self.subTest(previous=end, target=target):
                previous = case.receipt()
                added = {row["t"] for row in fixture.calendar_bars(end, target)}
                case.download(end=target, bars=fixture.calendar_bars(start, target))
                observed = set()

                def resolve(report, *args, **kwargs):
                    for descriptor in report["local"]["downloads"]:
                        rows = kwargs["authenticated_download_reader"](None, None, descriptor).rows
                        timestamps = {row["t"] for row in rows}
                        self.assertEqual(timestamps, added)
                        observed.update(timestamps)
                    return build(report, *args, **kwargs)

                with patch.object(transform, "build_security_correlation_report", side_effect=resolve):
                    result = case.run_build()
                self.assertEqual(observed, added)
                self.assertTrue(result["results"][0]["reused_closed_boundaries"])
                receipt = case.receipt()
                self.assertEqual(receipt["source_rows"], previous["source_rows"] + 8 * len(added))
                self.assertEqual(receipt["unresolved_rows"], 0)
                self.assertEqual(
                    sum(s["row_count"] for s in case.assignments()), len(fixture.calendar_bars(start, target))
                )
                self.assertEqual(receipt["assignment_summary"]["raw"]["rows"], receipt["source_rows"] // 8)
                end = target

    def test_same_month_does_not_open_approved_closed_assignments_or_prices(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        before = case.receipt()
        case.download(end="2026-10-15", bars=case.bars + [fixture.bar("2026-10-14")])
        read = Path.read_bytes

        def selected(path):
            if path.name == "completed_years.manifest.json" or path.name == "year.manifest.json":
                self.fail(f"closed publication reopened: {path}")
            if path.suffix == ".gz" and path.parent.name != "Oct":
                self.fail(f"closed prices reopened: {path}")
            return read(path)

        with patch.object(Path, "read_bytes", selected):
            case.run_build()
        after = case.receipt()
        for kind in ("completed_years", "current_year"):
            self.assertEqual(before["boundaries"][kind], after["boundaries"][kind])

    def test_prior_receipt_without_summary_consumes_assignments_without_recalculating_them(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        receipt = case.receipt()
        receipt.pop("assignment_summary")
        with storage.writer_lock(case.store) as root:
            index = storage.read_index(root)
            storage.publish(case.store, root, index, {"GOOG": receipt}, {"GOOG": {}}, {})
        case.download(end="2026-10-15", bars=case.bars + [fixture.bar("2026-10-14")])
        build = transform.build_security_correlation_report

        def resolve(report, *args, **kwargs):
            for descriptor in report["local"]["downloads"]:
                rows = kwargs["authenticated_download_reader"](None, None, descriptor).rows
                self.assertEqual([row["t"] for row in rows], [fixture.bar("2026-10-14")["t"]])
            return build(report, *args, **kwargs)

        with patch.object(transform, "build_security_correlation_report", side_effect=resolve):
            case.run_build()
        self.assertIn("assignment_summary", case.receipt())

    def test_correction_or_missing_generation_proof_does_not_claim_approved_prefix(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        previous = case.receipt()
        from libs.alpaca_pricing_download.storage import load_source

        case.download(end="2026-10-15", bars=case.bars + [fixture.bar("2026-10-14")])
        source = load_source(case.store / "end_of_day/pricing/GOOG", "GOOG")
        self.assertTrue(eligible_source(source, previous, refresh=False))
        self.assertFalse(eligible_source(source, previous, refresh=True))
        for changes in ([], [{"source_sha256": previous["source_sha256"], "changed_from": "2026-09-01"}]):
            self.assertFalse(eligible_source({**source, "history_changes": changes}, previous, refresh=False))

    def test_unchanged_prices_advance_without_payload_reads_or_rule_execution(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        bars = fixture.calendar_bars("2025-12-01", "2026-10-17")
        case.download(end="2026-10-17", bars=bars)
        case.run_build()

        def decisions():
            return [
                {key: value for key, value in segment.items() if key not in {"row_start", "row_end"}}
                for segment in case.assignments()
            ]

        expected = decisions()
        for target in ("2026-10-19", "2026-10-21", "2026-11-03", "2027-01-06", "2028-02-03"):
            with self.subTest(target=target):
                before = case.receipt()
                case.download(end=target, bars=bars)
                with (
                    patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price read")),
                    patch.object(pricing, "load_published_views", side_effect=AssertionError("price read")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("rule replay")),
                ):
                    result = case.run_build()
                after = case.receipt()
                self.assertTrue(result["results"][0]["reused_decision"])
                self.assertEqual(result["results"][0]["pricing_rows_processed"], 0)
                self.assertEqual(before["source_rows"], after["source_rows"])
                self.assertEqual(before["assignment_summary"], after["assignment_summary"])
                self.assertEqual(after["processed_through"][:10], target)
                self.assertEqual(decisions(), expected)

    def test_unchanged_unresolved_history_keeps_its_accepted_decisions(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        client = fixture.IdentityClient(assets={})
        case.download()
        case.run_build(client=client)
        before = case.receipt()
        assignments = case.assignments()
        self.assertGreater(before["unresolved_rows"], 0)
        case.download(end="2026-10-15")
        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("price read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price read")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("rule replay")),
        ):
            case.run_build(client=client)
        after = case.receipt()
        for field in ("source_rows", "correlated_rows", "unresolved_rows", "boundaries", "assignment_summary"):
            self.assertEqual(before[field], after[field], field)
        self.assertEqual(case.assignments(), assignments)

    def test_unchanged_dependency_group_preserves_all_published_members(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        names = ("GOOG", "OTHER")
        client = fixture.IdentityClient(assets={name: fixture.asset(name, "shared-id") for name in names})
        for name in names:
            case.download(symbol=name)
        case.run_build(symbols=list(names), client=client)
        previous = {name: case.receipt(name) for name in names}
        self.assertEqual(set(previous["GOOG"]["input_sources"]), set(names))
        for name in names:
            case.download(symbol=name, end="2026-10-15")
        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("price read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price read")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("rule replay")),
        ):
            result = case.run_build(symbols=["GOOG"], client=client)
        self.assertEqual(result["results"][0]["processed"], list(names))
        self.assertTrue(result["results"][0]["reused_decision"])
        for name in names:
            current = case.receipt(name)
            self.assertEqual(current["processed_through"][:10], "2026-10-15")
            for field in ("source_rows", "correlated_rows", "unresolved_rows", "boundaries"):
                self.assertEqual(previous[name][field], current[field], (name, field))
        for name in names:
            bars = case.bars + ([fixture.bar("2026-10-15")] if name == "GOOG" else [])
            case.download(symbol=name, end="2026-10-16", bars=bars)
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate:
            changed = case.run_build(symbols=["GOOG"], client=client)
        self.assertGreater(correlate.call_count, 0)
        self.assertFalse(changed["results"][0]["reused_decision"])

    def test_empty_histories_advance_without_price_reads_or_rule_execution(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        case.download(bars=[])
        case.run_build()
        case.download(end="2027-01-06", bars=[])
        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("price read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price read")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("rule replay")),
        ):
            result = case.run_build()
        self.assertTrue(result["results"][0]["reused_decision"])
        self.assertEqual(case.receipt()["source_rows"], 0)
        self.assertEqual(case.receipt()["processed_through"][:10], "2027-01-06")

    def test_unchanged_prices_with_changed_identity_evidence_are_reconciled(self):
        case = self.case
        case.download()
        case.run_build()
        before = case.receipt()
        case.download(end="2026-10-15")
        client = fixture.IdentityClient(assets={"GOOG": fixture.asset("GOOG", "replacement-id")})
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate:
            result = case.run_build(client=client)
        self.assertGreater(correlate.call_count, 0)
        self.assertFalse(result["results"][0].get("reused_decision", False))
        self.assertNotEqual(before["asset_ids"], case.receipt()["asset_ids"])

    def test_append_consumes_completed_cusip_mapping_with_matching_fresh_catalogue(self):
        from test_alpaca_lineage_batching import Provider, event

        from libs.alpaca_lineage_build.metadata import EvidenceClient
        from libs.alpaca_lineage_build.resume import stable_evidence

        case = self.case
        action = event("GOOG", "accepted", process_date="2025-12-15", cusip="HISTORICAL-CUSIP")

        def client():
            provider = Provider(["GOOG"], [action])
            provider.assets["HISTORICAL-CUSIP"] = fixture.asset("GOOG")
            evidence = EvidenceClient(
                provider.client,
                observation={"records": provider.catalogue, "catalogue_observed_at_utc": "2026-10-21T00:00:00Z"},
            )
            return provider, evidence

        bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download(bars=bars)
        provider, evidence = client()
        case.run_build(client=provider.client, evidence_client=evidence)
        previous = case.receipt()
        case.download(end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        provider, evidence = client()
        case.run_build(client=provider.client, evidence_client=evidence)
        self.assertEqual([kind for kind, _ in provider.calls], ["actions"])
        self.assertTrue(stable_evidence(previous["evidence"], case.receipt()["evidence"]))
