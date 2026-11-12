"""Accepted prefixes survive arbitrary append ranges and publication rollover."""

import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import pipeline, policy_engine, storage
from libs.alpaca_lineage_build.resume import unchanged_source
from libs.alpaca_lineage_build.transform import ASSIGNMENT_FIELDS
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class LineageContinuationTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def decisions(self):
        return {
            adjustment: [
                {key: value for key, value in segment.items() if key in ASSIGNMENT_FIELDS}
                for segment in self.case.assignments(adjustment=adjustment)
                for _ in range(segment["row_count"])
            ]
            for adjustment in ADJUSTMENTS
        }

    def prepare_asof_group_evidence_change(self):
        import test_alpaca_end_of_day_security_correlation as correlation_fixture

        case = self.case
        names = ("CURRENT", "OLD")
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        correlation_fixture.add_name_change_edge(
            lineage,
            action_id="old-current",
            process_date="2024-03-04",
            predecessor=("old-id", "OLD", "old-cusip", "Old issue"),
            successor=("current-id", "CURRENT", "current-cusip", "Current issue"),
        )
        actions = deepcopy(lineage["corporate_actions"])
        actions.append(
            {
                "action_type": "cash_dividend",
                "id": "changed-current-evidence",
                "symbol": "CURRENT",
                "cusip": "current-cusip",
                "process_date": "2026-10-01",
                "ex_date": "2026-10-01",
                "record_date": "2026-10-01",
                "payable_date": "2026-10-01",
                "rate": "0.1",
            }
        )
        assets = {
            key: identity
            for identity in lineage["identities"]
            for key in (identity["symbol"], *identity["lookup_cusips"])
        }
        histories = {
            "OLD": fixture.informative_bars("2024-02-26", "2024-03-04"),
            "CURRENT": fixture.informative_bars("2024-02-26", "2024-03-05"),
        }
        for name in names:
            case.download(
                symbol=name,
                bars=histories[name],
                refresh_all=True,
            )
        case.run_build(symbols=["CURRENT"], client=fixture.IdentityClient(assets, actions))
        for name in names:
            case.download(
                symbol=name,
                end="2026-10-15",
                bars=histories[name],
            )
        actions[-1]["rate"] = "0.2"
        return names, fixture.IdentityClient(assets, actions)

    def prepare_predecessor_group_evidence_change(self):
        import test_alpaca_end_of_day_security_correlation as correlation_fixture

        case = self.case
        names = ("BEFORE", "NEXT", "OLD", "OTHER")
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for action_id, when, predecessor, successor in (
            (
                "old-next",
                "2024-08-16",
                ("old1", "OLD", "old-cusip1", "First old issue"),
                ("new1", "NEXT", "new-cusip1", "Next issue"),
            ),
            (
                "before-old",
                "2025-01-02",
                ("pre2", "BEFORE", "pre-cusip2", "Earlier label"),
                ("old2", "OLD", "old-cusip2", "Second old issue"),
            ),
            (
                "old-other",
                "2025-10-30",
                ("old2", "OLD", "old-cusip2", "Second old issue"),
                ("new2", "OTHER", "new-cusip2", "Other issue"),
            ),
        ):
            correlation_fixture.add_name_change_edge(
                lineage,
                action_id=action_id,
                process_date=when,
                predecessor=predecessor,
                successor=successor,
            )
        actions = deepcopy(lineage["corporate_actions"])
        actions.append(
            {
                "action_type": "cash_dividend",
                "id": "changed-old-evidence",
                "symbol": "OLD",
                "cusip": "old-cusip2",
                "process_date": "2026-10-01",
                "ex_date": "2026-10-01",
                "record_date": "2026-10-01",
                "payable_date": "2026-10-01",
                "rate": "0.1",
            }
        )
        assets = {
            key: identity
            for identity in lineage["identities"]
            for key in (identity["symbol"], *identity["lookup_cusips"])
        }
        assets["OLD"] = next(identity for identity in lineage["identities"] if identity["id"] == "old2")
        traded = fixture.informative_bars("2024-08-14", "2024-08-17")
        histories = {name: traded if name in {"NEXT", "OLD"} else [] for name in names}
        for name in names:
            case.download(
                symbol=name,
                bars=histories[name],
                refresh_all=True,
                mapping_asof="2025-06-01" if name == "OLD" else None,
            )
        case.run_build(symbols=["OLD"], client=fixture.IdentityClient(assets, actions))
        for name in names:
            case.download(symbol=name, end="2026-10-15", bars=histories[name])
        actions[-1]["rate"] = "0.2"
        return names, fixture.IdentityClient(assets, actions)

    def assert_asof_current_history(self):
        assignments = self.case.assignments("CURRENT", adjustment="raw")
        self.assertTrue(assignments)
        self.assertEqual(0, self.case.receipt("CURRENT")["unresolved_rows"])
        self.assertTrue(
            all(
                item["correlation_rule"]["name"] == "asof_selected_uuid_lineage_interval" for item in assignments
            )
        )

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
        self.assertEqual([params["start"] for kind, params in client.calls if kind == "actions"], ["2026-10-01"])

    def test_only_added_rows_reach_the_owner_across_days_months_and_years(self):
        case = self.case
        start, end = "2025-12-01", "2026-09-29"
        case.download(end=end, bars=fixture.calendar_bars(start, end))
        case.run_build()
        build = policy_engine.build_security_correlation_report
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

                with patch.object(policy_engine, "build_security_correlation_report", side_effect=resolve):
                    result = case.run_build()
                self.assertEqual(observed, added)
                self.assertTrue(result["results"][0]["reused_closed_boundaries"])
                self.assertEqual(result["results"][0]["historical_rows_reused"], previous["source_rows"])
                self.assertEqual(result["results"][0]["pricing_rows_processed"], 8 * len(added))
                receipt = case.receipt()
                self.assertEqual(receipt["source_rows"], previous["source_rows"] + 8 * len(added))
                self.assertEqual(receipt["unresolved_rows"], 0)
                self.assertEqual(
                    sum(s["row_count"] for s in case.assignments()), len(fixture.calendar_bars(start, target))
                )
                self.assertEqual(receipt["assignment_summary"]["raw"]["rows"], receipt["source_rows"] // 8)
                decisions = self.decisions()
                case.run_build(refresh=True)
                self.assertEqual(self.decisions(), decisions)
                end = target

    def test_unchanged_inputs_do_not_open_approved_assignments_or_prices(self):
        case = self.case
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download()
        case.run_build()
        before = case.receipt()
        read = Path.read_bytes

        def selected(path):
            if path.name == "completed_years.manifest.json" or path.name == "year.manifest.json":
                self.fail(f"closed publication reopened: {path}")
            if path.suffix == ".gz" and path.parent.name != "Oct":
                self.fail(f"closed prices reopened: {path}")
            return read(path)

        with patch.object(Path, "read_bytes", selected):
            case.run_build(symbols=["GOOG"])
        after = case.receipt()
        for kind in ("completed_years", "current_year"):
            self.assertEqual(before["boundaries"][kind], after["boundaries"][kind])

    def test_prior_receipt_without_summary_projects_accepted_assignments_for_append(self):
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
        build = policy_engine.build_security_correlation_report

        def resolve(report, *args, **kwargs):
            for descriptor in report["local"]["downloads"]:
                rows = kwargs["authenticated_download_reader"](None, None, descriptor).rows
                self.assertEqual(
                    [row["t"] for row in rows],
                    [fixture.bar("2026-10-14")["t"]],
                )
            return build(report, *args, **kwargs)

        with patch.object(policy_engine, "build_security_correlation_report", side_effect=resolve):
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
        self.assertFalse(unchanged_source(source, previous))
        self.assertFalse(unchanged_source(source, previous))
        for changes in ([], [{"source_sha256": previous["source_sha256"], "changed_from": "2026-09-01"}]):
            self.assertFalse(unchanged_source({**source, "history_changes": changes}, previous))

    def test_later_plain_request_with_equal_prices_reuses_history_without_payloads(self):
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
                    patch.object(
                        pipeline, "correlate", side_effect=AssertionError("old lineage replay")
                    ) as owner,
                    patch.object(
                        pricing, "load_published_views", side_effect=AssertionError("old pricing read")
                    ),
                    patch.object(
                        pricing, "load_published_boundary", side_effect=AssertionError("old pricing read")
                    ),
                ):
                    result = case.run_build()
                after = case.receipt()
                self.assertTrue(result["results"][0]["reused_decision"])
                self.assertEqual(owner.call_count, 0)
                self.assertEqual(result["results"][0]["pricing_rows_processed"], 0)
                self.assertEqual(result["results"][0]["historical_rows_reused"], before["source_rows"])
                self.assertEqual(before["source_rows"], after["source_rows"])
                self.assertEqual(before["assignment_summary"], after["assignment_summary"])
                self.assertEqual(after["processed_through"][:10], target)
                self.assertEqual(decisions(), expected)
                accepted = self.decisions()
                case.run_build(refresh=True)
                self.assertEqual(self.decisions(), accepted)

    def test_unchanged_unresolved_history_keeps_its_accepted_decisions(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        client = fixture.IdentityClient(assets={})
        case.download()
        case.run_build(client=client)
        before = case.receipt()
        assignments = case.assignments()
        self.assertGreater(before["unresolved_rows"], 0)
        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("price read")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("price read")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("rule replay")),
        ):
            case.run_build(symbols=["GOOG"], client=client)
        after = case.receipt()
        for field in ("source_rows", "correlated_rows", "unresolved_rows", "boundaries", "assignment_summary"):
            self.assertEqual(before[field], after[field], field)
        self.assertEqual(case.assignments(), assignments)

    def test_unchanged_dependency_group_preserves_all_published_members(self):
        from libs.alpaca_lineage_build import pricing

        case = self.case
        names = ("GOOG", "OTHER")
        client = fixture.IdentityClient(assets={name: fixture.asset(name, "shared-id") for name in names})
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        for name in names:
            case.download(symbol=name)
        case.run_build(symbols=list(names), client=client)
        previous = {name: case.receipt(name) for name in names}
        self.assertEqual(set(previous["GOOG"]["input_sources"]), set(names))
        for name in names:
            case.download(symbol=name, end="2026-10-14")
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
            self.assertEqual(current["processed_through"][:10], "2026-10-14")
            for field in ("source_rows", "correlated_rows", "unresolved_rows", "boundaries"):
                self.assertEqual(previous[name][field], current[field], (name, field))
        for name in names:
            bars = case.bars + ([fixture.bar("2026-10-15")] if name == "GOOG" else [])
            case.download(symbol=name, end="2026-10-16", bars=bars)
        with (
            patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate,
            patch.object(
                pricing,
                "load_published_views",
                wraps=pricing.load_published_views,
            ) as full_reads,
        ):
            changed = case.run_build(symbols=["GOOG"], client=client)
        self.assertGreater(correlate.call_count, 0)
        self.assertFalse(changed["results"][0]["reused_decision"])
        self.assertFalse(changed["results"][0]["reused_closed_boundaries"])
        self.assertEqual(full_reads.call_count, 2)

    def test_complete_group_evidence_change_materializes_every_selected_peer(self):
        import test_alpaca_end_of_day_security_correlation as correlation_fixture

        from libs.alpaca_end_of_day_security_correlation.row_correlation import (
            correlate_download_rows,
        )
        from libs.alpaca_lineage_build import pricing

        names, client = self.prepare_asof_group_evidence_change()
        self.assert_asof_current_history()
        correlate = pipeline.correlate
        load_pricing = pipeline.load_pricing
        observed = []
        completed_reports = []
        partial_withheld = []
        materializations = []

        def selected_input(*args, **kwargs):
            view = load_pricing(*args, **kwargs)
            materializations.append((args[1], view.materialization_scope))
            return view

        def complete_inputs(store, inputs, evidence, **kwargs):
            observed.append(
                [(name, view.source_sha256, view.materialization_scope) for name, view in sorted(inputs.items())]
            )
            donor = inputs["OLD"]
            donor_raw = donor.downloads["raw.csv.gz"]
            donor_descriptor = next(item for item in donor.local["downloads"] if item["adjustment"] == "raw")
            self.assertGreater(len(donor_raw.rows), 0)
            self.assertEqual(donor_descriptor["rows"], len(donor_raw.rows))

            # Exercise the exact completeness gate and row owner with held
            # in-memory facts, not a second pipeline/policy replay. A suffix-only
            # OLD source is omitted as rename authority, so the five historical
            # CURRENT rows cannot inherit the unproved name-change edge.
            partial_donor = deepcopy(donor)
            partial_donor.materialization_scope = "retained_suffix"
            for download in partial_donor.downloads.values():
                download.rows = download.rows[-1:]
            cache = policy_engine._correlation_cache(kwargs["policy"])
            cache.selected_evidence = evidence
            policy_engine._register_sources(
                cache,
                {"CURRENT": inputs["CURRENT"], "OLD": partial_donor},
            )
            self.assertNotIn("OLD", cache._name_change_sources)
            intervals, _, edges, _ = cache.lineage_analysis(evidence["CURRENT"])
            assessment = cache.name_change_assessment(evidence["CURRENT"])
            self.assertNotIn("old-current", assessment.accepted)
            self.assertNotIn("old-current", {edge.action_id for edge in edges})
            current_raw = inputs["CURRENT"].downloads["raw.csv.gz"]
            source = correlation_fixture.correlation_source("CURRENT", asof=current_raw.manifest["asof"])
            without_donor = correlate_download_rows(
                current_raw.rows,
                source,
                evidence["CURRENT"],
                intervals,
                uuid_lineage_edges=edges,
                name_change_assessment=assessment,
            )
            partial_withheld.append(sum(item["row_count"] for item in without_donor["uncorrelated"]))

            requests = client.request_count
            reports = correlate(store, inputs, evidence, **kwargs)
            self.assertEqual(requests, client.request_count)
            completed_reports.append(reports)
            return reports

        with (
            patch.object(pipeline, "load_pricing", side_effect=selected_input),
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("lineage requested provider pricing")
            ),
            patch.object(pipeline, "correlate", side_effect=complete_inputs) as owner,
            patch.object(pipeline, "event", wraps=pipeline.event) as events,
            patch.object(pricing, "load_published_views", wraps=pricing.load_published_views) as full_reads,
            patch.object(
                pricing, "load_published_boundary", wraps=pricing.load_published_boundary
            ) as boundary_reads,
        ):
            result = self.case.run_build(symbols=["CURRENT"], client=client)["results"][0]
        self.assertFalse(result["reused_closed_boundaries"])
        self.assertEqual(owner.call_count, 1)
        self.assertEqual(
            [(name, "2026-10-01", "2026-10-15") for name in sorted(names)],
            client.calls,
        )
        self.assertEqual(5, client.request_count)
        self.assertEqual(full_reads.call_count, len(names))
        self.assertEqual(boundary_reads.call_count, 3 * len(names))
        self.assertEqual(partial_withheld, [5])
        for name in names:
            self.assertEqual(
                ["retained_suffix", "complete_history"],
                [scope for symbol, scope in materializations if symbol == name],
            )
        self.assertEqual(
            observed,
            [[(name, self.case.receipt(name)["source_sha256"], "complete_history") for name in sorted(names)]],
        )
        self.assertEqual(
            {item["corporate_action_id"] for item in completed_reports[0]["CURRENT"]["uuid_lineage_edges"]},
            {"old-current"},
        )
        correlation_inputs = [
            call.kwargs for call in events.call_args_list if call.args == ("lineage_correlation_inputs",)
        ]
        self.assertEqual([item["symbol"] for item in correlation_inputs], sorted(names))
        self.assertTrue(all(item["materialization_scope"] == "complete_history" for item in correlation_inputs))
        self.assertEqual(
            [item["selected_generation"] for item in correlation_inputs],
            [self.case.receipt(name)["source_sha256"] for name in sorted(names)],
        )
        self.assert_asof_current_history()

    def test_complete_group_evidence_change_restores_predecessor_donor_report_and_rows(self):
        from libs.alpaca_lineage_build import pricing

        names, client = self.prepare_predecessor_group_evidence_change()
        self.assertEqual(
            [
                "exact_independently_resolved_predecessor_prefix",
                "latest_dated_ownership_interval_overlapping_claims",
            ],
            [item["correlation_rule"]["name"] for item in self.case.assignments("OLD", adjustment="raw")],
        )
        self.assertEqual(
            ["asof_selected_uuid_lineage_interval", "asof_selected_uuid_lineage_interval"],
            [item["correlation_rule"]["name"] for item in self.case.assignments("NEXT", adjustment="raw")],
        )
        correlate = pipeline.correlate
        load_pricing = pipeline.load_pricing
        observed = []
        completed_reports = []
        materializations = []

        def selected_input(*args, **kwargs):
            view = load_pricing(*args, **kwargs)
            materializations.append((args[1], view.materialization_scope))
            return view

        def complete_inputs(store, inputs, evidence, **kwargs):
            observed.append(
                [(name, view.source_sha256, view.materialization_scope) for name, view in sorted(inputs.items())]
            )
            donor = inputs["NEXT"]
            donor_raw = donor.downloads["raw.csv.gz"]
            donor_descriptor = next(item for item in donor.local["downloads"] if item["adjustment"] == "raw")
            self.assertEqual(3, len(donor_raw.rows))
            self.assertEqual(donor_descriptor["rows"], len(donor_raw.rows))
            requests = client.request_count
            reports = correlate(store, inputs, evidence, **kwargs)
            self.assertEqual(requests, client.request_count)
            completed_reports.append(reports)
            return reports

        with (
            patch.object(pipeline, "load_pricing", side_effect=selected_input),
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("lineage requested provider pricing")
            ),
            patch.object(pipeline, "correlate", side_effect=complete_inputs) as owner,
            patch.object(pricing, "load_published_views", wraps=pricing.load_published_views) as full_reads,
            patch.object(
                pricing, "load_published_boundary", wraps=pricing.load_published_boundary
            ) as boundary_reads,
        ):
            result = self.case.run_build(symbols=["OLD"], client=client)["results"][0]
        self.assertFalse(result["reused_closed_boundaries"])
        self.assertEqual(owner.call_count, 1)
        self.assertEqual(
            [(name, "2026-10-01", "2026-10-15") for name in sorted(names)],
            client.calls,
        )
        self.assertEqual(9, client.request_count)
        self.assertEqual(full_reads.call_count, len(names))
        self.assertEqual(boundary_reads.call_count, 3 * len(names))
        for name in names:
            self.assertEqual(
                ["retained_suffix", "complete_history"],
                [scope for symbol, scope in materializations if symbol == name],
            )
        self.assertEqual(
            observed,
            [[(name, self.case.receipt(name)["source_sha256"], "complete_history") for name in sorted(names)]],
        )
        report = completed_reports[0]
        self.assertEqual(
            [
                {
                    "donor_symbol": "NEXT",
                    "corporate_action_id": "old-next",
                    "first_session": "2024-08-14",
                    "last_session": "2024-08-15",
                    "rows": 2,
                    "asset_id": "old1",
                    "cusip": "old-cusip1",
                    "exact_raw_prices_and_timestamps": True,
                    "all_adjustment_session_grids_match": True,
                    "independent_donor_rule": "asof_selected_uuid_lineage_interval",
                }
            ],
            report["OLD"]["predecessor_prefix_assessments"],
        )
        donor_raw_segments = [
            item for item in report["NEXT"]["correlated"]["segments"] if item["adjustment"] == "raw"
        ]
        self.assertEqual(
            [
                ("asof_selected_uuid_lineage_interval", 2, "OLD", "old1"),
                ("asof_selected_uuid_lineage_interval", 1, "NEXT", "new1"),
            ],
            [
                (
                    item["correlation_rule"]["name"],
                    item["row_count"],
                    item["historical_symbol"],
                    item["canonical_security"]["alpaca_asset_id"],
                )
                for item in donor_raw_segments
            ],
        )
        self.assertEqual(
            [
                "exact_independently_resolved_predecessor_prefix",
                "latest_dated_ownership_interval_overlapping_claims",
            ],
            [item["correlation_rule"]["name"] for item in self.case.assignments("OLD", adjustment="raw")],
        )

    def test_complete_group_fails_before_correlation_when_loader_stays_partial(self):
        names, client = self.prepare_asof_group_evidence_change()
        load = pipeline.load_pricing

        def incomplete_peer(*args, **kwargs):
            view = load(*args, **kwargs)
            if args[1] == "OLD" and kwargs.get("resume_from") is None:
                view.materialization_scope = "retained_suffix"
            return view

        with (
            patch.object(pipeline, "load_pricing", side_effect=incomplete_peer),
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("lineage requested provider pricing")
            ),
            patch.object(pipeline, "correlate", side_effect=AssertionError("partial group correlated")),
            patch.object(storage, "publish", side_effect=AssertionError("partial group published")),
        ):
            result = pipeline.run_lineage(self.case.store, ["CURRENT"], identity_client=client)
        self.assertEqual(result["failed"], 1, result)
        self.assertIn(
            "complete-group lineage requires complete pricing history for OLD", result["results"][0]["error"]
        )
        self.assertEqual(set(names), set(self.case.receipt("CURRENT")["input_sources"]))

    def test_complete_group_publication_recovery_does_not_repeat_accepted_work(self):
        names, client = self.prepare_asof_group_evidence_change()
        with (
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("lineage requested provider pricing")
            ),
            patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted publication")),
        ):
            failed = pipeline.run_lineage(self.case.store, ["CURRENT"], identity_client=client)
        self.assertEqual(failed["failed"], 1, failed)
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("recovery reloaded pricing")),
            patch.object(pipeline, "acquire", side_effect=AssertionError("recovery reacquired metadata")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("recovery repeated correlation")),
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("recovery requested provider pricing")
            ),
        ):
            recovered = pipeline.run_lineage(self.case.store, ["CURRENT"], identity_client=client)
        self.assertTrue(recovered["recovered"], recovered)
        self.assertEqual(recovered["failed"], 0, recovered)
        self.assertEqual(set(names), set(self.case.receipt("CURRENT")["input_sources"]))
        self.assert_asof_current_history()

    def test_resolved_dependency_changes_use_the_single_pipeline_and_match_refresh(self):
        from libs.alpaca_lineage_build import pricing

        def assigned_rows(name):
            # Continuation may retain a segment seam that a full publication
            # coalesces. Compare every row's accepted decisions, not run packing.
            ignored = {"row_start", "row_end", "row_count", "first_timestamp", "last_timestamp", "from", "to"}
            return [
                {key: value for key, value in segment.items() if key not in ignored}
                for segment in case.assignments(name)
                for _ in range(segment["row_count"])
            ]

        case = self.case
        names = ("GOOG", "OTHER")
        client = fixture.IdentityClient(assets={name: fixture.asset(name, "shared-id") for name in names})
        case.bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        for name in names:
            case.download(symbol=name)
        case.run_build(symbols=list(names), client=client)
        for name in names:
            self.assertEqual(case.receipt(name)["unresolved_rows"], 0)
            case.download(
                symbol=name,
                end="2026-10-15",
                bars=case.bars + [fixture.bar("2026-10-14")],
            )
        with patch.object(
            pricing, "load_published_views", side_effect=AssertionError("accepted group history reopened")
        ):
            changed = case.run_build(symbols=["GOOG"], client=client)
        self.assertTrue(changed["results"][0]["reused_closed_boundaries"])
        self.assertEqual(changed["results"][0]["pricing_rows_processed"], 16)
        assignments = {name: assigned_rows(name) for name in names}
        counts = {name: case.receipt(name)["source_rows"] for name in names}
        case.run_build(symbols=["GOOG"], client=client, refresh=True)
        self.assertEqual(assignments, {name: assigned_rows(name) for name in names})
        self.assertEqual(counts, {name: case.receipt(name)["source_rows"] for name in names})
        # Unequal coverage is a suffix fact, not permission to replace an already
        # accepted historical source decision. The new suffix still sees every
        # member's current evidence and the full accepted group topology.
        for name in names:
            case.download(
                symbol=name,
                end="2026-10-16",
                bars=case.bars
                + [fixture.bar("2026-10-14")]
                + ([fixture.bar("2026-10-15")] if name == "OTHER" else []),
            )
        with patch.object(
            pricing, "load_published_views", side_effect=AssertionError("accepted group history reopened")
        ) as reads:
            changed = case.run_build(symbols=["GOOG"], client=client)
        self.assertTrue(changed["results"][0]["reused_closed_boundaries"])
        self.assertEqual(changed["results"][0]["pricing_rows_processed"], 8)
        self.assertEqual(reads.call_count, 0)
        assignments = {name: assigned_rows(name) for name in names}
        case.run_build(symbols=["GOOG"], client=client, refresh=True)
        refreshed = {name: assigned_rows(name) for name in names}

        # Explicit refresh may choose the now-longer peer as descriptive source.
        # That is not authority for an ordinary append to replace the accepted
        # historical source selection; row ownership must remain identical.
        def without_selection(rows):
            return [{key: value for key, value in row.items() if key != "observation_selection"} for row in rows]

        for name in names:
            self.assertEqual(without_selection(assignments[name]), without_selection(refreshed[name]))

    def test_fresh_descriptive_asset_change_updates_suffix_without_reopening_history(self):
        from libs.alpaca_lineage_build import pricing
        from libs.alpaca_lineage_build.metadata import EvidenceClient

        case = self.case
        bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        original = fixture.asset("GOOG")
        case.download(bars=bars)
        first = fixture.IdentityClient({"GOOG": original})
        case.run_build(client=first)
        before = case.receipt()

        changed_asset = {**original, "name": "Renamed issuer", "status": "inactive", "tradable": False}
        case.download(end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        identity = fixture.IdentityClient({"GOOG": changed_asset})
        evidence = EvidenceClient(
            identity,
            observation={
                "records": [changed_asset],
                "catalogue_observed_at_utc": "2026-10-15T23:00:00Z",
            },
        )
        with patch.object(
            pricing, "load_published_views", side_effect=AssertionError("descriptive change reopened history")
        ):
            result = case.run_build(client=identity, evidence_client=evidence)["results"][0]
        self.assertTrue(result["reused_closed_boundaries"])
        self.assertEqual(result["historical_rows_reused"], before["source_rows"])
        self.assertEqual(result["pricing_rows_processed"], 8)
        lookup = case.receipt()["evidence"]["asset_lookups"][0]
        self.assertEqual(lookup["asset"]["name"], "Renamed issuer")
        self.assertEqual(lookup["asset"]["status"], "inactive")

    def test_empty_histories_reuse_decisions_when_only_coverage_advances(self):
        case = self.case
        case.download(bars=[])
        case.run_build()
        case.download(end="2027-01-06", bars=[])
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as owner:
            result = case.run_build()
        self.assertTrue(result["results"][0]["reused_decision"])
        self.assertEqual(owner.call_count, 0)
        self.assertEqual(case.receipt()["source_rows"], 0)
        self.assertEqual(case.receipt()["processed_through"][:10], "2027-01-06")

    def test_completed_query_membership_does_not_reopen_accepted_history(self):
        from libs.alpaca_lineage_build import pricing
        from libs.alpaca_lineage_build.resume import evidence_delta

        case = self.case
        bars = fixture.calendar_bars("2025-12-01", "2026-10-14")
        case.download(bars=bars)
        case.run_build()
        previous = case.receipt()
        # A previously completed negative related-symbol query disappears from
        # the next query projection. Its unchanged returned facts remain held.
        previous["evidence"]["queried_symbols"].append("UNAVAILABLE")
        with storage.writer_lock(case.store) as root:
            index = storage.read_index(root)
            storage.publish(case.store, root, index, {"GOOG": previous}, {"GOOG": {}}, {})
        before = self.decisions()
        case.download(end="2026-10-15", bars=bars + [fixture.bar("2026-10-14")])
        with patch.object(
            pricing, "load_published_views", side_effect=AssertionError("query bookkeeping reopened prices")
        ):
            result = case.run_build()["results"][0]
        self.assertTrue(result["reused_closed_boundaries"])
        self.assertEqual(result["pricing_rows_processed"], 8)
        for adjustment, rows in self.decisions().items():
            self.assertEqual(rows[:-1], before[adjustment])

        current = deepcopy(previous["evidence"])
        current["queried_symbols"].append("ANOTHER")
        self.assertEqual(evidence_delta(previous["evidence"], current, append=True)["scope"], "none")
        current["asset_lookups"][0]["asset"]["id"] = "changed-owner"
        self.assertEqual(evidence_delta(previous["evidence"], current, append=True)["scope"], "complete_group")

    def test_disjoint_append_preserves_mixed_source_rename_ownership_and_restart(self):
        from libs.alpaca_lineage_build import pricing
        from libs.alpaca_lineage_build.resume import append_history_invalidation, reusable_prefix
        from libs.alpaca_pricing_download.storage import load_source

        case = self.case
        current = fixture.asset("GOOG", "same-owner")
        action = {
            "id": "rename",
            "action_type": "name_change",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
            "process_date": "2026-10-01",
        }
        client = fixture.IdentityClient({"GOOG": current, "111111111": current}, [action])
        older = fixture.informative_bars("2026-09-29", "2026-10-01")
        case.download(bars=fixture.informative_bars("2026-09-28", "2026-10-14"))
        case.download(symbol="OLD", bars=older)
        case.run_build(symbols=["GOOG", "OLD"], client=client)
        prior = case.receipt()
        self.assertEqual(prior["unresolved_rows"], 8)
        self.assertGreater(prior["correlated_rows"], 0)
        before = self.decisions()
        self.assertTrue(any(row.get("historical_symbol") == "OLD" for row in before["raw"]))

        case.download(end="2026-10-15", bars=fixture.informative_bars("2026-09-28", "2026-10-15"))
        case.download(symbol="OLD", end="2026-10-15", bars=older)
        build = policy_engine.build_security_correlation_report

        def suffix_only(report, *args, **kwargs):
            for descriptor in report["local"]["downloads"]:
                rows = kwargs["authenticated_download_reader"](None, None, descriptor).rows
                self.assertTrue(all(row["t"][:10] == "2026-10-14" for row in rows))
            return build(report, *args, **kwargs)

        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("historical prices read")),
            patch.object(policy_engine, "build_security_correlation_report", side_effect=suffix_only),
            patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted publication")),
        ):
            failed = pipeline.run_lineage(case.store, ["GOOG"], identity_client=client)
        self.assertEqual(failed["failed"], 1)
        with (
            patch.object(pricing, "load_published_views", side_effect=AssertionError("recovery history read")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("recovery repeated decisions")),
        ):
            case.run_build(symbols=["GOOG"], client=client)
        for adjustment, rows in self.decisions().items():
            self.assertEqual(rows[:-1], before[adjustment])
        self.assertEqual(case.receipt()["unresolved_rows"], prior["unresolved_rows"])

        # A new peer observation overlapping that accepted history still owns
        # reconciliation; a changed pinned mapping cannot inherit this prefix.
        overlap = SimpleNamespace(views={"raw": (("2026-09-28T04:00:00Z",),)})
        self.assertIsNotNone(append_history_invalidation({"GOOG": prior}, {"PEER": overlap}))
        source = load_source(case.store / "end_of_day/pricing/GOOG", "GOOG")
        source["latest_request"]["asof"] = "2026-09-30"
        self.assertFalse(reusable_prefix(source, prior))

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
        self.assertFalse(stable_evidence(previous["evidence"], case.receipt()["evidence"]))
