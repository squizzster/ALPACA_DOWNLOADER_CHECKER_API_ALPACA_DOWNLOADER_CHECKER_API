"""Retained fact envelopes and memberships keep their accepted producer scopes."""

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_build as fixture
import test_alpaca_predecessor_prefix as prefix_fixture
from test_alpaca_public_common_continuation import common_fact
from test_alpaca_security_relations import conversion_fact

from libs.alpaca_end_of_day_security_correlation import correlation, row_correlation
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    catalogue,
    continuation,
    metadata,
    pipeline,
    pricing,
    resume,
    source_work,
    storage,
    transform,
    universe_state,
)
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage import event_supplements, sourced_facts


class RetainedFactEnvelopeTests(TestCase):
    def test_new_fact_order_matches_the_metadata_producer_without_retry(self):
        first, second = common_fact(), common_fact()
        first["match"]["source_id"], second["match"]["source_id"] = "z-last", "a-first"
        client = metadata.EvidenceClient(None, event_supplements=[first, second])
        evidence = {
            "query": {"input_symbol": first["facts"]["predecessor"]["symbol"]},
            "queried_symbols": [first["facts"]["predecessor"]["symbol"]],
            "corporate_actions": [],
            "asset_lookups": [],
        }
        # The actual producer consumes this ordering before attaching its fact
        # extension. A later applicability check must accept that first output.
        selected = sourced_facts.selected(client.all_event_supplements(), symbols=evidence["queried_symbols"])
        evidence["sourced_public_common_continuations"] = {"schema_version": 1, "facts": selected}
        self.assertEqual(["a-first", "z-last"], [item["match"]["source_id"] for item in selected])
        self.assertTrue(client.evidence_current(evidence))

    def test_conversion_recipient_lookup_does_not_expand_the_completed_fact_scope(self):
        client = metadata.EvidenceClient(None, event_supplements=[conversion_fact(), common_fact()])
        evidence = {
            "query": {"input_symbol": "FUTURE"},
            "queried_symbols": ["FUTURE"],
            "corporate_actions": [],
            "asset_lookups": [{"lookup_kind": "cusip", "lookup": "111111111"}],
        }
        selected = client.sourced_for(evidence)
        self.assertEqual(["distinct_security_conversion"], [item["kind"] for item in selected])

    def test_completed_group_fact_scope_survives_narrower_consumer_selection(self):
        fact = event_supplements.accept_supplements([common_fact()])[0]
        evidence = {
            "query": {"input_symbol": "UNRELATED"},
            "queried_symbols": ["UNRELATED"],
            "corporate_actions": [],
            "asset_lookups": [],
            "sourced_public_common_continuations": {"version": 1, "facts": [fact]},
        }
        client = metadata.EvidenceClient(None)
        client.retain_event_supplements([evidence])
        self.assertIs(fact, client.sourced_for(evidence)[0])
        changed = common_fact()
        changed["facts"]["legal_effective_date"] = "2024-01-03"
        newer = metadata.EvidenceClient(None, event_supplements=[changed])
        newer.retain_event_supplements([evidence])
        self.assertEqual("2024-01-03", newer.sourced_for(evidence)[0]["facts"]["legal_effective_date"])

    def test_known_prepared_envelope_retains_facts_without_reproving_them(self):
        fact = event_supplements.accept_supplements([common_fact()])[0]
        evidence = {"sourced_public_common_continuations": {"version": 1, "facts": [fact]}}
        client = metadata.EvidenceClient(None)
        with (
            patch.object(sourced_facts, "validate", side_effect=AssertionError("accepted fact revalidated")),
            patch.object(
                event_supplements, "_project_action", side_effect=AssertionError("accepted event replayed")
            ),
        ):
            client.retain_event_supplements([evidence])
            client.retain_event_supplements([evidence])
        self.assertIs(fact, client.all_event_supplements()[0])
        self.assertEqual({"version": 1, "facts": [fact]}, evidence["sourced_public_common_continuations"])

    def test_envelope_spelling_alone_cannot_reopen_accepted_owners(self):
        fact = event_supplements.accept_supplements([common_fact()])[0]
        old = {"sourced_public_common_continuations": {"version": 1, "facts": [fact]}}
        current = {"sourced_public_common_continuations": {"schema_version": 1, "facts": [fact]}}
        self.assertEqual("none", resume.evidence_delta(old, current, append=True)["scope"])
        changed_fact = common_fact()
        changed_fact["match"]["source_id"] = "another-fact"
        changed = {
            "sourced_public_common_continuations": {
                "schema_version": 1,
                "facts": list(event_supplements.accept_supplements([changed_fact])),
            }
        }
        self.assertEqual("complete_group", resume.evidence_delta(old, changed, append=True)["scope"])

    def test_unknown_empty_or_conflicting_envelopes_are_not_absence_authority(self):
        for envelope in (
            {},
            {"facts": []},
            {"version": True, "facts": []},
            {"version": 2, "facts": []},
            {"version": 1, "schema_version": 1, "facts": []},
            {"schema_version": 1, "facts": {}},
        ):
            with self.subTest(envelope=envelope), self.assertRaisesRegex(LineageError, "unsupported retained"):
                metadata.EvidenceClient(None).retain_event_supplements([{"sourced_market_occupancy": envelope}])
        accepted = {"sourced_market_occupancy": {"schema_version": 1, "facts": []}}
        client = metadata.EvidenceClient(None)
        client.retain_event_supplements([accepted])
        self.assertIs(accepted, client._retained_supplement_evidence[id(accepted)])


def identity(name, members):
    return {"security_id": name, "member_alpaca_asset_ids": members, "member_tickers": [name]}


class SourceMembershipScopeTests(TestCase):
    def test_shared_uuid_does_not_replace_either_sources_accepted_component(self):
        pinned, live = identity("pinned", ["old", "shared"]), identity("live", ["shared", "new"])
        previous = {
            "PINNED": {"identity_memberships": [pinned]},
            "LIVE": {"identity_memberships": [live]},
            "OTHER": {"identity_memberships": []},
        }
        with patch.object(
            source_work, "compose_memberships", side_effect=AssertionError("accepted components rejoined")
        ):
            result = continuation.scoped_identity_memberships(previous, {})
        self.assertIs(pinned, result["PINNED"]["shared"])
        self.assertIs(live, result["LIVE"]["shared"])
        self.assertNotIn("shared", result["OTHER"])
        self.assertIs(pinned, result["OTHER"]["old"])
        self.assertIs(live, result["OTHER"]["new"])

    def test_ambiguity_inside_one_source_still_requires_owning_recovery(self):
        previous = {"BAD": {"identity_memberships": [identity("one", ["shared"]), identity("two", ["shared"])]}}
        with self.assertRaisesRegex(LineageError, "membership is ambiguous"):
            continuation.scoped_identity_memberships(previous, {})

    def test_complete_positions_borrow_old_owners_without_timestamp_or_price_work(self):
        owner = identity("accepted", ["uuid"])

        def segment(day, position, branch):
            result = {
                "branch": branch,
                "row_start": position,
                "row_end": position,
                "row_count": 1,
                "from": day,
                "to": day,
                "first_timestamp": day + "T05:00:00Z",
                "last_timestamp": day + "T05:00:00Z",
            }
            if branch == "correlated":
                result.update(
                    security_identity=owner,
                    canonical_security={
                        "security_id": "accepted",
                        "alpaca_asset_id": "uuid",
                        "cusip": "111111111",
                    },
                    historical_symbol="SAME",
                    observation_selection="selected",
                    observation_tickers=["SAME"],
                )
            return result

        segments = {
            "completed_years": [segment("2023-12-29", 1, "correlated")],
            "current_year": [
                segment("2024-01-02", 1, "uncorrelated"),
                segment("2024-01-03", 2, "correlated"),
                segment("2024-02-01", 3, "correlated"),
            ],
            "current_month": [segment("2024-02-01", 1, "correlated")],
        }
        documents = {
            kind: {"adjustments": [{"adjustment": view, "segments": pieces} for view in ADJUSTMENTS]}
            for kind, pieces in segments.items()
        }
        prior = {
            "symbol": "SAME",
            "anchor_month": "2024-02",
            "source_rows": 32,
            "identity_memberships": [owner],
            "row_partition_complete": True,
            "evidence_issues": [],
            "ownership_interval_rule_order": [],
            "row_correlation_rule_order": [],
        }
        local = {
            "downloads": [
                {"file": view.replace(",", "_") + ".csv.gz", "adjustment": view} for view in ADJUSTMENTS
            ]
        }
        with (
            patch.object(source_work, "assignment_documents", return_value=documents),
            patch(
                "libs.alpaca_lineage_build.pricing.load_pricing", side_effect=AssertionError("old prices opened")
            ),
        ):
            report = source_work.retained_ownership_report(prior, local, {"uuid": owner}, None, {})
        self.assertEqual(24, report["correlated"]["row_count"])
        self.assertEqual(8, report["uncorrelated"]["row_count"])
        raw = [segment for segment in report["correlated"]["segments"] if segment["adjustment"] == "raw"]
        self.assertEqual([(1, 1), (3, 3), (4, 4)], [(s["row_start"], s["row_end"]) for s in raw])
        self.assertTrue(all(s["observation_selection"] == "selected" for s in raw))
        self.assertTrue(all("download_sha256" not in item for item in report["source_downloads"]))


class RetainedPeerProvenanceTests(TestCase):
    def setUp(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.store = case.store
        self.inputs = {}
        bars = [{**fixture.bar(row["t"][:10], row["c"]), "t": row["t"]} for row in prefix_fixture.rows()]
        for name in ("OLD", "NEXT"):
            case.download(name, end="2026-10-14", bars=bars)
            self.inputs[name] = pricing.load_pricing(case.store, name)
        self.prefix = prefix_fixture.PredecessorPrefixTests()
        self.prefix.setUp()
        owner = security_identity_for_alpaca_assets(
            ["old1"], [], tickers=["OLD"], basis="single_alpaca_asset_id"
        ).as_report_object()
        successor = security_identity_for_alpaca_assets(
            ["new1"], [], tickers=["NEXT"], basis="single_alpaca_asset_id"
        ).as_report_object()
        report = {
            "correlated": {"row_count": 24, "segments": []},
            "uncorrelated": {"row_count": 0, "segments": []},
        }
        view = self.inputs["NEXT"]
        for adjustment in ADJUSTMENTS:
            for start, end, symbol, asset_id, cusip, identity in (
                (1, 2, "OLD", "old1", "old-cusip1", owner),
                (3, 3, "NEXT", "new1", "new-cusip1", successor),
            ):
                report["correlated"]["segments"].append(
                    {
                        "adjustment": adjustment,
                        "row_start": start,
                        "row_end": end,
                        "row_count": end - start + 1,
                        "first_timestamp": view.views[adjustment][start - 1][0],
                        "last_timestamp": view.views[adjustment][end - 1][0],
                        "historical_symbol": symbol,
                        "canonical_security": {
                            "security_id": identity["security_id"],
                            "alpaca_asset_id": asset_id,
                            "cusip": cusip,
                        },
                        "security_identity": identity,
                        "correlation_rule": {"name": "asof_selected_uuid_lineage_interval"},
                    }
                )
        indexes = transform._row_indexes(view)
        self.documents = {
            kind: transform.boundary(view, report, kind, view.source["anchor_month"], indexes)
            for kind in ("completed_years", "current_year", "current_month")
        }
        self.prior = prior = {
            "symbol": "NEXT",
            "anchor_month": view.source["anchor_month"],
            "source_rows": 24,
            "identity_memberships": [owner, successor],
            "evidence_issues": [],
            "row_partition_complete": True,
            "ownership_interval_rule_order": [],
            "row_correlation_rule_order": [],
        }
        for module, name, reason in (
            (storage, "read_receipt", "accepted receipt reopened"),
            (pricing, "load_source", "pricing selector reopened"),
            (pricing, "load_published_boundary", "old prices reopened"),
            (pricing, "document_digest", "pricing digest recalculated"),
            (fixture.FakeClient, "fetch", "provider requested"),
        ):
            self.enterContext(patch.object(module, name, side_effect=AssertionError(reason)))
        decide = row_correlation._decide_row

        def fresh_only(context, **kwargs):
            self.assertEqual("OLD", context.requested_symbol, "accepted donor ownership replayed")
            return decide(context, **kwargs)

        self.enterContext(patch.object(row_correlation, "_decide_row", side_effect=fresh_only))
        with patch.object(source_work, "assignment_documents", return_value=self.documents) as assemble:
            view.accepted_report = source_work.retained_ownership_report(
                prior, view.local, {"old1": owner, "new1": successor}, None, {}
            )
        self.assertEqual(1, assemble.call_count)
        self.enterContext(
            patch.object(source_work, "assignment_documents", side_effect=AssertionError("assignments reopened"))
        )

    def test_source_work_binds_retained_owners_to_its_materialized_downloads(self):
        view = self.inputs["NEXT"]
        control = pricing.metadata_source_control(self.store, "NEXT", view.source, view.source_sha256, None)
        control.previous = self.prior
        control.local["counts_known"] = True
        empty = {"version": 1, "certificates": [], "issues": []}
        plan = source_work.SourceWorkPlan({"NEXT": "B"}, {}, empty, empty, {}, None, {}, False)

        def consume(retained, *_args):
            self.assertIs(view.local, retained.local, "materialized provenance was discarded")
            raise RuntimeError("retained report ingress")

        with patch.object(source_work, "accepted_report", side_effect=consume):
            with self.assertRaisesRegex(RuntimeError, "retained report ingress"):
                source_work._decide_source_work(
                    self.store,
                    self.store / "end_of_day/lineage",
                    {"NEXT": control},
                    {"NEXT": {}},
                    {},
                    plan,
                    None,
                    {},
                    loader=lambda *_args, **_kwargs: view,
                )

    def correlate(self):
        cache = row_correlation.CorrelationCache()
        _, report_loader, rows_loader = pricing.rule_loaders(
            self.inputs, {"NEXT": self.inputs["NEXT"].accepted_report}, cache
        )
        work = [
            (download, correlation._source_context(download), cache.session_dates(download.rows))
            for download in self.inputs["OLD"].downloads.values()
        ]
        proofs = prefix_fixture.assess_predecessor_prefixes(
            "OLD",
            self.prefix.lineage,
            self.prefix.intervals,
            self.prefix.edges,
            work,
            correlation_loader=report_loader,
            source_rows_loader=rows_loader,
        )
        # The fresh owner consumes the completed certificate; the retained
        # donor's row cascade is never entered.
        result = row_correlation.correlate_download_rows(
            work[0][0].rows,
            work[0][1],
            self.prefix.lineage,
            self.prefix.intervals,
            predecessor_prefixes=proofs,
        )
        return proofs, result

    def test_fresh_peer_consumes_retained_donor_provenance_without_reauthentication(self):
        proofs, result = self.correlate()
        self.assertEqual(1, len(proofs))
        self.assertEqual(2, result["correlated"][0]["row_count"])
        self.assertEqual("old1", result["correlated"][0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(1, result["uncorrelated"][0]["row_count"])

    def test_changed_donor_generation_cannot_supply_the_retained_certificate(self):
        for download in self.inputs["NEXT"].downloads.values():
            download.manifest = {**download.manifest, "sha256": "changed-generation"}
        proofs, result = self.correlate()
        self.assertEqual((), proofs)
        self.assertEqual([], result["correlated"])

    def test_peer_failure_keeps_the_completed_retained_report_for_retry(self):
        accepted = self.inputs["NEXT"].accepted_report
        loaders = pricing.rule_loaders

        def interrupted(*args):
            sessions, report, _ = loaders(*args)

            def rows(*_args):
                raise RuntimeError("peer interrupted")

            return sessions, report, rows

        with patch.object(pricing, "rule_loaders", side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, "peer interrupted"):
                self.correlate()
        proofs, result = self.correlate()
        self.assertIs(accepted, self.inputs["NEXT"].accepted_report)
        self.assertEqual(1, len(proofs))
        self.assertEqual(2, result["correlated"][0]["row_count"])


class AcceptedEmptyCoverageTests(TestCase):
    def view(self, previous_end):
        rows = (("2026-11-10T05:00:00Z",),)
        return SimpleNamespace(
            views=dict.fromkeys(ADJUSTMENTS, rows),
            previous_assignment_summary={
                name: {"last_session": "2023-02-03", "rows": 10} for name in ADJUSTMENTS
            },
            previous_coverage_end=previous_end,
            source={"latest_request": {"end_exclusive": "2026-11-11T00:00:00Z"}},
            correction_from=None,
        )

    def test_only_the_new_interval_enters_the_calendar(self):
        view = self.view("2026-11-10T00:00:00Z")
        calendar = resume.expected_market_session_coverage
        calls = []

        def new_only(start, end):
            self.assertEqual(view.previous_coverage_end, start)
            calls.append((start, end))
            return calendar(start, end)

        with patch.object(resume, "expected_market_session_coverage", side_effect=new_only):
            self.assertTrue(resume.complete_append(view))
        self.assertEqual(1, len(calls))  # Exact alias views consume the same completed grid.

    def test_a_gap_inside_new_coverage_still_selects_owning_work(self):
        self.assertFalse(resume.complete_append(self.view("2026-11-09T00:00:00Z")))

    def test_missing_coverage_cannot_trigger_a_historical_reconstruction(self):
        with patch.object(
            resume, "expected_market_session_coverage", side_effect=AssertionError("old grid audited")
        ):
            self.assertFalse(resume.complete_append(self.view(None)))


class CompletedCatalogueRecoveryTests(TestCase):
    def test_selected_observation_survives_restart_without_catalogue_reacceptance(self):
        from test_alpaca_lineage_build import asset

        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            first = catalogue.completed_catalogue([asset("FIRST")], {"steps": []})
            universe_state.accept(store, first, [], {})
            reference = universe_state.read_state(store)["last_catalogue_observation"]
            universe_state.accept(store, catalogue.completed_catalogue([asset("LATER")], {"steps": []}), [], {})
            with (
                patch.object(catalogue, "completed_catalogue", side_effect=AssertionError("catalogue replay")),
                patch.object(catalogue, "document_digest", side_effect=AssertionError("digest replay")),
            ):
                restored = universe_state.read_observation(store, reference)
                client = metadata.EvidenceClient(None, observation=restored)
            self.assertEqual(first, restored)
            self.assertTrue(client.catalogue_attempted)
            self.assertIn("FIRST", client.catalogue)
            self.assertNotIn("LATER", client.catalogue)
            with self.assertRaisesRegex(LineageError, "changed or is missing"):
                universe_state.read_observation(store, {**reference, "observed_at_utc": "changed"})
            with self.assertRaisesRegex(LineageError, "changed or is missing"):
                universe_state.read_observation(store, {**reference, "id": 999})


class RetainedMetadataScopeTests(TestCase):
    def test_recovery_keeps_completed_query_when_it_covers_the_selected_observation_dates(self):
        evidence = {
            "asset_lookups": [],
            "query": {
                "corporate_actions_data_quality": "complete",
                "corporate_actions_start": "2016-01-01",
                "corporate_actions_end": "2026-11-10",
            },
        }
        source = {
            "history_start": "2016-01-01T00:00:00Z",
            "latest_request": {
                "end_exclusive": "2026-11-11T00:00:00Z",
            },
        }
        view = SimpleNamespace(source=source)
        client = SimpleNamespace(evidence_current=lambda value: value is evidence)
        with patch.object(metadata, "acquire", side_effect=AssertionError("accepted metadata replayed")):
            self.assertIs(evidence, metadata.retained_metadata(view, client, {"evidence": evidence}))
            self.assertIsNone(metadata.retained_metadata(view, client, {"evidence": evidence}, refresh=True))
            source["latest_request"]["end_exclusive"] = "2026-11-12T00:00:00Z"
            self.assertIsNone(metadata.retained_metadata(view, client, {"evidence": evidence}))

    def test_changed_metadata_projection_keeps_actual_query_bounds(self):
        evidence = {
            "asset_lookups": [],
            "query": {
                "corporate_actions_data_quality": "complete",
                "corporate_actions_start": "2016-01-01",
                "corporate_actions_end": "2026-11-10",
            },
        }
        view = SimpleNamespace(
            source={
                "history_start": "2016-01-01T00:00:00Z",
                "latest_request": {
                    "end_exclusive": "2026-11-11T00:00:00Z",
                },
            }
        )
        client = SimpleNamespace(evidence_current=lambda value: False)
        with patch.object(metadata, "acquire", return_value={"new": "projection"}) as owner:
            self.assertEqual(
                {"new": "projection"}, metadata.retained_metadata(view, client, {"evidence": evidence})
            )
        self.assertEqual(("2016-01-01", "2026-11-10"), owner.call_args.kwargs["query_dates"])
        self.assertIs(evidence, owner.call_args.args[1].evidence)


class SourceScopePipelineTests(TestCase):
    def test_interruption_after_correlation_consumes_its_completed_group_directly(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        rows = fixture.informative_bars("2026-10-01", "2026-10-05")
        case.download(bars=rows)
        case.run_build(symbols=["GOOG"])
        added = fixture.informative_bars("2026-10-14", "2026-10-15")
        case.download(end="2026-10-15", bars=rows + added)
        accepted = {}

        def interrupt(**kwargs):
            accepted.update(kwargs)
            raise RuntimeError("interruption after completed correlation")

        with patch.object(pipeline, "finish_correlated_group", side_effect=interrupt):
            stopped = pipeline.run_lineage(case.store, ["GOOG"], identity_client=fixture.IdentityClient())
        self.assertEqual(1, stopped["failed"])
        self.assertTrue(accepted["reports"])
        with (
            storage.writer_lock(case.store),
            storage.pricing_locks(case.store, {"GOOG"}, known=accepted["index"]["symbols"]),
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed correlation repeated")),
            patch.object(pipeline, "acquire", side_effect=AssertionError("completed metadata repeated")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
        ):
            result = pipeline.finish_correlated_group(**accepted)
        self.assertEqual("processed", result["status"])
        self.assertEqual(8, result["pricing_rows_processed"])
        self.assertEqual((len(rows) + 1) * 8, case.receipt()["source_rows"])

    def test_peer_append_keeps_differently_scoped_accepted_components_and_old_choices(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        assets = {name: fixture.asset(name, "shared") for name in ("GOOG", "PEER")}
        histories = {
            "GOOG": fixture.informative_bars("2026-10-01", "2026-10-05"),
            "PEER": fixture.informative_bars("2026-10-05", "2026-10-07"),
        }
        for name, rows in histories.items():
            case.download(symbol=name, bars=rows)
        case.run_build(symbols=list(histories), client=fixture.IdentityClient(assets))
        previous = {name: case.receipt(name) for name in histories}
        with storage.writer_lock(case.store) as root:
            index = storage.read_index(root)
            with storage.pricing_locks(case.store, {"GOOG"}, known=index["symbols"]):
                source = pricing.load_source(case.store / "end_of_day/pricing/GOOG", "GOOG")
                control = pricing.metadata_source_control(
                    case.store, "GOOG", source, previous["GOOG"]["source_sha256"], previous["GOOG"]
                )
                control.previous_receipt_generation = index["symbols"]["GOOG"]["sha256"]
                owner = security_identity_for_alpaca_assets(
                    ["earlier", "shared"], [], tickers=["GOOG", "PEER"], basis="accepted_component_fixture"
                ).as_report_object()
                empty = {"version": 1, "certificates": [], "issues": []}
                plan = source_work.SourceWorkPlan(
                    {"GOOG": "A"},
                    dict.fromkeys(owner["member_alpaca_asset_ids"], owner),
                    empty,
                    empty,
                    {},
                    previous["GOOG"]["security_relations"],
                    {},
                    True,
                )
                receipts, documents, _, _ = source_work.execute_source_work(
                    case.store,
                    root,
                    {"GOOG": control},
                    {"GOOG": previous["GOOG"]["evidence"]},
                    {},
                    plan,
                    load_authorized_policy(),
                    {},
                    loader=lambda *a, **k: self.fail("accepted prices opened by membership producer"),
                )
                storage.publish(
                    case.store,
                    root,
                    index,
                    receipts,
                    documents,
                    {},
                    previous={"GOOG": previous["GOOG"]},
                    sources={"GOOG": source},
                )
        accepted = {name: case.receipt(name) for name in histories}
        self.assertNotEqual(
            accepted["GOOG"]["identity_memberships"][0]["security_id"],
            accepted["PEER"]["identity_memberships"][0]["security_id"],
        )
        old_choices = {name: case.assignments(name) for name in histories}
        for name, rows in histories.items():
            added = fixture.informative_bars("2026-10-14", "2026-10-15") if name == "PEER" else []
            case.download(symbol=name, end="2026-10-15", bars=rows + added)
        decide = row_correlation._decide_row
        decisions = []

        def new_only(context, **kwargs):
            self.assertEqual("2026-10-14", context.session_date.isoformat())
            decisions.append(context.session_date)
            return decide(context, **kwargs)

        with patch.object(row_correlation, "_decide_row", side_effect=new_only):
            result = case.run_build(symbols=["GOOG"], client=fixture.IdentityClient(assets))
        self.assertEqual(0, result["failed"], result)
        self.assertTrue(decisions)
        for name in histories:
            current = case.receipt(name)
            self.assertEqual(accepted[name]["identity_memberships"], current["identity_memberships"])
            after = case.assignments(name)
            self.assertEqual(old_choices[name], [item for item in after if item["to"] < "2026-10-14"])
