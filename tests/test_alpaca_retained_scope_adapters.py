"""Retained observations and closed memberships keep their producer scopes."""

import copy
from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_build as fixture
import test_alpaca_predecessor_prefix as prefix_fixture
from test_alpaca_end_of_day_security_correlation import asset, complete_query, correlation_source
from test_alpaca_pipeline_codec import roundtrip
from test_alpaca_public_common_continuation import common_fact
from test_alpaca_security_relations import conversion_fact

from libs.alpaca_end_of_day_security_correlation import correlation, row_correlation
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import TickerOwnershipInterval
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    catalogue,
    continuation,
    metadata,
    pipeline,
    policy_engine,
    pricing,
    resume,
    source_work,
    source_work_contract,
    storage,
    transform,
)
from libs.alpaca_lineage_build.contract import LineageError, RecoveryRequired, failure_outcome
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage import event_supplements, sourced_facts


class RetainedFactEnvelopeTests(TestCase):
    def retained(self, *, symbol="UNRELATED", field="sourced_public_common_continuations", envelope=None):
        from libs.alpaca_security_lineage.lineage import report_from_observations

        result = report_from_observations(
            symbol,
            {"present": True, "downloads": [], "counts_known": False},
            [],
            [],
            [symbol],
            start_date="2024-01-01",
            end_date="2024-01-10",
        )
        result[field] = envelope if envelope is not None else {"version": 1, "facts": [common_fact()]}
        return result

    def test_new_factual_inputs_reject_before_ordering_or_projection(self):
        first, second = common_fact(), common_fact()
        first["match"]["source_id"], second["match"]["source_id"] = "z-last", "a-first"
        with (
            patch.object(sourced_facts, "selected", side_effect=AssertionError("fact selection")),
            patch.object(event_supplements, "_project_action", side_effect=AssertionError("event projection")),
            self.assertRaisesRegex(LineageError, "inadmissible"),
        ):
            metadata.EvidenceClient(None, event_supplements=[first, second])

    def test_conversion_recipient_lookup_cannot_admit_researched_scope(self):
        with self.assertRaisesRegex(LineageError, "inadmissible"):
            metadata.EvidenceClient(None, event_supplements=[conversion_fact(), common_fact()])

    def test_narrower_consumer_keeps_raw_observation_scope_and_closed_provenance(self):
        original = self.retained()
        unchanged = copy.deepcopy(original)
        client = metadata.EvidenceClient(None)
        projected = client.provider_view(original)
        self.assertEqual(["UNRELATED"], projected["queried_symbols"])
        self.assertEqual([], projected["asset_lookups"])
        self.assertNotIn("sourced_public_common_continuations", projected)
        self.assertEqual(unchanged, original)

    def test_completed_provider_projection_is_cached_without_fact_reproof(self):
        original = self.retained()
        client = metadata.EvidenceClient(None)
        with (
            patch.object(sourced_facts, "validate", side_effect=AssertionError("accepted fact revalidated")),
            patch.object(event_supplements, "_project_action", side_effect=AssertionError("event replayed")),
        ):
            projected = client.provider_view(original)
        with patch(
            "libs.alpaca_security_lineage.lineage.report_from_observations",
            side_effect=AssertionError("provider projection replayed"),
        ):
            self.assertIs(projected, client.provider_view(original))
        self.assertEqual(
            {"version": 1, "facts": [common_fact()]}, original["sourced_public_common_continuations"]
        )

    def test_research_envelope_changes_do_not_reopen_accepted_provider_owners(self):
        old = self.retained()
        current = self.retained(envelope={"schema_version": 1, "facts": [common_fact()]})
        changed = copy.deepcopy(current)
        changed["sourced_public_common_continuations"]["facts"][0]["match"]["source_id"] = "another-fact"
        client = metadata.EvidenceClient(None)
        before = client.provider_view(old)
        for retained in (current, changed):
            after = client.provider_view(retained)
            self.assertEqual("none", resume.evidence_delta(before, after, append=True)["scope"])

    def test_malformed_research_envelopes_are_never_runtime_absence_authority(self):
        for envelope in (
            {},
            {"facts": []},
            {"version": True, "facts": []},
            {"version": 2, "facts": []},
            {"version": 1, "schema_version": 1, "facts": []},
            {"schema_version": 1, "facts": {}},
        ):
            original = self.retained(field="sourced_market_occupancy", envelope=envelope)
            unchanged = copy.deepcopy(original)
            with (
                self.subTest(envelope=envelope),
                patch.object(
                    sourced_facts, "validate", side_effect=AssertionError("research envelope interpreted")
                ),
            ):
                projected = metadata.EvidenceClient(None).provider_view(original)
                self.assertFalse(projected.get("sourced_market_occupancy"))
                self.assertEqual([], projected["corporate_actions"])
                self.assertEqual(unchanged, original)
        missing_raw = self.retained(field="event_supplements", envelope=[{"legacy": True}])
        with self.assertRaisesRegex(ValueError, "raw provider observations"):
            metadata.EvidenceClient(None).provider_view(missing_raw)


def identity(name, members):
    return {"security_id": name, "member_alpaca_asset_ids": members, "member_tickers": [name]}


class SourceMembershipScopeTests(TestCase):
    def dated_case(self, *, departure="2024-01-12"):
        first, last = identity("first", ["shared"]), identity("later", ["shared", "new"])
        route = {
            "asset_id": "shared",
            "historical_symbol": "CLOSED",
            "start_date_inclusive": "2024-01-08",
            "end_date_exclusive": departure,
        }
        local = {
            "identity_memberships": [first, last],
            "identity_intervals": [
                {"security_id": "first", "from": "2024-01-03", "to": "2024-01-04"},
                {"security_id": "later", "from": "2024-01-08", "to": "2024-01-09"},
            ],
            "assignment_summary": {"raw": {"last_session": "2024-01-09"}},
            "observation_routes": {"later": ["2024-01-09T05:00:00Z", route]},
        }
        previous = {"CLOSED": local, "LIVE": {"identity_memberships": [last]}, "PEER": {}}
        views = {
            name: pricing.MetadataSourceControl("/test", name, {}, name, {}, prior)
            for name, prior in previous.items()
        }
        members = continuation.scoped_identity_memberships(previous, {}, inputs=views)
        for name, mapping in members.items():
            views[name].accepted_identity_memberships = mapping
        return previous, views

    def test_disjoint_accepted_components_stay_local_and_split_otherwise_identical_row_decisions_on_dates(self):
        prior, views = self.dated_case()
        earlier, later = prior["CLOSED"]["identity_memberships"]
        self.assertNotIn("shared", views["CLOSED"].accepted_identity_memberships)
        self.assertNotIn("shared", views["PEER"].accepted_identity_memberships)
        self.assertIs(views["LIVE"].accepted_identity_memberships["shared"], later)
        self.assertIs(views["PEER"].accepted_identity_memberships["new"], later)
        with patch.object(
            continuation, "_source_memberships", side_effect=AssertionError("repeated producer decision")
        ):
            views, earlier, later = roundtrip((views, earlier, later))
        cache = row_correlation.CorrelationCache()
        policy_engine._register_sources(cache, views)
        ticker = "CLOSED"
        issuer = asset("shared", ticker, "Synthetic security")
        evidence = {
            "query": complete_query(ticker),
            "corporate_actions": [],
            "identities": [issuer],
            "asset_lookups": [{"lookup": ticker, "lookup_kind": "symbol", "found": True, "asset": issuer}],
            "symbol_occupancy_evidence": [],
        }
        interval = TickerOwnershipInterval(ticker, "111111111", "shared", None, None, ())
        days = ("03", "04", "05", "08", "09", "10", "11", "12")
        rows = [{"t": "2024-01-" + day + "T17:00:00Z"} for day in days]
        result = row_correlation.correlate_download_rows(
            rows, correlation_source(ticker, asof="2024-01-12"), evidence, [interval], cache=cache
        )
        self.assertEqual(result["uncorrelated"], [])
        parts = result["correlated"]
        self.assertEqual([(s["row_start"], s["row_end"]) for s in parts], [(1, 2), (3, 3), (4, 7), (8, 8)])
        self.assertIs(parts[0]["security_identity"], earlier)
        self.assertIs(parts[2]["security_identity"], later)
        self.assertEqual(parts[1]["security_identity"], parts[-1]["security_identity"])
        self.assertNotIn(parts[1]["security_identity"]["security_id"], {"first", "later"})
        # No owner is manufactured when the ordinary dated row rules cannot
        # assign a provider UUID, even on an established membership date.
        no_asset = {
            "query": complete_query(ticker),
            "corporate_actions": [],
            "identities": [],
            "asset_lookups": [],
        }
        unresolved = row_correlation.correlate_download_rows(
            rows[:1], correlation_source(ticker, asof="2024-01-12"), no_asset, [], cache=cache
        )
        self.assertEqual(unresolved["correlated"], [])
        self.assertEqual(unresolved["uncorrelated"][0]["row_count"], 1)

    def test_open_route_extends_only_the_latest_actual_own_symbol_uuid_observation(self):
        for changed in (
            "open",
            "another_uuid",
            "another_label",
            "earlier_observation",
            "later_other_source_owner",
        ):
            with self.subTest(changed=changed):
                prior, views = self.dated_case(departure=None)
                source = prior["CLOSED"]
                route = source["observation_routes"]["later"]
                if changed == "another_uuid":
                    route[1]["asset_id"] = "new"
                elif changed == "another_label":
                    route[1]["historical_symbol"] = "OTHER"
                elif changed == "earlier_observation":
                    route[0] = "2024-01-08T05:00:00Z"
                elif changed == "later_other_source_owner":
                    source["assignment_summary"]["raw"]["last_session"] = "2024-01-10"
                continuation.scoped_identity_memberships(prior, {}, inputs=views)
                periods = views["CLOSED"].accepted_dated_identity_memberships["shared"]
                self.assertEqual(periods[-1][1], date.max if changed == "open" else date(2024, 1, 9))

    def test_empty_peer_suffix_never_assembles_old_date_scopes_or_lends_a_conflicted_uuid(self):
        prior, views = self.dated_case()
        closed = views["CLOSED"]
        closed.materialization_scope = "retained_suffix"
        closed.views = dict.fromkeys(ADJUSTMENTS, ())
        with patch.object(
            continuation, "identity_intervals", side_effect=AssertionError("opened closed membership dates")
        ):
            memberships = continuation.scoped_identity_memberships(prior, {}, inputs=views)
            no_closed_input = continuation.scoped_identity_memberships(prior, {}, inputs={"LIVE": views["LIVE"]})
        self.assertFalse(hasattr(closed, "accepted_dated_identity_memberships"))
        self.assertNotIn("shared", memberships["CLOSED"])
        self.assertNotIn("shared", memberships["PEER"])
        self.assertEqual("later", memberships["LIVE"]["shared"]["security_id"])
        self.assertEqual(memberships, no_closed_input)

    def test_frozen_positions_and_observation_routes_keep_both_ids_on_every_view(self):
        previous, views = self.dated_case()
        prior = previous["CLOSED"]
        owners = prior["identity_memberships"]
        prior.update(
            symbol="CLOSED",
            anchor_month="2024-01",
            source_rows=16,
            evidence_issues=[],
            row_partition_complete=True,
            ownership_interval_rule_order=[],
            row_correlation_rule_order=[],
        )
        early = {"asset_id": "shared", "historical_symbol": "CLOSED", "end_date_exclusive": "2024-01-05"}
        prior["observation_routes"]["first"] = ["2024-01-04T05:00:00Z", early]
        segments = []
        for index, (day, owner) in enumerate(zip(("04", "09"), owners, strict=True), 1):
            session = "2024-01-" + day
            segments.append(
                {
                    "from": session,
                    "to": session,
                    "branch": "correlated",
                    "row_count": 1,
                    "row_start": index,
                    "row_end": index,
                    "security_identity": owner,
                    "canonical_security": {"security_id": owner["security_id"], "alpaca_asset_id": "shared"},
                    "historical_symbol": "CLOSED",
                    "observation_selection": "selected",
                }
            )
        documents = {
            "current_month": {
                "adjustments": [{"adjustment": view, "segments": segments} for view in ADJUSTMENTS]
            }
        }
        local = {"downloads": [{"adjustment": view, "file": view + ".gz"} for view in ADJUSTMENTS]}
        with (
            patch.object(source_work, "assignment_documents", return_value=documents),
            patch.object(pricing, "load_pricing", side_effect=AssertionError("opened price history")),
            patch.object(row_correlation, "_decide_row", side_effect=AssertionError("replayed accepted owner")),
        ):
            report = source_work.retained_ownership_report(
                prior, local, views["CLOSED"].accepted_identity_memberships, None, {}
            )
        for view in ADJUSTMENTS:
            parts = [s for s in report["correlated"]["segments"] if s["adjustment"] == view]
            self.assertEqual(
                [
                    (
                        p["row_start"],
                        p["security_identity"]["security_id"],
                        p["canonical_security"]["security_id"],
                    )
                    for p in parts
                ],
                [(1, "first", "first"), (2, "later", "later")],
            )
            self.assertTrue(all(p["accepted_row_ownership"] for p in parts))
        self.assertEqual(report["retained_observation_routes"], prior["observation_routes"])

    def test_closed_metadata_projection_preserves_accepted_public_ids_without_opening_assignment_bodies(self):
        for scope in ("retained", "B", "scoped_refinement"):
            with self.subTest(scope=scope):
                previous, views = self.dated_case()
                prior, control = previous["CLOSED"], views["CLOSED"]
                first, later = prior["identity_memberships"]
                control.source = {"boundaries": {}, "latest_request": {"end_exclusive": "2024-01-25T00:00:00Z"}}
                held = {"correlated": {"segments": [{"security_identity": first}, {"security_identity": later}]}}
                control.accepted_report = held
                plan = source_work_contract.SourceWorkPlan(
                    {"CLOSED": "A" if scope == "scoped_refinement" else scope},
                    {"shared": later, "new": later},
                    {},
                    {},
                    {},
                    {},
                    {},
                    False,
                    prepared_reports={"CLOSED": held},
                    membership_refinement={"changes": {"other-source": "selected"}}
                    if scope == "scoped_refinement"
                    else None,
                )
                with (
                    patch.object(
                        source_work,
                        "assignment_documents",
                        side_effect=AssertionError("opened completed assignments"),
                    ),
                    patch.object(pricing, "load_pricing", side_effect=AssertionError("opened accepted prices")),
                ):
                    closed = source_work.closed_source(control, plan)
                    receipt, documents = source_work.project_closed_source(
                        closed, {}, plan, SimpleNamespace(provenance=lambda: {}), None, {}
                    )
                self.assertNotIn("shared", closed.accepted_identity_memberships)
                self.assertIs(held, closed.accepted_report)
                self.assertEqual(receipt["identity_memberships"], [first, later])
                self.assertIs(receipt["observation_routes"], prior["observation_routes"])
                self.assertIs(receipt["identity_intervals"], prior["identity_intervals"])
                self.assertEqual(documents, {})

    def test_missing_or_overlapping_selected_date_bounds_use_typed_internal_recovery(self):
        for kind in ("missing", "overlap"):
            with self.subTest(kind=kind):
                previous, views = self.dated_case()
                if kind == "overlap":
                    previous["CLOSED"]["identity_intervals"][0]["to"] = "2024-01-08"
                else:
                    previous["CLOSED"]["identity_intervals"].pop(0)
                with self.assertRaisesRegex(RecoveryRequired, "accepted UUID membership is ambiguous") as raised:
                    continuation.scoped_identity_memberships(previous, {}, inputs=views)
                self.assertEqual(failure_outcome(raised.exception)["outcome"], "recovery_required")

    def test_shared_uuid_does_not_replace_either_sources_accepted_component(self):
        pinned, live = identity("pinned", ["old", "shared"]), identity("live", ["shared", "new"])
        previous = {
            "PINNED": {"identity_memberships": [pinned]},
            "LIVE": {"identity_memberships": [live]},
            "OTHER": {"identity_memberships": []},
        }
        with patch.object(
            source_work_contract,
            "compose_memberships",
            side_effect=AssertionError("accepted components rejoined"),
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


class CatalogueEvidenceLifetimeTests(TestCase):
    def test_metadata_uses_the_held_observation_and_a_new_operation_observes_again(self):
        from test_alpaca_universe_update import CatalogueClient

        provider = CatalogueClient([fixture.asset("FIRST")])
        first = catalogue.observe_catalogue(provider)
        provider.records[:] = [fixture.asset("LATER")]
        with (
            patch.object(catalogue, "completed_catalogue", side_effect=AssertionError("catalogue replay")),
            patch.object(provider, "execute_plan", side_effect=AssertionError("catalogue requested again")),
        ):
            current = metadata.EvidenceClient(provider, observation=first)
            self.assertIs(current.observe_catalogue(), first)
            self.assertEqual(set(current.catalogue), {"FIRST"})
        later = metadata.EvidenceClient(provider)
        latest = later.observe_catalogue()
        self.assertIs(later.observe_catalogue(), latest)
        self.assertEqual(set(later.catalogue), {"LATER"})
        self.assertEqual(provider.catalogue_calls, 2)


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
        view = SimpleNamespace(source=source, local={})
        client = SimpleNamespace(
            evidence_current=lambda value: value is evidence, provider_view=lambda value, **_: value
        )
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
            local={},
            source={
                "history_start": "2016-01-01T00:00:00Z",
                "latest_request": {
                    "end_exclusive": "2026-11-11T00:00:00Z",
                },
            },
        )
        client = SimpleNamespace(evidence_current=lambda value: False, provider_view=lambda value, **_: value)
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
