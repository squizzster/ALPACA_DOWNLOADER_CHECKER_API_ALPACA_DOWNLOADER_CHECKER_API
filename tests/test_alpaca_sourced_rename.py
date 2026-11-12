"""Sourced unchanged-equity membership and independently scoped mapped rows."""

import copy
import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import sourced_rename
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_security_lineage import event_supplements
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256
from tests.test_alpaca_end_of_day_security_correlation import occupancy
from tests.test_alpaca_name_change_evidence import bars, inputs_for, raw_segments, rename


def supplement():
    return {
        "kind": "identity_preserving_rename",
        "match": {
            "id": "transition",
            "action_type": "name_change",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "process_date": "2024-01-05",
        },
        "facts": {
            "schema_version": 1,
            "share_class": "common_stock",
            "unchanged_security": True,
            "unchanged_rights": True,
            "no_share_exchange": True,
            "legal_effective_date": "2024-01-04",
            "exchange_effective_date": "2024-01-05",
            "prior_labels": [
                {
                    "symbol": "EARLY",
                    "cusip": "111111111",
                    "start_inclusive": "2024-01-01",
                    "end_exclusive": "2024-01-03",
                },
                {
                    "symbol": "OLD",
                    "cusip": "111111111",
                    "start_inclusive": "2024-01-03",
                    "end_exclusive": "2024-01-05",
                },
            ],
        },
        "sources": [
            {
                "url": "https://issuer.example/unchanged-equity",
                "title": "Same common shares and bounded label history",
                "authority": "issuer",
                "document_date": "2024-01-04",
            }
        ],
    }


def case():
    evidence = json.loads(
        json.dumps(rename()).replace("old-cusip", "111111111").replace("new-cusip", "222222222")
    )
    old, new = evidence["identities"]
    for asset in (old, new):
        asset.update(exchange="NYSE", status="active", tradable=True)
    rival = {**old, "id": "unrelated-id", "lookup_cusips": ["333333333"]}
    evidence["identities"].append(rival)
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": key,
            "asset": asset,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, key, asset in (
            ("symbol", "OLD", rival),
            ("symbol", "NEW", new),
            ("cusip", "111111111", old),
            ("cusip", "222222222", new),
            ("cusip", "333333333", rival),
        )
    ]
    evidence.update(
        query={
            "input_symbol": "NEW",
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2024-01-09",
            "corporate_actions_data_quality": "complete",
        },
        queried_symbols=["OLD", "NEW"],
        unresolved_cusips=[],
    )
    evidence["corporate_actions"].append(
        {
            "id": "unrelated-dividend",
            "action_type": "cash_dividend",
            "symbol": "OLD",
            "cusip": "333333333",
            "process_date": "2024-01-05",
            "ex_date": "2024-01-05",
            "rate": 1,
        }
    )
    evidence["symbol_occupancy_evidence"].append(
        occupancy(
            "OLD",
            "333333333",
            "unrelated-id",
            action_id="unrelated-dividend",
            action_type="cash_dividend",
            process_date="2024-01-05",
            role="subject",
        )
    )
    evidence["provider_corporate_actions"] = copy.deepcopy(evidence["corporate_actions"])
    evidence["corporate_actions"], evidence["event_supplements"] = event_supplements.project_actions(
        evidence["provider_corporate_actions"], event_supplements.accept_supplements([supplement()])
    )
    days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")
    inputs = inputs_for(
        {"NEW": bars(days, 10), "OLD": bars(days[-2:], 100)}, asofs={"NEW": "2024-01-09", "OLD": "2024-01-09"}
    )
    return evidence, inputs


class SourcedRenameTests(unittest.TestCase):
    def evaluate(self, evidence, inputs, *, version=81):
        policy = replace(load_authorized_policy(), implementation_correlation_version=version)
        us_market_hours_sha256()
        with patch.object(Path, "open", side_effect=AssertionError("unexpected retained payload read")):
            return correlate(Path("/unused"), inputs, {name: evidence for name in inputs}, policy=policy)

    def test_same_security_one_id_full_labels_and_reused_donor_untouched(self):
        evidence, inputs = case()
        previous = self.evaluate(evidence, inputs, version=80)
        with (
            patch.object(sourced_rename, "certify_group", wraps=sourced_rename.certify_group) as membership,
            patch.object(sourced_rename, "source_history", wraps=sourced_rename.source_history) as source,
        ):
            reports = self.evaluate(evidence, inputs)
        self.assertEqual(1, membership.call_count)
        self.assertEqual(2, source.call_count)
        report = reports["NEW"]
        self.assertEqual(40, report["correlated"]["row_count"])
        segments = raw_segments(report)
        self.assertEqual(["EARLY", "OLD", "NEW"], [item["historical_symbol"] for item in segments])
        self.assertEqual(1, len({item["security_identity"]["security_id"] for item in segments}))
        self.assertEqual({"new-id", "old-id"}, set(segments[0]["security_identity"]["member_alpaca_asset_ids"]))
        self.assertEqual(
            [
                (s["historical_symbol"], s["security_identity"], s["row_count"])
                for s in raw_segments(previous["OLD"])
            ],
            [
                (s["historical_symbol"], s["security_identity"], s["row_count"])
                for s in raw_segments(reports["OLD"])
            ],
        )
        self.assertEqual(previous["OLD"]["uncorrelated"]["segments"], reports["OLD"]["uncorrelated"]["segments"])
        interval = segments[0]["ticker_ownership_interval"]
        self.assertEqual([], interval["supporting_corporate_action_ids"])
        self.assertEqual(["transition"], interval["supporting_sourced_fact_ids"])

    def test_missing_predecessor_source_does_not_require_lookup_of_historical_label(self):
        evidence, inputs = case()
        inputs.pop("OLD")
        reports = self.evaluate(evidence, inputs)
        self.assertEqual(40, reports["NEW"]["correlated"]["row_count"])
        self.assertNotIn("EARLY", evidence["queried_symbols"])

    def test_source_veto_does_not_revoke_independently_proved_membership(self):
        def context(inputs, key, value):
            for download in inputs["NEW"].downloads.values():
                download.manifest[key] = value

        for label, mutate in {
            "historical asof": lambda e, i: context(i, "asof", "2024-01-04"),
            "disabled asof": lambda e, i: context(i, "asof", "-"),
            "incomplete view": lambda e, i: i["NEW"].local["downloads"][0].update(rows=6),
            "context mismatch": lambda e, i: next(iter(i["NEW"].downloads.values())).manifest.update(feed="iex"),
            "wrong current mapping": lambda e, i: e["asset_lookups"][1].update(asset=e["identities"][-1]),
        }.items():
            with self.subTest(label=label):
                evidence, inputs = case()
                mutate(evidence, inputs)
                report = self.evaluate(evidence, inputs)["NEW"]
                self.assertIsNone(report["sourced_rename_ownership"])
                self.assertTrue(
                    any(edge.get("corporate_action_id") == "transition" for edge in report["uuid_lineage_edges"])
                )

    def test_conversion_and_conflicting_peer_endpoint_veto_membership(self):
        evidence, _ = case()
        peer = copy.deepcopy(evidence)
        peer.pop("event_supplements")
        peer["corporate_actions"].append(
            {
                "id": "conversion",
                "action_type": "stock_merger",
                "recipients": [{"cusip": "111111111"}],
                "process_date": "2024-01-04",
            }
        )
        peer["provider_corporate_actions"] = peer["corporate_actions"]
        self.assertEqual((), sourced_rename.certify_group((evidence, peer))[0])
        peer = copy.deepcopy(evidence)
        peer.pop("event_supplements")
        peer["asset_lookups"][2]["asset"]["id"] = "conflicting-id"
        self.assertEqual((), sourced_rename.certify_group((evidence, peer))[0])

    def test_no_facts_replays_v80_assignments_exactly(self):
        evidence, inputs = case()
        evidence.pop("event_supplements")
        previous = self.evaluate(evidence, inputs, version=80)
        current = self.evaluate(evidence, inputs)
        for symbol in inputs:
            for branch in ("correlated", "uncorrelated"):
                self.assertEqual(previous[symbol][branch], current[symbol][branch])

    def test_ingress_preserves_provider_actions_and_accepts_once(self):
        item = supplement()
        accepted = event_supplements.accept_supplements([item])
        with patch.object(
            event_supplements, "validate_supplements", side_effect=AssertionError("repeated ingress")
        ):
            self.assertIs(accepted, event_supplements.accept_supplements(accepted))
        action = copy.deepcopy(item["match"])
        projected, applied = event_supplements.project_actions([action], accepted)
        self.assertIs(action, projected[0])
        self.assertEqual(accepted, applied)
        with self.assertRaises(ValueError):
            event_supplements.project_actions([{**action, "effective_date": "2024-01-06"}], accepted)

    def test_receipt_envelope_fails_closed_without_reproving_fact(self):
        evidence, inputs = case()
        report = self.evaluate(evidence, inputs)["NEW"]
        value = report["sourced_rename_ownership"]
        receipt = {
            "schema_version": 36,
            "symbol": "NEW",
            "current_source_ownership": None,
            "sourced_rename_ownership": value,
            "sourced_rename_membership": report["sourced_rename_membership"],
        }
        validate_handoff(receipt)
        for wrong in (None, {**value, "version": True}, {**value, "last": "2023-01-01"}):
            altered = copy.deepcopy(receipt)
            if wrong is None:
                altered.pop("sourced_rename_ownership")
            else:
                altered["sourced_rename_ownership"] = wrong
            with self.assertRaisesRegex(Exception, "owning lineage recovery"):
                validate_handoff(altered)


class MappedSourceSeparationTests(unittest.TestCase):
    evaluate = SourcedRenameTests.evaluate

    def terminal_case(self):
        evidence, inputs = case()
        terminal = {
            **evidence["identities"][0],
            "id": "terminal-id",
            "symbol": "NEW",
            "status": "inactive",
            "tradable": False,
            "lookup_cusips": ["444444444"],
        }
        evidence["identities"].append(terminal)
        evidence["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "444444444",
                "asset": terminal,
                "found": True,
                "outcome": "found",
                "http_status": 200,
            }
        )
        for action in (
            {
                "id": "terminal-dividend",
                "action_type": "cash_dividend",
                "symbol": "NEW",
                "cusip": "444444444",
                "process_date": "2024-01-02",
                "ex_date": "2024-01-02",
                "rate": 1,
            },
            {
                "id": "terminal-exit",
                "action_type": "cash_merger",
                "acquiree_symbol": "NEW",
                "acquiree_cusip": "444444444",
                "process_date": "2024-01-04",
                "effective_date": "2024-01-04",
                "rate": 50,
            },
        ):
            evidence["corporate_actions"].append(action)
            evidence["provider_corporate_actions"].append(action)
            evidence["symbol_occupancy_evidence"].append(
                occupancy(
                    "NEW",
                    "444444444",
                    "terminal-id",
                    action_id=action["id"],
                    action_type=action["action_type"],
                    process_date=action["process_date"],
                    role="acquiree" if action["action_type"] == "cash_merger" else "subject",
                )
            )
        return evidence, inputs

    def test_current_mapping_never_falls_back_to_terminal_label_occupant(self):
        for failure in ("partial", "old endpoint missing", "old endpoint conflicting"):
            evidence, inputs = self.terminal_case()
            if failure == "partial":
                inputs["NEW"].local["downloads"][0]["rows"] += 1
            elif failure == "old endpoint missing":
                evidence["asset_lookups"] = [
                    item for item in evidence["asset_lookups"] if item["lookup"] != "111111111"
                ]
            else:
                evidence["asset_lookups"][2]["asset"]["id"] = "conflicting-id"
            with self.subTest(failure=failure):
                report = self.evaluate(evidence, inputs)["NEW"]
                self.assertIsNone(report["sourced_rename_ownership"])
                self.assertFalse(
                    any(
                        segment["canonical_security"]["alpaca_asset_id"] == "terminal-id"
                        for segment in raw_segments(report)
                    )
                )
                self.assertGreater(report["uncorrelated"]["row_count"], 0)

    def test_pinned_terminal_mapping_keeps_its_original_ownership(self):
        evidence, inputs = self.terminal_case()
        for download in inputs["NEW"].downloads.values():
            download.manifest["asof"] = "2024-01-03"
        before, after = (self.evaluate(evidence, inputs, version=v)["NEW"] for v in (80, 81))

        def terminal_rows(report):
            return [
                (s["first_session_date"], s["last_session_date"], s["row_count"])
                for s in raw_segments(report)
                if s["canonical_security"]["alpaca_asset_id"] == "terminal-id"
            ]

        self.assertTrue(terminal_rows(before))
        self.assertEqual(terminal_rows(before), terminal_rows(after))
        self.assertIsNone(after["sourced_rename_ownership"])

    def test_missing_session_and_uninformative_history_never_gain_source_scope(self):
        for kind in ("gap", "empty", "constant"):
            evidence, inputs = case()
            for download in inputs["NEW"].downloads.values():
                if kind == "gap":
                    download.rows.pop(1)
                elif kind == "empty":
                    download.rows.clear()
                else:
                    for row in download.rows:
                        row.update(dict.fromkeys(("o", "h", "l", "c", "vw"), "1"), v="0", n="0")
                download.manifest["rows"] = len(download.rows)
            for descriptor in inputs["NEW"].local["downloads"]:
                descriptor["rows"] = len(inputs["NEW"].downloads[descriptor["file"]].rows)
            with self.subTest(kind=kind):
                self.assertIsNone(self.evaluate(evidence, inputs)["NEW"]["sourced_rename_ownership"])


class SelectedGroupHandoffTests(unittest.TestCase):
    def test_completed_empty_membership_is_a_reusable_handoff(self):
        evidence, inputs = case()
        evidence.pop("event_supplements")
        report = SourcedRenameTests().evaluate(evidence, inputs)["NEW"]
        self.assertEqual({"version": 1, "certificates": [], "issues": []}, report["sourced_rename_membership"])
        _, following_inputs = case()
        for view in following_inputs.values():
            view.accepted_sourced_rename_membership = report["sourced_rename_membership"]
        with patch.object(
            sourced_rename, "certify_group", side_effect=AssertionError("empty completed decision repeated")
        ):
            following = SourcedRenameTests().evaluate(evidence, following_inputs)["NEW"]
        self.assertEqual(report["sourced_rename_membership"], following["sourced_rename_membership"])

    def test_missing_or_conflicting_unpriced_peer_cannot_be_ignored(self):
        evidence, inputs = case()
        inputs.pop("OLD")
        peer = copy.deepcopy(evidence)
        peer.pop("event_supplements")
        peer["provider_corporate_actions"].append(
            {
                "id": "conversion",
                "action_type": "stock_merger",
                "recipients": [{"cusip": "111111111"}],
                "process_date": "2024-01-04",
            }
        )
        policy = load_authorized_policy()
        for scope in (
            {"selected_group": ["NEW", "MISSING"]},
            {"selected_group": ["NEW", "MISSING"], "unavailable_pricing": ["MISSING"]},
            {
                "selected_group": ["NEW", "MISSING"],
                "unavailable_pricing": ["MISSING"],
                "auxiliary_evidence": {"MISSING": peer},
            },
        ):
            with self.subTest(scope=scope.keys()):
                report = correlate(
                    Path("/unused"), copy.deepcopy(inputs), {"NEW": evidence}, policy=policy, **scope
                )["NEW"]
                self.assertEqual([], report["sourced_rename_membership"]["certificates"])
                self.assertIsNone(report["sourced_rename_ownership"])

    def test_completed_positive_and_negative_membership_survives_new_price_objects(self):
        for valid in (True, False):
            evidence, inputs = case()
            if not valid:
                evidence["asset_lookups"] = [
                    item for item in evidence["asset_lookups"] if item["lookup"] != "111111111"
                ]
            # An incomplete source cannot carry positive source history, but the
            # completed independent metadata decision remains authoritative.
            inputs["NEW"].local["downloads"][0]["rows"] += 1
            report = SourcedRenameTests().evaluate(evidence, inputs)["NEW"]
            self.assertIsNone(report["sourced_rename_ownership"])
            _, next_inputs = case()
            for view in next_inputs.values():
                view.accepted_sourced_rename_membership = report["sourced_rename_membership"]
            with (
                self.subTest(valid=valid),
                patch.object(
                    sourced_rename,
                    "certify_group",
                    side_effect=AssertionError("completed metadata proof repeated"),
                ),
            ):
                following = SourcedRenameTests().evaluate(evidence, next_inputs)["NEW"]
            self.assertEqual(report["sourced_rename_membership"], following["sourced_rename_membership"])

    def test_metadata_delta_is_handed_off_once_including_unavailable_peer(self):
        from libs.alpaca_lineage_build import resume

        evidence, _ = case()
        before = {"NEW": {"evidence": evidence, "source_rows": 1}}
        other = {"peer": {"identities": []}}
        changed = {"peer": {"identities": [{"id": "changed"}]}}
        for after, expected in ((other, "none"), (changed, "complete_group")):
            completed = {}
            with (
                patch.object(resume, "reusable_decision", return_value=True),
                patch.object(resume, "evidence_delta", wraps=resume.evidence_delta) as delta,
            ):
                self.assertTrue(
                    resume.requires_lineage(
                        before,
                        {"NEW": evidence},
                        pricing_unchanged=False,
                        completed_deltas=completed,
                        auxiliary_previous=other,
                        auxiliary_current=after,
                    )
                )
            self.assertEqual(2, delta.call_count)
            self.assertEqual(expected, completed["peer"]["scope"])
            self.assertEqual("none", completed["NEW"]["scope"])


class MetadataProjectionHandoffTests(unittest.TestCase):
    def test_action_request_planning_needs_no_live_projection_owner(self):
        from libs.alpaca_lineage_build import metadata

        previous = {
            "anchor_month": "2024-06",
            "evidence": {
                "queried_symbols": ["OLD", "NEW"],
                "query": {"corporate_actions_start": "2024-01-01"},
            },
        }
        client = metadata.EvidenceClient(None)
        with (
            patch.object(
                metadata, "EventProjection", side_effect=AssertionError("planning allocated projector")
            ),
            patch.object(
                event_supplements,
                "_project_action",
                side_effect=AssertionError("planning replayed event projection"),
            ),
        ):
            self.assertEqual(
                [("NEW", "2024-01-01", "2024-07-01")],
                metadata.action_requests("NEW", "2024-01-01", "2024-07-01"),
            )
            self.assertEqual(
                [("NEW", "2024-06-01", "2024-07-01"), ("OLD", "2024-06-01", "2024-07-01")],
                metadata.action_requests("NEW", "2024-01-01", "2024-07-01", previous),
            )
            self.assertIsNone(metadata.WindowClient(None, "NEW", previous, False).event_projection)
            self.assertIs(
                client.event_projection, metadata.WindowClient(client, "NEW", previous, False).event_projection
            )

    def test_no_fact_reports_are_not_retained_but_malformed_facts_fail_closed(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient

        client = EvidenceClient(None)
        client.retain_event_supplements([{"corporate_actions": [{"id": "ordinary"}]}])
        self.assertEqual({}, client._retained_supplement_evidence)
        self.assertEqual({}, client.event_projection.accepted)
        with self.assertRaisesRegex(Exception, "unsupported retained sourced succession schema"):
            client.retain_event_supplements([{"sourced_successions": {}}])

    def test_traversal_and_peer_failure_reuse_completed_projection(self):
        fact = event_supplements.accept_supplements([supplement()])[0]
        action = dict(fact["match"])
        owner = event_supplements.EventProjection()
        project = owner.bind([fact])
        with patch.object(
            event_supplements, "_project_action", wraps=event_supplements._project_action
        ) as produce:
            first = project([action])
            self.assertEqual(first, project([action]))
            bad = {**action, "old_cusip": "999999999"}
            with self.assertRaises(ValueError):
                project([bad])
            self.assertEqual(first, owner.bind([fact])([action]))
        self.assertEqual(2, produce.call_count)  # accepted input once, distinct bad observation once
        for key, value in (
            ("process_date", "2024-01-06"),
            ("new_cusip", "999999999"),
            ("effective_date", "2024-01-06"),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                project([{**action, key: value}])
        changed_fact = supplement()
        changed_fact["match"]["new_symbol"] = "OTHER"
        with self.assertRaises(ValueError):
            owner.bind(event_supplements.accept_supplements([changed_fact]))([action])

    def test_retained_equal_fact_copies_use_canonical_completed_projection(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient

        evidence, _ = case()
        evidence["event_supplement_schema_version"] = 1
        client = EvidenceClient(None)
        peer = copy.deepcopy(evidence)
        client.retain_event_supplements([evidence, peer])
        project = client.event_projection.bind(client.all_event_supplements())
        with patch.object(
            event_supplements, "_project_action", side_effect=AssertionError("retained event rematched")
        ):
            for accepted in (evidence, peer):
                self.assertEqual(
                    accepted["corporate_actions"], project(accepted["provider_corporate_actions"])[0]
                )
            client.retain_event_supplements([evidence, peer])
            self.assertEqual(evidence["corporate_actions"], project(evidence["provider_corporate_actions"])[0])

    def test_retained_negative_projection_is_carried_without_recalculation(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient
        from tests.test_alpaca_event_supplements import derivative_supplement

        fact = event_supplements.accept_supplements([derivative_supplement()])[0]
        action = dict(fact["match"])
        evidence = {
            "event_supplement_schema_version": 1,
            "event_supplements": [fact],
            "provider_corporate_actions": [action],
            "corporate_actions": [],
        }
        client = EvidenceClient(None)
        client.retain_event_supplements([evidence])
        with patch.object(
            event_supplements, "_project_action", side_effect=AssertionError("negative projection repeated")
        ):
            self.assertEqual(
                ([], [fact]), client.event_projection.bind(client.all_event_supplements())([action])
            )


class EmptyMembershipPipelineTests(unittest.TestCase):
    def test_price_replacement_carries_empty_result_without_added_metadata_comparison(self):
        from libs.alpaca_lineage_build import resume
        from libs.alpaca_pricing_download.download import download_pricing
        from tests.test_alpaca_pricing_download import FakeClient
        from tests.test_alpaca_sourced_rename_pipeline import SourcedRenamePipelineTests

        fixture = SourcedRenamePipelineTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture.build()
        before = fixture.receipt()
        self.assertEqual({"version": 1, "certificates": [], "issues": []}, before["sourced_rename_membership"])
        changed = copy.deepcopy(fixture.rows["NEXT"])
        for field in ("o", "h", "l", "c", "vw"):
            changed[0][field] += 1
        download_pricing(fixture.store, "NEXT", "2024-06-21", client=FakeClient(changed), refresh_all=True)
        classify = resume.evidence_delta

        def retained_peer_only(before, after, **kwargs):
            # The unchanged peer needs one metadata applicability decision for
            # its numerical carry. The replaced source and completed empty
            # membership result need no additional classification.
            self.assertEqual(before["query"]["input_symbol"], "PRIOR")
            return classify(before, after, **kwargs)

        with (
            patch.object(
                resume,
                "evidence_delta",
                side_effect=retained_peer_only,
            ) as classify_peer,
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("completed empty result repeated")
            ),
        ):
            fixture.build()
        self.assertEqual(classify_peer.call_count, 1)
        after = fixture.receipt()
        self.assertNotEqual(before["source_sha256"], after["source_sha256"])
        self.assertEqual(before["sourced_rename_membership"], after["sourced_rename_membership"])
        # New submitted facts invalidate the absence proof even when pricing is
        # unchanged; the existing owner must produce the newly supported union.
        with patch.object(sourced_rename, "certify_group", wraps=sourced_rename.certify_group) as produce:
            fixture.build(submit=True)
        self.assertEqual(1, produce.call_count)
        self.assertTrue(fixture.receipt()["sourced_rename_membership"]["certificates"])

    def test_newly_observed_event_with_new_applied_fact_invalidates_empty_result(self):
        from tests.test_alpaca_sourced_rename_pipeline import SourcedRenamePipelineTests

        fixture = SourcedRenamePipelineTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture.build()
        before = fixture.receipt()
        new_action = {**fixture.actions[0], "id": "newly-observed-rename", "process_date": "2024-06-18"}
        fixture.actions.append(new_action)
        fixture.supplement = copy.deepcopy(fixture.supplement)
        fixture.supplement["match"] = dict(new_action)
        fixture.supplement["facts"]["exchange_effective_date"] = "2024-06-18"
        fixture.supplement["facts"]["prior_labels"][-1]["end_exclusive"] = "2024-06-18"
        fixture.append()
        with patch.object(sourced_rename, "certify_group", wraps=sourced_rename.certify_group) as produce:
            fixture.build(submit=True)
        after = fixture.receipt()
        self.assertEqual(set(before["input_sources"]), set(after["input_sources"]))
        self.assertEqual(1, produce.call_count)
        self.assertEqual("newly-observed-rename", after["evidence"]["event_supplements"][0]["match"]["id"])
        self.assertNotEqual(before["sourced_rename_membership"], after["sourced_rename_membership"])
        self.assertTrue(after["sourced_rename_membership"]["issues"])

    def test_changed_complete_group_cannot_inherit_old_empty_scope(self):
        from libs.alpaca_pricing_download.download import download_pricing
        from tests.test_alpaca_lineage_build import asset
        from tests.test_alpaca_pricing_download import FakeClient
        from tests.test_alpaca_sourced_rename_pipeline import SourcedRenamePipelineTests

        fixture = SourcedRenamePipelineTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture.build()
        before = fixture.receipt()
        download_pricing(
            fixture.store, "THIRD", "2024-06-21", from_date="2024-01-01", client=FakeClient(fixture.rows["NEXT"])
        )
        fixture.actions.append(
            {
                "id": "new-peer",
                "action_type": "name_change",
                "old_symbol": "THIRD",
                "new_symbol": "NEXT",
                "old_cusip": "555555555",
                "new_cusip": "222222222",
                "process_date": "2024-06-13",
            }
        )
        original_client = fixture.client

        def expanded_client():
            client = original_client()
            client.assets.update({"THIRD": asset("THIRD", "third-id"), "555555555": asset("THIRD", "third-id")})
            return client

        fixture.client = expanded_client
        with patch.object(sourced_rename, "certify_group", wraps=sourced_rename.certify_group) as produce:
            fixture.build(refresh=True)
        after = fixture.receipt()
        self.assertNotEqual(set(before["input_sources"]), set(after["input_sources"]))
        self.assertEqual({"NEXT", "PRIOR", "THIRD"}, set(after["input_sources"]))
        self.assertEqual(1, produce.call_count)
        self.assertEqual(before["sourced_rename_membership"], after["sourced_rename_membership"])
