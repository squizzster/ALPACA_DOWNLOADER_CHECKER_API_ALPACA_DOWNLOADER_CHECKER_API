"""Reject researched rename ingress; retain provider inference and completed handoffs."""

import copy
import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import sourced_rename
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
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
    evidence["event_supplements"] = [supplement()]
    days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")
    inputs = inputs_for(
        {"NEW": bars(days, 10), "OLD": bars(days[-2:], 100)}, asofs={"NEW": "2024-01-09", "OLD": "2024-01-09"}
    )
    return evidence, inputs


def provider_case():
    evidence, inputs = case()
    evidence.pop("event_supplements")
    return evidence, inputs


def completed_certificate():
    # Frozen producer output, not an invocation of the retired researched certifier.
    return sourced_rename.RenameCertificate.from_report(
        {
            "action_id": "transition",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "old_id": "old-id",
            "new_id": "new-id",
            "boundary": "2024-01-05",
            "legal_date": "2024-01-04",
            "labels": [["EARLY", "2024-01-01", "2024-01-03"], ["OLD", "2024-01-03", "2024-01-05"]],
        }
    )


class SourcedRenameTests(unittest.TestCase):
    def evaluate(self, evidence, inputs, *, version=97):
        policy = replace(load_authorized_policy(), implementation_correlation_version=version)
        us_market_hours_sha256()
        with patch.object(Path, "open", side_effect=AssertionError("unexpected retained payload read")):
            return correlate(Path("/unused"), inputs, {name: evidence for name in inputs}, policy=policy)

    def test_well_formed_rename_assertion_rejects_before_binding(self):
        evidence, _ = case()
        with patch.object(
            sourced_rename, "exact_cusip_lookup_asset", side_effect=AssertionError("endpoint selected")
        ):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                sourced_rename.certify_group([evidence])

    def test_incomplete_group_or_relations_cannot_reenable_fact_input(self):
        evidence, _ = case()
        for complete in (True, False):
            with self.subTest(complete=complete), self.assertRaisesRegex(ValueError, "inadmissible"):
                sourced_rename.certify_group([evidence], complete=complete, relations={})

    def test_transform_rejects_before_price_or_row_work(self):
        evidence, inputs = case()
        with self.assertRaisesRegex(Exception, "inadmissible"):
            self.evaluate(evidence, inputs)

    def test_legacy_cache_switch_rejects(self):
        with self.assertRaisesRegex(Exception, "inadmissible"):
            CorrelationCache(sourced_rename_certificates=True)

    def test_provider_only_certifier_result_needs_no_endpoint_or_storage(self):
        evidence, _ = provider_case()
        with (
            patch.object(
                sourced_rename, "exact_cusip_lookup_asset", side_effect=AssertionError("endpoint selected")
            ),
            patch.object(Path, "open", side_effect=AssertionError("storage opened")),
        ):
            self.assertEqual(((), ()), sourced_rename.certify_group([evidence]))

    def test_researched_source_recipe_cannot_establish_new_bounds(self):
        with patch.object(Path, "open", side_effect=AssertionError("price opened")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                sourced_rename.source_history(
                    "NEW", completed_certificate(), None, None, complete=True, informative=True
                )

    def test_closed_certificate_serializes_without_recertification(self):
        certificate = completed_certificate()
        with patch.object(
            sourced_rename, "certify_group", side_effect=AssertionError("closed decision rechecked")
        ):
            self.assertEqual(certificate, sourced_rename.RenameCertificate.from_report(certificate.to_report()))
            self.assertEqual("old-id", certificate.identity_edge().predecessor_asset_id)

    def test_closed_label_intervals_are_cached_and_immutable(self):
        certificate = completed_certificate()
        self.assertIs(certificate.intervals(), certificate.intervals())
        self.assertEqual(["EARLY", "OLD"], [x.historical_symbol for x in certificate.intervals()])
        with self.assertRaises(Exception):
            certificate.old_id = "changed"

    def test_closed_source_bounds_are_read_without_new_source_proof(self):
        certificate = completed_certificate()
        handoff = {"symbol": "NEW", "first": "2024-01-02", "last": "2024-01-04"}
        with (
            patch.object(sourced_rename, "source_history", side_effect=AssertionError("closed proof replayed")),
            patch.object(Path, "open", side_effect=AssertionError("closed prices reopened")),
        ):
            history = sourced_rename.histories(handoff, certificate)
        self.assertEqual(["EARLY", "OLD"], [h.interval.historical_symbol for h in history])
        self.assertEqual(["2024-01-02", "2024-01-03"], [h.first.isoformat() for h in history])

    def test_retained_research_labels_do_not_reenter_provider_projection(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient

        evidence, _ = case()
        selected = EvidenceClient(None).provider_view(evidence)
        self.assertNotIn("event_supplements", selected)
        self.assertNotIn("EARLY", selected["queried_symbols"])
        self.assertTrue(evidence["event_supplements"])

    def test_provider_same_security_rule_generalizes_to_future_labels(self):
        days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")
        for old, new in (("OLD", "NEW"), ("FUTUREA", "FUTUREB")):
            evidence = rename(old=old, new=new)
            with (
                self.subTest(labels=(old, new)),
                patch.object(sourced_rename, "certify_group", side_effect=AssertionError("research needed")),
            ):
                report = self.evaluate(evidence, inputs_for({old: bars(days[:3]), new: bars(days)}))[new]
            self.assertEqual(0, report["uncorrelated"]["row_count"])
            self.assertEqual(
                {"old-id", "new-id"}, {s["canonical_security"]["alpaca_asset_id"] for s in raw_segments(report)}
            )

    def test_missing_provider_endpoint_does_not_gain_researched_label_history(self):
        evidence, inputs = provider_case()
        evidence["asset_lookups"] = [x for x in evidence["asset_lookups"] if x["lookup"] != "111111111"]
        report = self.evaluate(evidence, inputs)["NEW"]
        self.assertIsNone(report["sourced_rename_ownership"])
        self.assertFalse(any(s.get("ticker") == "EARLY" for s in raw_segments(report)))


class MappedSourceSeparationTests(unittest.TestCase):
    evaluate = SourcedRenameTests.evaluate

    def terminal_case(self):
        evidence, inputs = provider_case()
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

    def test_provider_terminal_owner_never_extends_past_its_dated_exit(self):
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
                terminal = [
                    s
                    for s in raw_segments(report)
                    if s["canonical_security"]["alpaca_asset_id"] == "terminal-id"
                ]
                self.assertTrue(terminal)
                self.assertTrue(all(s["last_session_date"] < "2024-01-04" for s in terminal))
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
            evidence, inputs = provider_case()
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
    def test_metadata_delta_is_handed_off_once_including_unavailable_peer(self):
        from libs.alpaca_lineage_build import resume

        evidence, _ = provider_case()
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

    def test_provider_view_is_reused_without_projection_replay(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient
        from libs.alpaca_security_lineage import lineage

        evidence, _ = case()
        client = EvidenceClient(None)
        first = client.provider_view(evidence)
        with patch.object(
            lineage, "report_from_observations", side_effect=AssertionError("completed projection repeated")
        ):
            self.assertIs(first, client.provider_view(evidence))
        self.assertTrue(evidence["event_supplements"])

    def test_unavailable_peer_scope_does_not_trigger_researched_certifier(self):
        evidence, inputs = provider_case()
        inputs.pop("OLD")
        with patch.object(sourced_rename, "certify_group", side_effect=AssertionError("research accessed")):
            report = correlate(
                Path("/unused"),
                inputs,
                {"NEW": evidence},
                policy=load_authorized_policy(),
                selected_group=["NEW", "MISSING"],
                unavailable_pricing=["MISSING"],
            )["NEW"]
        self.assertIsNone(report["sourced_rename_ownership"])


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
        with (
            patch.object(
                event_supplements, "EventProjection", side_effect=AssertionError("planning allocated projector")
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

    def test_new_fact_rejects_before_reopening_completed_store(self):
        from libs.alpaca_lineage_build import pipeline
        from tests.test_alpaca_sourced_rename_pipeline import SourcedRenamePipelineTests

        fixture = SourcedRenamePipelineTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture.build()
        before = fixture.receipt()
        with patch.object(pipeline, "accept_context", side_effect=AssertionError("accepted store reopened")):
            with self.assertRaisesRegex(Exception, "inadmissible"):
                fixture.build(submit=True)
        self.assertEqual(before, fixture.receipt())

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
        self.assertEqual(0, produce.call_count)
        self.assertEqual(before["sourced_rename_membership"], after["sourced_rename_membership"])
