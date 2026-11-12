"""Application incorporation belongs to its exact selected input component."""

from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact

from libs.alpaca_lineage_build import sourced_rules
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.work_contract import WorkDeferred, WorkPolicy
from libs.alpaca_security_lineage.sourced_facts import EXTENSIONS


class ApplicationComponentScopeTests(TestCase):
    def setUp(self):
        self.fact = documented_fact(
            "generic-common-chronology",
            ("COMMON", "111111111"),
            ("CONTINUING", "111111111"),
            [("COMMON", "111111111", "2024-01-01", None, "Continuing Common")],
        )
        self.other_fact = documented_fact(
            "generic-right-chronology",
            ("RIGHT", "222222222"),
            ("CONTINUING", "222222222"),
            [("RIGHT", "222222222", "2024-01-01", None, "Separate Right")],
        )

    def setup_group(self, facts, *, members=("COMMON", "UNIT"), split_components=False):
        applications, _ = sourced_rules.accept_applications(
            [{"id": "generic-component-application", "symbols": list(members), "supplements": facts}]
        )
        self.application = applications[0]
        self.sources = {name: {"latest_request": {"end_exclusive": "2024-01-10T00:00:00Z"}} for name in members}
        self.receipts = {
            name: {
                "symbol": name,
                "source_sha256": "pricing-" + name,
                "input_sources": {
                    member: "pricing-" + member for member in ((name,) if split_components else members)
                },
                "evidence": {"asset_lookups": []},
            }
            for name in members
        }
        self.policy = WorkPolicy(
            Path("/synthetic/no-store"),
            {"symbols": {}},
            {
                name: {
                    "obligation_id": "obligation-" + name,
                    "end_exclusive": "2024-01-10",
                    "required_sourced_facts": {self.application["id"]: self.application["fingerprint"]},
                }
                for name in members
            },
            [],
            "2024-01-10",
        )
        self.policy.rule_applications = applications

    def attach(self, name, *facts):
        evidence = self.receipts[name]["evidence"]
        for fact in facts:
            extension = EXTENSIONS[fact["kind"]]
            evidence.setdefault(extension, {"schema_version": 1, "facts": []})["facts"].append(fact)

    def assert_complete(self):
        for name, receipt in self.receipts.items():
            self.assertEqual(receipt["update_obligation_id"], "obligation-" + name)
            self.assertEqual(
                receipt["evidence"]["sourced_fact_applications"][self.application["id"]],
                self.application["fingerprint"],
            )

    def test_common_fact_is_incorporated_for_selected_unit_without_copying_ownership_evidence(self):
        self.setup_group([self.fact])
        self.attach("COMMON", self.fact)
        self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()
        self.assertNotIn(EXTENSIONS[self.fact["kind"]], self.receipts["UNIT"]["evidence"])

    def test_application_bundle_can_be_completed_across_distinct_selected_source_evidence(self):
        self.setup_group([self.fact, self.other_fact], members=("COMMON", "RIGHT", "UNIT"))
        self.attach("COMMON", self.fact)
        self.attach("RIGHT", self.other_fact)
        self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()

    def test_co_publication_cannot_borrow_facts_from_a_different_selected_component(self):
        self.setup_group([self.fact], split_components=True)
        self.attach("COMMON", self.fact)
        with self.assertRaisesRegex(WorkDeferred, "UNIT: required sourced facts"):
            self.policy.stamp(self.receipts, self.sources)
        self.assertNotIn("sourced_fact_applications", self.receipts["UNIT"]["evidence"])

    def test_missing_or_changed_fact_cannot_satisfy_the_current_application(self):
        for changed in (False, True):
            with self.subTest(changed=changed):
                self.setup_group([self.fact, self.other_fact])
                self.attach("COMMON", self.fact)
                if changed:
                    other = deepcopy(self.other_fact)
                    other["facts"]["episodes"][0]["name"] = "Different Source Assertion"
                    self.attach("UNIT", other)
                with self.assertRaisesRegex(WorkDeferred, "required sourced facts"):
                    self.policy.stamp(self.receipts, self.sources)

    def test_conflicting_peer_facts_fail_without_choosing_an_arbitrary_assertion(self):
        self.setup_group([self.fact])
        self.attach("COMMON", self.fact)
        changed = deepcopy(self.fact)
        changed["facts"]["episodes"][0]["name"] = "Competing Source Assertion"
        self.attach("UNIT", changed)
        with self.assertRaisesRegex(LineageError, "conflicting sourced"):
            self.policy.stamp(self.receipts, self.sources)

    def test_current_peer_marker_is_authoritative_without_rechecking_any_fact_body(self):
        self.setup_group([self.fact])
        self.receipts["COMMON"]["evidence"]["sourced_fact_applications"] = {
            self.application["id"]: self.application["fingerprint"]
        }
        with (
            patch.object(sourced_rules, "items", side_effect=AssertionError("accepted facts reread")),
            patch.object(
                sourced_rules, "supplement_key", side_effect=AssertionError("accepted fact keys recalculated")
            ),
            patch.object(
                sourced_rules, "incorporated", side_effect=AssertionError("source-local acceptance replayed")
            ),
        ):
            self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()
        with patch.object(
            sourced_rules,
            "incorporated_group",
            side_effect=AssertionError("completed group projection repeated"),
        ):
            self.policy.stamp(self.receipts, self.sources)

    def test_later_pricing_failure_preserves_completed_group_incorporation_for_retry(self):
        self.setup_group([self.fact])
        self.attach("COMMON", self.fact)
        self.policy.work["UNIT"].update(
            invalidate_pricing=True,
            acquisition_evidence_fingerprint="selected-mapping-obligation",
        )
        with self.assertRaisesRegex(WorkDeferred, "pricing replacement is incomplete"):
            self.policy.stamp(self.receipts, self.sources)
        for receipt in self.receipts.values():
            self.assertEqual(
                receipt["evidence"]["sourced_fact_applications"][self.application["id"]],
                self.application["fingerprint"],
            )
        self.sources["UNIT"]["acquisition_evidence_sha256"] = "selected-mapping-obligation"
        with (
            patch.object(
                sourced_rules,
                "incorporated_group",
                side_effect=AssertionError("completed group projection repeated"),
            ),
            patch.object(
                sourced_rules, "incorporated", side_effect=AssertionError("source-local acceptance replayed")
            ),
        ):
            self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()

    def test_a_failed_independent_component_does_not_reopen_its_completed_peer(self):
        self.setup_group([self.fact], members=("COMMON", "UNIT", "OTHER"))
        self.attach("COMMON", self.fact)
        self.receipts["COMMON"]["input_sources"].pop("OTHER")
        self.receipts["UNIT"]["input_sources"].pop("OTHER")
        self.receipts["OTHER"]["input_sources"] = {"OTHER": "pricing-OTHER"}
        with self.assertRaisesRegex(WorkDeferred, "OTHER: required sourced facts"):
            self.policy.stamp(self.receipts, self.sources)
        self.attach("OTHER", self.fact)
        original = sourced_rules.incorporated_group

        def failed_component_only(client, evidence, **kwargs):
            self.assertEqual(set(evidence), {"OTHER"}, "successful component recalculated")
            return original(client, evidence, **kwargs)

        with patch.object(sourced_rules, "incorporated_group", side_effect=failed_component_only):
            self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()

    def test_selected_auxiliary_evidence_can_complete_the_owning_component(self):
        self.setup_group([self.fact])
        self.attach("COMMON", self.fact)
        auxiliary = self.receipts.pop("COMMON")["evidence"]
        self.sources.pop("COMMON")
        self.policy.work.pop("COMMON")
        self.receipts["UNIT"]["input_sources"] = {"UNIT": "pricing-UNIT"}
        self.receipts["UNIT"]["unavailable_pricing"] = {
            "COMMON": {"evidence": auxiliary, "outcome": "invalid_symbol", "request": {}}
        }
        self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()

    def test_changed_selected_source_binding_cannot_lend_even_a_completed_peer_marker(self):
        self.setup_group([self.fact])
        self.receipts["COMMON"]["evidence"]["sourced_fact_applications"] = {
            self.application["id"]: self.application["fingerprint"]
        }
        self.receipts["UNIT"]["input_sources"]["COMMON"] = "different-generation"
        with self.assertRaisesRegex(WorkDeferred, "UNIT: required sourced facts"):
            self.policy.stamp(self.receipts, self.sources)

    def test_changed_application_intent_needs_its_completed_projection(self):
        self.setup_group([self.fact])
        self.attach("COMMON", self.fact)
        self.policy.stamp(self.receipts, self.sources)
        old_fingerprint = self.application["fingerprint"]
        applications, _ = sourced_rules.accept_applications(
            [
                {
                    "id": self.application["id"],
                    "symbols": ["COMMON", "UNIT"],
                    "supplements": [self.fact],
                    "refine_accepted_dates": False,
                }
            ]
        )
        self.policy.rule_applications = applications
        self.application = applications[0]
        self.assertNotEqual(old_fingerprint, self.application["fingerprint"])
        for item in self.policy.work.values():
            item["required_sourced_facts"] = {self.application["id"]: self.application["fingerprint"]}
        with self.assertRaisesRegex(WorkDeferred, "required sourced facts"):
            self.policy.stamp(self.receipts, self.sources)
        self.receipts["COMMON"]["evidence"]["sourced_fact_intents"] = {self.fact["match"]["source_id"]: False}
        self.policy.stamp(self.receipts, self.sources)
        self.assert_complete()
