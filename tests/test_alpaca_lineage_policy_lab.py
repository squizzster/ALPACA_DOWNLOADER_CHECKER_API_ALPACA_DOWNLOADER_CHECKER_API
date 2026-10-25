"""Adversarial contract tests for the read-only correlation policy laboratory."""

import copy
import hashlib
import io
import tempfile
from contextlib import contextmanager, nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_component_occupancy as component_occupancy_fixtures
import test_alpaca_end_of_day_security_correlation as correlation_fixtures
import test_alpaca_group_occupancy as group_occupancy_fixtures
import yaml
from test_alpaca_name_change_evidence import bars, inputs_for

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    OWNERSHIP_INTERVAL_RULES,
    build_ticker_ownership_intervals,
    ownership_interval_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    ROW_CORRELATION_RULES,
    CorrelationCache,
    correlate_download_rows,
    row_correlation_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.terminal_fallback import (
    TERMINAL_FALLBACK_RULE_NAME,
)
from libs.alpaca_lineage_build import transform
from libs.alpaca_lineage_build.contract import LineageError, document_digest, versions
from libs.alpaca_lineage_policy_lab.active_suffix import (
    EFFECT as ACTIVE_SUFFIX_EFFECT,
)
from libs.alpaca_lineage_policy_lab.active_suffix import (
    FACT_PRODUCER as ACTIVE_SUFFIX_FACT_PRODUCER,
)
from libs.alpaca_lineage_policy_lab.active_suffix import (
    FACT_TYPES as ACTIVE_SUFFIX_FACT_TYPES,
)
from libs.alpaca_lineage_policy_lab.active_suffix import (
    ROW_RULE_NAME as ACTIVE_SUFFIX_ROW_RULE_NAME,
)
from libs.alpaca_lineage_policy_lab.active_suffix import (
    ActiveSuffixCertificate,
    active_suffix_row_rule,
    prepare_active_suffix_policy,
    with_active_suffix_rule,
)
from libs.alpaca_lineage_policy_lab.audit import (
    _AuditTimedOut,
    _compact_group_result,
    _completed_report,
    _index_prefilter,
    _PreparedGroup,
    _selector_groups,
    _shared_pricing_locks,
    _suffix_prefilter,
    audit,
)
from libs.alpaca_lineage_policy_lab.comparison import (
    _difference,
    _load_group,
    _migration_preserved_rule_order_provenance,
    _normalize_boundaries,
    compare,
)
from libs.alpaca_lineage_policy_lab.contract import (
    LAB_SCHEMA_VERSION,
    LAB_VERSION,
    PolicyLabError,
    validate_change_log,
)
from libs.alpaca_lineage_policy_lab.evidence_quality import (
    FACT_PRODUCER,
    FACT_TYPES,
    _rate_equivalent,
    _structural_link,
    apply_evidence_quality_policy,
)
from libs.alpaca_lineage_policy_lab.fallbacks import (
    _certificate_ownership_rule,
    _corrected_reconciliation_rule,
    _corrected_reconciliations,
    _corrected_suffix_rule,
    _validate_corrected_certificate_effect,
    _validate_phase_monotonicity,
    correlate_schema_four,
)
from libs.alpaca_lineage_policy_lab.policy import load_policy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage import EXCHANGE_TEST_SYMBOL
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "resources/alpaca_security_correlation_v58_legacy_policy.yaml"
QUALITY_POLICY = ROOT / "resources/alpaca_security_correlation_experimental_v58_legacy_policy.yaml"
CURRENT_POLICY = ROOT / "resources/alpaca_security_correlation_policy.yaml"
V59_POLICY = ROOT / "resources/alpaca_security_correlation_v59_legacy_policy.yaml"
CURRENT_QUALITY_POLICY = ROOT / "resources/alpaca_security_correlation_evidence_quality_policy.yaml"


def _load_running_single_phase_policy(path):
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["implementation"]["correlation_version"] = versions()["correlation"]
    with tempfile.TemporaryDirectory() as directory:
        candidate = Path(directory) / path.name
        candidate.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        return load_policy(candidate)


def _without_policy_provenance(reports):
    return {
        symbol: {key: value for key, value in report.items() if key != "correlation_policy"}
        for symbol, report in reports.items()
    }


class PolicyCompilerTests(TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.document = yaml.safe_load(POLICY.read_text(encoding="utf-8"))
        self.document["implementation"]["correlation_version"] = versions()["correlation"]
        self.text = yaml.safe_dump(self.document, sort_keys=False)

    def load_document(self, document, *, name="policy.yaml"):
        path = self.directory / name
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        return load_policy(path)

    def load_text(self, text, *, name="policy.yaml"):
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return load_policy(path)

    def test_manifest_is_exact_current_registry_and_has_stable_digests(self):
        compiled = self.load_document(self.document, name="running-schema-one.yaml")

        self.assertEqual(LAB_VERSION, len(validate_change_log()))
        self.assertEqual(1, compiled.schema_version)
        self.assertEqual(8, LAB_SCHEMA_VERSION)
        self.assertFalse(compiled.evidence_rules)
        self.assertFalse(compiled.active_suffix_rules)
        self.assertEqual(OWNERSHIP_INTERVAL_RULES[:3], compiled.ownership_rules)
        self.assertEqual(ROW_CORRELATION_RULES, compiled.row_rules)
        self.assertEqual(TERMINAL_FALLBACK_RULE_NAME, compiled.terminal_rule)
        self.assertEqual(
            ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES[:3]),
            ownership_interval_rule_order(rules=compiled.ownership_rules),
        )
        self.assertEqual(
            row_correlation_rule_order(),
            row_correlation_rule_order(rules=compiled.row_rules),
        )
        self.assertEqual(
            hashlib.sha256(Path(compiled.source).read_bytes()).hexdigest(),
            compiled.file_sha256,
        )
        self.assertEqual(64, len(compiled.semantic_sha256))

        reformatted = self.directory / "reformatted.yaml"
        reformatted.write_text(
            "# formatting is not policy meaning\n" + yaml.safe_dump(self.document, sort_keys=True),
            encoding="utf-8",
        )
        again = load_policy(reformatted)
        self.assertNotEqual(compiled.file_sha256, again.file_sha256)
        self.assertEqual(compiled.semantic_sha256, again.semantic_sha256)

    def test_v58_reference_manifests_bind_the_preserved_single_phase_profile(self):
        for path in (POLICY, QUALITY_POLICY):
            with self.subTest(path=path.name):
                policy = load_policy(path)
                self.assertEqual(58, policy.implementation_correlation_version)
                self.assertEqual("correlation_v58_single_phase", policy.execution_profile)
                self.assertEqual(OWNERSHIP_INTERVAL_RULES[:3], policy.ownership_rules)
                self.assertEqual(ROW_CORRELATION_RULES, policy.row_rules)
                self.assertIsNone(policy.fallback_profile)
                self.assertIsNone(policy.reconciliation_producer)
                self.assertIsNone(policy.ownership_fallback_producer)

    def test_schema_four_manifests_bind_the_current_and_v59_two_phase_profiles(self):
        production = load_policy(CURRENT_POLICY)
        published = load_policy(V59_POLICY)
        corrected = load_policy(CURRENT_QUALITY_POLICY)

        self.assertEqual(4, production.schema_version)
        self.assertEqual(60, production.implementation_correlation_version)
        self.assertEqual(34, production.implementation_lineage_version)
        self.assertEqual("production", production.policy_role)
        self.assertEqual("correlation_v60_policy_engine", production.execution_profile)
        self.assertEqual("published_v59", production.fallback_profile)
        self.assertEqual(4, published.schema_version)
        self.assertEqual(59, published.implementation_correlation_version)
        self.assertEqual(33, published.implementation_lineage_version)
        self.assertEqual("published_reference", published.policy_role)
        self.assertEqual("published_v59", published.fallback_profile)
        self.assertEqual(
            "legacy_frozen_reconciliation_projection_may_enable_suffix",
            published.producer_dependency,
        )
        self.assertEqual("corrected_v59", corrected.fallback_profile)
        self.assertEqual("corrected_candidate", corrected.policy_role)
        self.assertEqual(
            "independent_raw_evidence_projection_unchanged",
            corrected.producer_dependency,
        )
        self.assertEqual(OWNERSHIP_INTERVAL_RULES[:3], published.established_ownership_rules)
        self.assertEqual(OWNERSHIP_INTERVAL_RULES, published.ownership_rules)
        self.assertEqual(ROW_CORRELATION_RULES, published.row_rules)
        self.assertFalse(published.evidence_rules)
        self.assertFalse(published.active_suffix_rules)

    def test_corrected_schema_four_declares_and_binds_its_typed_yaml_policy(self):
        document = yaml.safe_load(CURRENT_QUALITY_POLICY.read_text(encoding="utf-8"))
        corrected = load_policy(CURRENT_QUALITY_POLICY)

        self.assertEqual(
            [FACT_PRODUCER, ACTIVE_SUFFIX_FACT_PRODUCER],
            document["fact_producers"],
        )
        self.assertEqual(
            {**FACT_TYPES, **ACTIVE_SUFFIX_FACT_TYPES},
            {name: declaration["type"] for name, declaration in document["facts"].items()},
        )
        self.assertEqual((FACT_PRODUCER,), corrected.evidence_fact_producers)
        self.assertEqual(FACT_TYPES, dict(corrected.evidence_fact_types))
        self.assertEqual((ACTIVE_SUFFIX_FACT_PRODUCER,), corrected.active_suffix_fact_producers)
        self.assertEqual(ACTIVE_SUFFIX_FACT_TYPES, dict(corrected.active_suffix_fact_types))
        self.assertEqual(1, len(corrected.evidence_rules))
        self.assertEqual(1, len(corrected.active_suffix_rules))
        self.assertEqual(
            set(FACT_TYPES),
            {name for name, _ in corrected.evidence_rules[0].conditions},
        )
        self.assertEqual(
            set(ACTIVE_SUFFIX_FACT_TYPES),
            {name for name, _ in corrected.active_suffix_rules[0].conditions},
        )

        changed = copy.deepcopy(document)
        conditions = changed["stages"]["evidence_quality"]["rules"][0]["when"]["all"]
        rate_gate = next(item for item in conditions if item["fact"] == "quality.rate_precision_equivalent")
        rate_gate["eq"] = False
        changed_policy = self.load_document(changed, name="changed-corrected-gate.yaml")

        self.assertNotEqual(corrected.semantic_sha256, changed_policy.semantic_sha256)
        self.assertFalse(dict(changed_policy.evidence_rules[0].conditions)["quality.rate_precision_equivalent"])

    def test_schema_four_rejects_mixed_or_weakened_execution_contracts(self):
        document = yaml.safe_load(CURRENT_POLICY.read_text(encoding="utf-8"))
        variants = []
        for path, value in (
            (("policy_role",), "corrected_candidate"),
            (("implementation", "lineage_version"), 32),
            (("execution", "frozen_baseline"), False),
            (("execution", "reconciliation", "scope"), "all_groups"),
            (("execution", "ownership_fallback", "position"), 3),
            (("execution", "second_fixed_point", "seed"), "empty"),
            (("execution", "producer_evaluation", "timing"), "each_pass"),
            (("execution", "observation_selection", "implementation"), "custom"),
            (
                ("execution", "producer_evaluation", "dependency"),
                "independent_raw_evidence_projection_unchanged",
            ),
        ):
            candidate = copy.deepcopy(document)
            target = candidate
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            variants.append((".".join(path), candidate))
        for label, candidate in variants:
            with self.subTest(label=label), self.assertRaises(PolicyLabError):
                self.load_document(candidate, name=label + ".yaml")


class SchemaFourFallbackTests(TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.text = POLICY.read_text(encoding="utf-8")
        self.document = yaml.safe_load(self.text)

    def load_document(self, document, *, name="policy.yaml"):
        path = self.directory / name
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        return load_policy(path)

    def load_text(self, text, *, name="policy.yaml"):
        path = self.directory / name
        path.write_text(text, encoding="utf-8")
        return load_policy(path)

    @staticmethod
    def _view():
        return SimpleNamespace(downloads={}, local={"downloads": []})

    def test_published_profile_traces_legacy_reconciliation_to_suffix_dependency(self):
        policy = load_policy(V59_POLICY)
        baseline = {
            "SUB": {
                "source_row_count": 8,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 8, "segments": []},
                "ticker_ownership_intervals": [],
            }
        }
        final = copy.deepcopy(baseline)
        decision = {
            "action_id": "foreign-copy",
            "effective_date": "2024-01-03",
        }
        with (
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._correlate_until_stable",
                side_effect=(baseline, final),
            ) as fixed_point,
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.reconcile_duplicate_foreign_occupancy",
                return_value={"SUB": (decision,)},
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.may_have_action_anchored_current_suffix",
                return_value=False,
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._validate_phase_monotonicity",
                return_value={"only_baseline_unresolved_rows_resolved": True},
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._report_rows",
                return_value={},
            ),
        ):
            reports, trace = correlate_schema_four(
                Path("/unused"),
                {"SUB": self._view()},
                {"SUB": {}},
                policy,
                selected_group=["SUB"],
                unavailable_pricing=[],
                evidence_generations={"SUB": "a" * 64},
                pricing_generations={"SUB": "b" * 64},
            )

        self.assertEqual(final, reports)
        self.assertEqual(["SUB"], trace["ownership_fallback"]["legacy_reconciliation_enabled_sources"])
        self.assertEqual(
            frozenset(("SUB",)),
            fixed_point.call_args_list[1].kwargs["suffix_sources"],
        )

    def test_published_schema_four_replays_the_default_v59_result(self):
        policy = load_policy(V59_POLICY)
        evidence = correlation_fixtures.RowCorrelationTests._overlapping_asof_lineage()
        inputs = inputs_for(
            {"CURRENT": bars(["2024-01-03"])},
            asofs={"CURRENT": "2026-10-15"},
        )

        default = transform.correlate(Path("."), inputs, {"CURRENT": evidence}, policy=policy)
        candidate, trace = correlate_schema_four(
            Path("."),
            inputs,
            {"CURRENT": evidence},
            policy,
            selected_group=["CURRENT"],
            unavailable_pricing=[],
            evidence_generations={"CURRENT": "a" * 64},
            pricing_generations={"CURRENT": "b" * 64},
        )

        self.assertTrue(all(report["correlation_policy"] == policy.provenance() for report in default.values()))
        self.assertEqual(_without_policy_provenance(default), candidate)
        self.assertEqual("published_v59", trace["profile"])
        self.assertGreaterEqual(trace["phase_one"]["pass_count"], 1)
        self.assertEqual(trace["baseline_sha256"], trace["phase_two"]["seed_sha256"])
        self.assertEqual(64, len(trace["frozen_plan_sha256"]))
        self.assertTrue(trace["phase_invariants"]["only_baseline_unresolved_rows_resolved"])

    def test_published_schema_four_replays_positive_v59_suffix(self):
        policy = load_policy(V59_POLICY)
        fixture = component_occupancy_fixtures.CurrentActionAnchoredSuffixTests()
        fixture.setUp()
        # Keep both UUIDs as requested-label candidates so the established
        # terminal fallback abstains and v59's optional fourth rule is exercised.
        fixture.previous_asset["symbol"] = "REUS"
        inputs = inputs_for(
            {"REUS": bars(["2024-01-05", "2024-01-08"])},
            asofs={"REUS": "2024-01-09"},
        )

        default = transform.correlate(Path("."), inputs, {"REUS": fixture.lineage}, policy=policy)
        candidate, trace = correlate_schema_four(
            Path("."),
            inputs,
            {"REUS": fixture.lineage},
            policy,
            selected_group=["REUS"],
            unavailable_pricing=[],
            evidence_generations={"REUS": "a" * 64},
            pricing_generations={"REUS": "b" * 64},
        )

        self.assertTrue(all(report["correlation_policy"] == policy.provenance() for report in default.values()))
        self.assertEqual(_without_policy_provenance(default), candidate)
        self.assertTrue(trace["second_fixed_point_executed"])
        self.assertEqual(["REUS"], trace["ownership_fallback"]["selected_sources"])
        self.assertEqual(1, len(trace["ownership_fallback"]["applied_intervals"]))
        self.assertGreaterEqual(trace["phase_two"]["pass_count"], 1)

    def test_published_schema_four_replays_positive_v59_reconciliation(self):
        policy = load_policy(V59_POLICY)
        fixture = group_occupancy_fixtures.DuplicateForeignOccupancyTests()
        fixture.setUp()
        inputs = inputs_for(
            {
                "CURR": bars(["2025-06-17"]),
                "OWN": bars(["2024-12-02"]),
            },
            asofs={"CURR": "2025-06-18", "OWN": "2025-06-18"},
        )
        for view in inputs.values():
            for download in view.downloads.values():
                download.manifest["request_to"] = "2025-06-18"
            view.source["latest_request"]["end_exclusive"] = "2025-06-18"

        default = transform.correlate(Path("."), inputs, fixture.evidence, policy=policy)
        candidate, trace = correlate_schema_four(
            Path("."),
            inputs,
            fixture.evidence,
            policy,
            selected_group=["CURR", "OWN"],
            unavailable_pricing=[],
            evidence_generations={"CURR": "a" * 64, "OWN": "b" * 64},
            pricing_generations={"CURR": "c" * 64, "OWN": "d" * 64},
        )

        self.assertTrue(all(report["correlation_policy"] == policy.provenance() for report in default.values()))
        self.assertEqual(_without_policy_provenance(default), candidate)
        self.assertEqual(1, trace["reconciliation"]["decision_count"])
        self.assertEqual(["CURR"], trace["ownership_fallback"]["selected_sources"])
        self.assertEqual([], trace["ownership_fallback"]["legacy_reconciliation_enabled_sources"])
        self.assertIn("OWN", trace["phase_invariants"]["completed_sources_unchanged"])

    def test_corrected_reconciliation_requires_bounded_decimal_rate_equivalence(self):
        decision = {
            "action_id": "foreign-copy",
            "authoritative_symbol": "OWN",
            "authoritative_action_id": "own-copy",
            "effective_date": "2024-01-03",
        }
        evidence = {
            "SUB": {"corporate_actions": [{"id": "foreign-copy", "rate": "1.0000004"}]},
            "OWN": {"corporate_actions": [{"id": "own-copy", "rate": "1.0000000"}]},
        }
        generations = {"SUB": "a" * 64, "OWN": "b" * 64}
        pair = SimpleNamespace(
            subject=SimpleNamespace(receipt_symbol="SUB", action={"id": "foreign-copy"}),
            reference=SimpleNamespace(receipt_symbol="OWN", action={"id": "own-copy"}),
        )
        quality_facts = {
            name: name
            not in {
                "quality.endpoint_lookup_unresolved",
                "quality.selected_structural_link_present",
            }
            for name in FACT_TYPES
        }
        quality_report = {
            "quarantines": [],
            "raw_evidence_sha256": {},
            "candidate_evidence_sha256": {},
            "quarantine_count": 1,
        }
        with (
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.reconcile_duplicate_foreign_occupancy",
                return_value={"SUB": (decision,)},
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.apply_evidence_quality_policy",
                return_value=(evidence, quality_report),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._candidate_pairs",
                return_value=(pair,),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._facts_for_pair",
                return_value=(quality_facts, {}),
            ),
        ):
            accepted, trace, returned_quality = _corrected_reconciliations(
                {}, evidence, {}, generations, {"SUB": {}, "OWN": {}}
            )
            self.assertEqual({"SUB": (decision,)}, accepted)
            self.assertTrue(trace[0]["rate_precision_equivalent"])
            self.assertIs(quality_report, returned_quality)

            evidence["SUB"]["corporate_actions"][0]["rate"] = "1.000002"
            rejected, trace, _ = _corrected_reconciliations(
                {}, evidence, {}, generations, {"SUB": {}, "OWN": {}}
            )
            self.assertEqual({}, rejected)
            self.assertFalse(trace[0]["rate_precision_equivalent"])

            evidence["SUB"]["corporate_actions"][0]["rate"] = "NaN"
            unknown, trace, _ = _corrected_reconciliations({}, evidence, {}, generations, {"SUB": {}, "OWN": {}})
            self.assertEqual({}, unknown)
            self.assertIsNone(trace[0]["rate_precision_equivalent"])

            for value in (None, True, "Infinity", "-1", "0"):
                with self.subTest(invalid_rate=value):
                    if value is None:
                        evidence["SUB"]["corporate_actions"][0].pop("rate", None)
                    else:
                        evidence["SUB"]["corporate_actions"][0]["rate"] = value
                    rejected, trace, _ = _corrected_reconciliations(
                        {}, evidence, {}, generations, {"SUB": {}, "OWN": {}}
                    )
                    self.assertEqual({}, rejected)
                    self.assertIsNone(trace[0]["rate_precision_equivalent"])

            for value, accepted_expected in (
                ("1.0000005", True),
                ("1.0000006", False),
                ("999", False),
            ):
                with self.subTest(rate=value):
                    evidence["SUB"]["corporate_actions"][0]["rate"] = value
                    decisions, trace, _ = _corrected_reconciliations(
                        {}, evidence, {}, generations, {"SUB": {}, "OWN": {}}
                    )
                    self.assertIs(bool(decisions), accepted_expected)
                    self.assertIs(trace[0]["rate_precision_equivalent"], accepted_expected)

    def test_corrected_profile_retains_every_v5_gate_with_fail_closed_polarity(self):
        reconciliation = _corrected_reconciliation_rule()
        suffix = _corrected_suffix_rule()

        self.assertEqual(set(FACT_TYPES), {name for name, _ in reconciliation.conditions})
        self.assertEqual(set(ACTIVE_SUFFIX_FACT_TYPES), {name for name, _ in suffix.conditions})
        self.assertEqual(
            {
                "quality.endpoint_lookup_unresolved",
                "quality.selected_structural_link_present",
            },
            {name for name, expected in reconciliation.conditions if expected is False},
        )
        self.assertEqual(
            {
                "suffix.unresolved_relevant_occupancy_present",
                "suffix.competing_occupancy_on_or_after_anchor",
                "suffix.structural_candidate_link_present",
                "suffix.uuid_candidate_edge_present",
            },
            {name for name, expected in suffix.conditions if expected is False},
        )

    def test_corrected_fourth_rule_is_generation_certified_and_end_bounded(self):
        certificate = ActiveSuffixCertificate(
            rule_id="GENERIC",
            symbol="SUB",
            asset_id="current-id",
            cusip="222222222",
            anchor_date=date(2024, 2, 1),
            competing_last_date=date(2024, 1, 1),
            anchor_action_ids=("anchor",),
            later_action_ids=("later",),
            candidate_pairs=(("current-id", "222222222"), ("old-id", "111111111")),
            evidence_sha256="a" * 64,
            pricing_generation="b" * 64,
            end_exclusive=date(2024, 3, 1),
            suffix_sessions=(date(2024, 2, 1), date(2024, 2, 2)),
        )
        rule = _certificate_ownership_rule((certificate,))
        context = SimpleNamespace(asset_ids_by_symbol_and_cusip={("SUB", "222222222"): "current-id"})

        result = rule.apply(context, (), 4, rule.name)

        self.assertEqual(1, len(result.intervals))
        interval = result.intervals[0]
        self.assertEqual(date(2024, 2, 1), interval.start_date_inclusive)
        self.assertEqual(date(2024, 3, 1), interval.end_date_exclusive)
        self.assertTrue(interval.contains(date(2024, 2, 29)))
        self.assertFalse(interval.contains(date(2024, 3, 1)))

    def test_corrected_effect_preserves_an_established_prefix_and_only_assigns_certified_sessions(self):
        certificate = ActiveSuffixCertificate(
            rule_id="GENERIC",
            symbol="SUB",
            asset_id="current-id",
            cusip="222222222",
            anchor_date=date(2024, 2, 1),
            competing_last_date=date(2024, 1, 1),
            anchor_action_ids=("anchor",),
            later_action_ids=("later",),
            candidate_pairs=(("current-id", "222222222"), ("old-id", "111111111")),
            evidence_sha256="a" * 64,
            pricing_generation="b" * 64,
            end_exclusive=date(2024, 3, 1),
            suffix_sessions=(date(2024, 2, 1), date(2024, 2, 2)),
        )
        rows = (
            ("2024-01-31T05:00:00Z",),
            ("2024-02-01T05:00:00Z",),
            ("2024-02-02T05:00:00Z",),
            ("2024-03-01T05:00:00Z",),
        )

        def identity(asset_id):
            return {
                "alpaca_asset_id": asset_id,
                "member_alpaca_asset_ids": [asset_id],
                "basis": "single_alpaca_asset_id",
            }

        segments = []
        for adjustment in ADJUSTMENTS:
            segments.extend(
                (
                    {
                        "adjustment": adjustment,
                        "row_start": 1,
                        "row_end": 1,
                        "canonical_security": {"alpaca_asset_id": "old-id"},
                        "security_identity": identity("old-id"),
                    },
                    {
                        "adjustment": adjustment,
                        "row_start": 2,
                        "row_end": 3,
                        "canonical_security": {"alpaca_asset_id": "current-id"},
                        "security_identity": identity("current-id"),
                    },
                )
            )
        unresolved_segments = [
            {
                "adjustment": adjustment,
                "row_start": 4,
                "row_end": 4,
                "reason_codes": ["ambiguous_security_identity_candidates"],
            }
            for adjustment in ADJUSTMENTS
        ]
        report = {
            "source_row_count": 4 * len(ADJUSTMENTS),
            "correlated": {"row_count": 3 * len(ADJUSTMENTS), "segments": segments},
            "uncorrelated": {"row_count": len(ADJUSTMENTS), "segments": unresolved_segments},
            "ticker_ownership_intervals": [
                {
                    "historical_symbol": "SUB",
                    "start_date_inclusive": "2024-02-01",
                    "end_date_exclusive": "2024-03-01",
                    "ownership_interval_rules": [{"name": "action_anchored_current_ticker_occupancy"}],
                }
            ],
        }
        baseline = copy.deepcopy(report)
        baseline["correlated"] = {
            "row_count": len(ADJUSTMENTS),
            "segments": [segment for segment in segments if segment["row_start"] == 1],
        }
        baseline["uncorrelated"] = {
            "row_count": 3 * len(ADJUSTMENTS),
            "segments": [
                {
                    "adjustment": adjustment,
                    "row_start": 2,
                    "row_end": 4,
                    "reason_codes": ["ambiguous_security_identity_candidates"],
                }
                for adjustment in ADJUSTMENTS
            ],
        }
        inputs = {"SUB": SimpleNamespace(views={name: rows for name in ADJUSTMENTS})}

        _validate_corrected_certificate_effect(
            {"SUB": baseline},
            {"SUB": report},
            inputs,
            (certificate,),
        )

        escaped = copy.deepcopy(report)
        for segment in escaped["correlated"]["segments"]:
            if segment["canonical_security"]["alpaca_asset_id"] == "current-id":
                segment["row_end"] = 4
        escaped["correlated"]["row_count"] = 4 * len(ADJUSTMENTS)
        escaped["uncorrelated"] = {"row_count": 0, "segments": []}
        with self.assertRaisesRegex(PolicyLabError, "exact certified session set"):
            _validate_corrected_certificate_effect(
                {"SUB": baseline},
                {"SUB": escaped},
                inputs,
                (certificate,),
            )

    def test_second_phase_can_only_resolve_baseline_unresolved_rows(self):
        def segment(branch, start, end, asset=None):
            value = {
                "adjustment": "raw",
                "row_start": start,
                "row_end": end,
                "row_count": end - start + 1,
            }
            if branch == "correlated":
                value["canonical_security"] = {"alpaca_asset_id": asset}
            else:
                value["reason_codes"] = ["ambiguous_security_identity_candidates"]
            return value

        baseline = {
            "SUB": {
                "source_row_count": 2,
                "correlated": {"row_count": 1, "segments": [segment("correlated", 2, 2, "id")]},
                "uncorrelated": {"row_count": 1, "segments": [segment("uncorrelated", 1, 1)]},
            }
        }
        resolved = copy.deepcopy(baseline)
        resolved["SUB"]["correlated"] = {
            "row_count": 2,
            "segments": [segment("correlated", 1, 1, "id"), segment("correlated", 2, 2, "id")],
        }
        resolved["SUB"]["uncorrelated"] = {"row_count": 0, "segments": []}

        invariant = _validate_phase_monotonicity(baseline, resolved)

        self.assertEqual(1, invariant["newly_resolved_row_count"])
        changed = copy.deepcopy(resolved)
        changed["SUB"]["correlated"]["segments"][1]["canonical_security"] = {"alpaca_asset_id": "different"}
        with self.assertRaisesRegex(PolicyLabError, "established correlated row"):
            _validate_phase_monotonicity(baseline, changed)

        completed = {
            "source_row_count": 1,
            "correlated": {"row_count": 1, "segments": [segment("correlated", 1, 1, "sibling")]},
            "uncorrelated": {"row_count": 0, "segments": []},
            "evidence_issues": [],
        }
        grouped_baseline = {**baseline, "DONE": completed}
        grouped_resolved = {**resolved, "DONE": copy.deepcopy(completed)}
        invariant = _validate_phase_monotonicity(grouped_baseline, grouped_resolved)
        self.assertEqual(["DONE"], invariant["completed_sources_unchanged"])

        changed_sibling = copy.deepcopy(grouped_resolved)
        changed_sibling["DONE"]["evidence_issues"].append({"reason_code": "changed"})
        with self.assertRaisesRegex(PolicyLabError, "completed baseline report"):
            _validate_phase_monotonicity(grouped_baseline, changed_sibling)

    def test_corrected_producers_run_once_in_order_and_their_plan_is_frozen(self):
        policy = load_policy(CURRENT_QUALITY_POLICY)
        baseline = {
            "SUB": {
                "source_row_count": 1,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 1, "segments": []},
                "ticker_ownership_intervals": [],
            }
        }
        final = copy.deepcopy(baseline)
        certificate = ActiveSuffixCertificate(
            rule_id="GENERIC",
            symbol="SUB",
            asset_id="current-id",
            cusip="222222222",
            anchor_date=date(2024, 2, 1),
            competing_last_date=date(2024, 1, 1),
            anchor_action_ids=("anchor",),
            later_action_ids=("later",),
            candidate_pairs=(("current-id", "222222222"), ("old-id", "111111111")),
            evidence_sha256="a" * 64,
            pricing_generation="b" * 64,
            end_exclusive=date(2024, 3, 1),
            suffix_sessions=(date(2024, 2, 1),),
        )
        certificate_report = {
            "fact_producers": ["active_cusip_dividend_suffix_v2"],
            "rules": [],
            "certificate_count": 1,
            "certificates": [certificate.as_report_object()],
            "certificate_sha256": "c" * 64,
            "evaluations": [],
            "trace_truncated": False,
        }
        quality = {
            "fact_producers": [],
            "rules": [],
            "raw_evidence_sha256": {"SUB": "d" * 64},
            "candidate_evidence_sha256": {"SUB": "d" * 64},
            "quarantine_count": 0,
            "quarantines": [],
            "trace_truncated": False,
        }
        events = []

        def fixed_point(*_args, **_kwargs):
            events.append("phase_one" if not events else "phase_two")
            return baseline if len(events) == 1 else final

        def raw_candidate(*_args, **_kwargs):
            events.append("raw_candidate")
            return True

        def reconciliation(*_args, **_kwargs):
            events.append("reconciliation")
            return {}, [], quality

        def suffix(*_args, **_kwargs):
            events.append("suffix")
            return (certificate,), certificate_report

        with (
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._correlate_until_stable",
                side_effect=fixed_point,
            ) as fixed,
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.may_have_action_anchored_current_suffix",
                side_effect=raw_candidate,
            ) as raw,
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._corrected_reconciliations",
                side_effect=reconciliation,
            ) as reconcile,
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.prepare_active_suffix_policy",
                side_effect=suffix,
            ) as prepare,
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks._validate_phase_monotonicity",
                return_value={"only_baseline_unresolved_rows_resolved": True},
            ),
            patch("libs.alpaca_lineage_policy_lab.fallbacks._validate_corrected_certificate_effect"),
            patch("libs.alpaca_lineage_policy_lab.fallbacks._report_rows", return_value={}),
        ):
            _, trace = correlate_schema_four(
                Path("/unused"),
                {"SUB": self._view()},
                {"SUB": {}},
                policy,
                selected_group=["SUB"],
                unavailable_pricing=[],
                evidence_generations={"SUB": "a" * 64},
                pricing_generations={"SUB": "b" * 64},
            )

        self.assertEqual(
            ["phase_one", "raw_candidate", "reconciliation", "suffix", "phase_two"],
            events,
        )
        self.assertEqual(2, fixed.call_count)
        raw.assert_called_once()
        reconcile.assert_called_once()
        prepare.assert_called_once()
        self.assertIs(fixed.call_args_list[1].args[4], baseline)
        self.assertEqual(frozenset(("SUB",)), fixed.call_args_list[1].kwargs["suffix_sources"])
        self.assertEqual(trace["baseline_sha256"], trace["phase_two"]["seed_sha256"])
        self.assertTrue(trace["baseline_frozen"])

    def test_duplicate_unknown_unsafe_and_extra_yaml_fail_closed(self):
        malformed = {
            "duplicate root key": self.text + "\nschema_version: 1\n",
            "unsafe tag": self.text.replace(
                "policy_id: alpaca_security_correlation",
                "policy_id: !!python/object/apply:os.system ['echo forbidden']",
            ),
            "non-mapping root": "- not\n- a\n- policy\n",
        }
        for label, text in malformed.items():
            with self.subTest(label=label), self.assertRaises(PolicyLabError):
                self.load_text(text, name=label.replace(" ", "_") + ".yaml")

        variants = []
        extra_root = copy.deepcopy(self.document)
        extra_root["surprise"] = True
        variants.append(("extra root", extra_root))
        extra_stage = copy.deepcopy(self.document)
        extra_stage["stages"]["row_assignment"]["callable"] = "os.system"
        variants.append(("extra stage", extra_stage))
        unknown_rule = copy.deepcopy(self.document)
        unknown_rule["stages"]["row_assignment"]["rules"][0] = "not_registered"
        variants.append(("unknown rule", unknown_rule))
        duplicate_rule = copy.deepcopy(self.document)
        duplicate_rule["stages"]["row_assignment"]["rules"].append(
            duplicate_rule["stages"]["row_assignment"]["rules"][0]
        )
        variants.append(("duplicate rule", duplicate_rule))
        for label, document in variants:
            with self.subTest(label=label), self.assertRaises(PolicyLabError):
                self.load_document(document, name=label.replace(" ", "_") + ".yaml")

    def test_types_default_and_terminal_are_strict(self):
        variants = []
        for path, value in (
            (("schema_version",), True),
            (("policy_version",), 1),
            (("implementation", "correlation_version"), True),
            (("fixed_point", "max_passes"), True),
            (("stages", "ownership_intervals", "rules"), "not-a-list"),
        ):
            document = copy.deepcopy(self.document)
            target = document
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            variants.append((".".join(path), document))

        missing_default = copy.deepcopy(self.document)
        missing_default["stages"]["row_assignment"].pop("default")
        variants.append(("missing default", missing_default))
        wrong_default = copy.deepcopy(self.document)
        wrong_default["stages"]["row_assignment"]["default"] = "unique_uuid_candidate_with_warnings"
        variants.append(("wrong default", wrong_default))
        repeated_default = copy.deepcopy(self.document)
        repeated_default["stages"]["row_assignment"]["rules"].append("unresolved_uuid_candidates")
        variants.append(("default repeated as rule", repeated_default))
        wrong_terminal = copy.deepcopy(self.document)
        wrong_terminal["stages"]["terminal_promotion"]["rule"] = "unresolved_uuid_candidates"
        variants.append(("wrong terminal", wrong_terminal))

        for label, document in variants:
            with self.subTest(label=label), self.assertRaises(PolicyLabError):
                self.load_document(document, name=label.replace(" ", "_") + ".yaml")


class EvidenceQualityPolicyTests(TestCase):
    @staticmethod
    def _asset(asset_id, symbol):
        return {
            "id": asset_id,
            "class": "us_equity",
            "exchange": "NYSE",
            "symbol": symbol,
            "name": f"{symbol} Incorporated",
            "status": "active",
            "tradable": True,
        }

    @classmethod
    def _lookup(cls, kind, value, asset_id, symbol):
        return {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": cls._asset(asset_id, symbol),
        }

    @staticmethod
    def _dividend(action_id, symbol, cusip, ex_date, rate):
        return {
            "id": action_id,
            "action_type": "cash_dividend",
            "symbol": symbol,
            "cusip": cusip,
            "ex_date": ex_date,
            "record_date": ex_date,
            "payable_date": ex_date[:5] + "12-20",
            "process_date": ex_date[:5] + "12-20",
            "rate": rate,
            "foreign": False,
            "special": False,
        }

    @classmethod
    def evidence(cls):
        owner_cusip = "123456789"
        subject_cusip = "987654321"
        subject_action = cls._dividend("subject-event", "LEFT", owner_cusip, "2024-06-17", 0.559028)
        owner_action = cls._dividend("owner-event", "RIGHT", owner_cusip, "2024-06-17", 0.5590278)
        left_actions = [
            cls._dividend("left-before", "LEFT", subject_cusip, "2024-01-15", 0.1),
            subject_action,
            cls._dividend("left-after", "LEFT", subject_cusip, "2025-01-15", 0.1),
        ]
        right_actions = [
            cls._dividend("right-before", "RIGHT", owner_cusip, "2024-01-15", 0.2),
            owner_action,
            cls._dividend("right-after", "RIGHT", owner_cusip, "2025-01-15", 0.2),
        ]

        def document(symbol, actions, lookups):
            return {
                "query": {
                    "input_symbol": symbol,
                    "corporate_actions_start": "2023-01-01",
                    "corporate_actions_end": "2026-01-01",
                    "corporate_actions_data_quality": "complete",
                },
                "catalogue_observed_at_utc": "2026-01-02T03:04:05Z",
                "queried_symbols": [symbol],
                "corporate_action_count": len(actions),
                "corporate_actions": actions,
                "asset_lookups": lookups,
                "identities": [
                    {
                        **copy.deepcopy(lookup["asset"]),
                        "lookup_symbols": [lookup["lookup"]] if lookup["lookup_kind"] == "symbol" else [],
                        "lookup_cusips": [lookup["lookup"]] if lookup["lookup_kind"] == "cusip" else [],
                    }
                    for lookup in lookups
                ],
                "symbol_changes": [],
                "symbol_occupancy_evidence": [],
                "input_symbol_occupants": [],
                "unresolved_cusips": [],
                "symbol_disposition": {},
                "warnings": ["source warning"],
            }

        left_lookups = [
            cls._lookup("symbol", "LEFT", "left-asset", "LEFT"),
            cls._lookup("cusip", subject_cusip, "left-asset", "LEFT"),
            cls._lookup("cusip", owner_cusip, "right-asset", "RIGHT"),
        ]
        right_lookups = [
            cls._lookup("symbol", "RIGHT", "right-asset", "RIGHT"),
            cls._lookup("cusip", owner_cusip, "right-asset", "RIGHT"),
        ]
        return {
            "LEFT": document("LEFT", left_actions, left_lookups),
            "RIGHT": document("RIGHT", right_actions, right_lookups),
        }

    def setUp(self):
        self.policy = _load_running_single_phase_policy(QUALITY_POLICY)
        self.generations = {"LEFT": "a" * 64, "RIGHT": "b" * 64}
        self.local = {
            "LEFT": {"present": True, "downloads": []},
            "RIGHT": {"present": True, "downloads": []},
        }

    def apply(self, evidence):
        return apply_evidence_quality_policy(
            evidence,
            self.policy.evidence_rules,
            evidence_generations=self.generations,
            local_reports=self.local,
        )

    def test_schema_three_policy_is_typed_and_strict(self):
        self.assertEqual(3, self.policy.schema_version)
        self.assertEqual((FACT_PRODUCER,), self.policy.evidence_fact_producers)
        self.assertEqual(FACT_TYPES, dict(self.policy.evidence_fact_types))
        self.assertEqual(
            (ACTIVE_SUFFIX_FACT_PRODUCER,),
            self.policy.active_suffix_fact_producers,
        )
        self.assertEqual(ACTIVE_SUFFIX_FACT_TYPES, dict(self.policy.active_suffix_fact_types))
        document = yaml.safe_load(QUALITY_POLICY.read_text(encoding="utf-8"))
        document["implementation"]["correlation_version"] = versions()["correlation"]
        variants = []
        unknown_fact = copy.deepcopy(document)
        unknown_fact["stages"]["evidence_quality"]["rules"][0]["when"]["all"][0]["fact"] = "quality.unknown"
        variants.append(unknown_fact)
        wrong_type = copy.deepcopy(document)
        wrong_type["facts"]["quality.same_cusip"]["type"] = "string"
        variants.append(wrong_type)
        wrong_effect = copy.deepcopy(document)
        wrong_effect["stages"]["evidence_quality"]["rules"][0]["effect"]["target"] = "identity"
        variants.append(wrong_effect)
        arbitrary_producer = copy.deepcopy(document)
        arbitrary_producer["fact_producers"] = ["module.callable"]
        variants.append(arbitrary_producer)
        missing_gate = copy.deepcopy(document)
        missing_gate["stages"]["evidence_quality"]["rules"][0]["when"]["all"].pop()
        variants.append(missing_gate)
        duplicate_gate = copy.deepcopy(document)
        duplicate_gate["stages"]["evidence_quality"]["rules"][0]["when"]["all"].append(
            copy.deepcopy(duplicate_gate["stages"]["evidence_quality"]["rules"][0]["when"]["all"][0])
        )
        variants.append(duplicate_gate)
        with tempfile.TemporaryDirectory() as directory:
            for index, variant in enumerate(variants):
                path = Path(directory) / f"invalid-{index}.yaml"
                path.write_text(yaml.safe_dump(variant, sort_keys=False), encoding="utf-8")
                with self.subTest(index=index), self.assertRaises(PolicyLabError):
                    load_policy(path)

    def test_precision_equivalence_is_exact_bounded_and_unknown_safe(self):
        self.assertTrue(_rate_equivalent("0.559028", "0.5590278"))
        self.assertTrue(_rate_equivalent("0.559028", "0.559028"))
        self.assertTrue(_rate_equivalent("1.0000005", "1.0000000"))
        self.assertFalse(_rate_equivalent("1.0000015", "1.000001"))
        self.assertFalse(_rate_equivalent("0.559028", "0.5590269"))
        self.assertIsNone(_rate_equivalent(True, "0.559028"))
        self.assertIsNone(_rate_equivalent("NaN", "0.559028"))
        self.assertIsNone(_rate_equivalent("Infinity", "0.559028"))
        self.assertIsNone(_rate_equivalent("0", "0.559028"))

    def test_exact_and_precision_equivalent_pairs_both_match(self):
        for label, subject_rate in (
            ("exact", 0.5590278),
            ("precision-equivalent", 0.559028),
        ):
            evidence = self.evidence()
            evidence["LEFT"]["corporate_actions"][1]["rate"] = subject_rate
            with self.subTest(label=label):
                _, report = self.apply(evidence)
                self.assertEqual(1, report["quarantine_count"])

    def test_generic_projection_is_deterministic_coherent_and_nonmutating(self):
        evidence = self.evidence()
        original = copy.deepcopy(evidence)
        first_candidate, first_report = self.apply(evidence)
        second_candidate, second_report = self.apply(evidence)

        self.assertEqual(original, evidence)
        self.assertEqual(first_candidate, second_candidate)
        self.assertEqual(first_report, second_report)
        self.assertFalse(first_report["trace_truncated"])
        self.assertEqual(1, first_report["quarantine_count"])
        self.assertNotEqual(
            first_report["raw_evidence_sha256"]["LEFT"],
            first_report["candidate_evidence_sha256"]["LEFT"],
        )
        self.assertEqual(
            first_report["raw_evidence_sha256"]["RIGHT"],
            first_report["candidate_evidence_sha256"]["RIGHT"],
        )
        self.assertEqual(
            ["left-before", "left-after"],
            [action["id"] for action in first_candidate["LEFT"]["corporate_actions"]],
        )
        self.assertEqual(2, first_candidate["LEFT"]["corporate_action_count"])
        self.assertNotIn(
            "123456789",
            [lookup["lookup"] for lookup in first_candidate["LEFT"]["asset_lookups"]],
        )
        self.assertIn(
            "LEFT",
            [
                lookup["lookup"]
                for lookup in first_candidate["LEFT"]["asset_lookups"]
                if lookup["lookup_kind"] == "symbol"
            ],
        )
        self.assertEqual(
            ["left-asset"],
            [identity["id"] for identity in first_candidate["LEFT"]["identities"]],
        )
        self.assertEqual([], first_candidate["LEFT"]["symbol_changes"])
        self.assertEqual([], first_candidate["LEFT"]["unresolved_cusips"])
        self.assertEqual(["source warning"], first_candidate["LEFT"]["warnings"])
        self.assertEqual(original["LEFT"]["query"], first_candidate["LEFT"]["query"])
        self.assertEqual(
            original["LEFT"]["catalogue_observed_at_utc"],
            first_candidate["LEFT"]["catalogue_observed_at_utc"],
        )
        self.assertEqual(1, len(first_candidate["LEFT"]["input_symbol_occupants"]))
        self.assertEqual(original["RIGHT"], first_candidate["RIGHT"])
        trace = first_report["quarantines"][0]
        self.assertEqual("subject-event", trace["subject"]["action_id"])
        self.assertEqual("owner-event", trace["reference"]["action_id"])
        self.assertEqual(self.generations["LEFT"], trace["subject"]["evidence_sha256"])
        self.assertEqual(self.generations["RIGHT"], trace["reference"]["evidence_sha256"])
        self.assertEqual(["subject-event"], trace["projection_removals"]["actions"])
        self.assertFalse(trace["facts"]["quality.selected_structural_link_present"])
        self.assertFalse(trace["facts"]["quality.endpoint_lookup_unresolved"])
        for fact in (
            "quality.selected_structural_link_present",
            "quality.endpoint_lookup_unresolved",
        ):
            references = trace["fact_evidence"][fact]
            self.assertTrue(references)
            self.assertTrue(all(item["evidence_sha256"] in self.generations.values() for item in references))
        self.assertEqual(
            [],
            trace["fact_evidence"]["quality.selected_structural_link_present"][0]["structural_action_ids"],
        )
        self.assertEqual(
            [],
            trace["fact_evidence"]["quality.selected_structural_link_present"][0]["uuid_edge_action_ids"],
        )

    def test_missing_conflicting_or_explained_evidence_abstains(self):
        variants = {}
        rate_mismatch = self.evidence()
        rate_mismatch["LEFT"]["corporate_actions"][1]["rate"] = 0.5
        variants["rate mismatch"] = rate_mismatch
        missing_bracket = self.evidence()
        missing_bracket["LEFT"]["corporate_actions"].pop()
        variants["missing bracket"] = missing_bracket
        incomplete = self.evidence()
        incomplete["LEFT"]["query"]["corporate_actions_data_quality"] = "all"
        variants["incomplete query"] = incomplete
        missing_query = self.evidence()
        missing_query["LEFT"].pop("query")
        variants["missing query"] = missing_query
        duplicate_reference = self.evidence()
        duplicate_reference["RIGHT"]["corporate_actions"].append(
            {**copy.deepcopy(duplicate_reference["RIGHT"]["corporate_actions"][1]), "id": "owner-event-copy"}
        )
        variants["ambiguous reference"] = duplicate_reference
        duplicate_subject_id = self.evidence()
        duplicate_subject_id["LEFT"]["corporate_actions"].append(
            copy.deepcopy(duplicate_subject_id["LEFT"]["corporate_actions"][1])
        )
        duplicate_subject_id["LEFT"]["corporate_action_count"] = len(
            duplicate_subject_id["LEFT"]["corporate_actions"]
        )
        variants["duplicate subject action ID"] = duplicate_subject_id
        shared_action_id = self.evidence()
        shared_action_id["RIGHT"]["corporate_actions"][1]["id"] = "subject-event"
        variants["subject and reference action IDs match"] = shared_action_id
        ambiguous = self.evidence()
        ambiguous["LEFT"]["asset_lookups"].append(self._lookup("cusip", "123456789", "another-asset", "RIGHT"))
        variants["ambiguous lookup"] = ambiguous
        inconsistent_route = self.evidence()
        inconsistent_route["LEFT"]["asset_lookups"][0]["asset"]["symbol"] = "RIGHT"
        variants["inconsistent symbol route"] = inconsistent_route
        incomplete_projection = self.evidence()
        incomplete_projection["LEFT"]["asset_lookups"].append({"lookup_kind": "issuer"})
        variants["incomplete projection closure"] = incomplete_projection
        explained = self.evidence()
        explained["LEFT"]["corporate_actions"].append(
            {
                "id": "rename",
                "action_type": "name_change",
                "process_date": "2024-06-18",
                "old_symbol": "LEFT",
                "old_cusip": "123456789",
                "new_symbol": "RIGHT",
                "new_cusip": "123456789",
            }
        )
        explained["LEFT"]["symbol_changes"].append(
            {
                "id": "rename",
                "process_date": "2024-06-18",
                "old_symbol": "LEFT",
                "old_cusip": "123456789",
                "new_symbol": "RIGHT",
                "new_cusip": "123456789",
            }
        )
        variants["structural explanation"] = explained

        for label, evidence in variants.items():
            with self.subTest(label=label):
                candidate, report = self.apply(evidence)
                self.assertEqual(evidence, candidate)
                self.assertEqual(0, report["quarantine_count"])

    def test_structural_terminal_action_and_uuid_edge_are_vetoes(self):
        evidence = self.evidence()
        endpoint_ids = frozenset(("left-asset", "right-asset"))
        cusips = {"123456789", "987654321"}
        evidence["LEFT"]["corporate_actions"].append(
            {
                "id": "terminal-event",
                "action_type": "cash_merger",
                "acquiree_symbol": "LEFT",
                "acquiree_cusip": "987654321",
            }
        )
        with patch(
            "libs.alpaca_lineage_policy_lab.evidence_quality.build_uuid_lineage_edges",
            return_value=([], []),
        ):
            present, references = _structural_link(evidence, {"LEFT", "RIGHT"}, cusips, endpoint_ids)
        self.assertTrue(present)
        self.assertEqual(["terminal-event"], references["LEFT"]["structural_action_ids"])

        evidence = self.evidence()
        edge = SimpleNamespace(
            action_id="selected-edge",
            predecessor_asset_id="right-asset",
            successor_asset_id="left-asset",
        )
        with patch(
            "libs.alpaca_lineage_policy_lab.evidence_quality.build_uuid_lineage_edges",
            return_value=([edge], []),
        ):
            present, references = _structural_link(evidence, {"LEFT", "RIGHT"}, cusips, endpoint_ids)
        self.assertTrue(present)
        self.assertEqual(["selected-edge"], references["LEFT"]["uuid_edge_action_ids"])

    def test_malformed_uuid_edge_derivation_is_unknown(self):
        with patch(
            "libs.alpaca_lineage_policy_lab.evidence_quality.build_uuid_lineage_edges",
            return_value=([], [{"reason_code": "unresolved_edge"}]),
        ):
            present, references = _structural_link(
                self.evidence(),
                {"LEFT", "RIGHT"},
                {"123456789", "987654321"},
                frozenset(("left-asset", "right-asset")),
            )
        self.assertIsNone(present)
        self.assertEqual({}, references)

    def test_schema_one_policy_is_an_exact_noop(self):
        evidence = self.evidence()
        policy = _load_running_single_phase_policy(POLICY)
        candidate, report = apply_evidence_quality_policy(
            evidence,
            policy.evidence_rules,
            evidence_generations=self.generations,
            local_reports=self.local,
        )
        self.assertIs(evidence, candidate)
        self.assertEqual(0, report["quarantine_count"])
        self.assertEqual([], report["fact_producers"])
        self.assertEqual(report["raw_evidence_sha256"], report["candidate_evidence_sha256"])


class ActiveSuffixPolicyTests(TestCase):
    symbol = "CURRENT"
    old_cusip = "111111111"
    current_cusip = "222222222"
    old_asset_id = "old-asset"
    current_asset_id = "current-asset"
    overlap_date = "2024-01-03"
    anchor_date = "2024-02-01"
    repeat_date = "2024-03-01"
    end_exclusive = "2024-04-01"

    @classmethod
    def _asset(cls, asset_id, status):
        return {
            "id": asset_id,
            "class": "us_equity",
            "exchange": "NYSE",
            "symbol": cls.symbol,
            "name": "Current Holdings",
            "status": status,
            "tradable": status == "active",
        }

    @classmethod
    def _lookup(cls, kind, value, asset_id, status, *, source=None):
        result = {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": cls._asset(asset_id, status),
        }
        if source is not None:
            result["source"] = source
        return result

    @classmethod
    def _dividend(cls, action_id, cusip, ex_date, rate):
        return {
            "id": action_id,
            "action_type": "cash_dividend",
            "symbol": cls.symbol,
            "cusip": cusip,
            "ex_date": ex_date,
            "record_date": ex_date,
            "payable_date": ex_date,
            "process_date": ex_date,
            "rate": rate,
            "foreign": False,
            "special": False,
        }

    @classmethod
    def fixture(cls):
        old_overlap = cls._dividend(
            "old-overlap",
            cls.old_cusip,
            cls.overlap_date,
            "1.0000005",
        )
        current_overlap = cls._dividend(
            "current-overlap",
            cls.current_cusip,
            cls.overlap_date,
            "1.0000000",
        )
        anchor = cls._dividend("current-anchor", cls.current_cusip, cls.anchor_date, "1.1")
        repeat = cls._dividend("current-repeat", cls.current_cusip, cls.repeat_date, "1.2")
        actions = [old_overlap, current_overlap, anchor, repeat]
        evidence = {
            "query": {
                "input_symbol": cls.symbol,
                "corporate_actions_start": "2024-01-01",
                "corporate_actions_end": cls.end_exclusive,
                "corporate_actions_data_quality": "complete",
            },
            "catalogue_observed_at_utc": "2024-04-02T00:00:00Z",
            "corporate_actions": actions,
            "symbol_changes": [],
            "asset_lookups": [
                cls._lookup(
                    "symbol",
                    cls.symbol,
                    cls.current_asset_id,
                    "active",
                    source="asset_catalogue",
                ),
                cls._lookup("cusip", cls.old_cusip, cls.old_asset_id, "inactive"),
                cls._lookup("cusip", cls.current_cusip, cls.current_asset_id, "active"),
            ],
            "unresolved_cusips": [],
            "symbol_occupancy_evidence": [
                {
                    "symbol": cls.symbol,
                    "cusip": cls.old_cusip,
                    "asset_id": cls.old_asset_id,
                    "evidence": [
                        {
                            "action_id": "old-overlap",
                            "action_type": "cash_dividend",
                            "symbol": cls.symbol,
                            "cusip": cls.old_cusip,
                            "process_date": cls.overlap_date,
                        }
                    ],
                },
                {
                    "symbol": cls.symbol,
                    "cusip": cls.current_cusip,
                    "asset_id": cls.current_asset_id,
                    "evidence": [
                        {
                            "action_id": action["id"],
                            "action_type": "cash_dividend",
                            "symbol": cls.symbol,
                            "cusip": cls.current_cusip,
                            "process_date": action["process_date"],
                        }
                        for action in (current_overlap, anchor, repeat)
                    ],
                },
            ],
        }
        coverage = expected_market_session_coverage("2024-01-02", cls.end_exclusive)
        rows = tuple((day + "T17:00:00Z",) for day in coverage.session_dates)
        view = SimpleNamespace(
            views={adjustment: rows for adjustment in ADJUSTMENTS},
            source={
                "latest_request": {
                    "symbol": cls.symbol,
                    "asof": cls.end_exclusive,
                    "end_exclusive": cls.end_exclusive,
                }
            },
            local={
                "downloads": [
                    {
                        "adjustment": adjustment,
                        "rows": len(rows),
                        "sessions": len(rows),
                        "request_to": cls.end_exclusive,
                    }
                    for adjustment in ADJUSTMENTS
                ]
            },
        )
        report = {
            "row_partition_complete": True,
            "correlated": {"segments": [], "row_count": 0},
            "uncorrelated": {
                "row_count": len(rows) * len(ADJUSTMENTS),
                "segments": [
                    {
                        "from": coverage.session_dates[0],
                        "to": coverage.session_dates[-1],
                        "correlation_rule": {"name": "unresolved_uuid_candidates"},
                        "reason_codes": ["ambiguous_security_identity_candidates"],
                        "candidate_securities": [
                            {"asset_id": cls.old_asset_id, "cusip": cls.old_cusip},
                            {"asset_id": cls.current_asset_id, "cusip": cls.current_cusip},
                        ],
                    }
                ],
            },
        }
        quality = {
            "raw_evidence_sha256": {cls.symbol: "same-digest"},
            "candidate_evidence_sha256": {cls.symbol: "same-digest"},
            "quarantine_count": 0,
        }
        return evidence, view, report, quality

    def setUp(self):
        self.policy = _load_running_single_phase_policy(QUALITY_POLICY)

    def prepare(self, evidence, view, report, quality, *, edge_result=([], [])):
        original = copy.deepcopy(evidence)
        with patch(
            "libs.alpaca_lineage_policy_lab.active_suffix.build_uuid_lineage_edges",
            return_value=edge_result,
        ):
            result = prepare_active_suffix_policy(
                {self.symbol: evidence},
                {self.symbol: view},
                {self.symbol: report},
                self.policy.active_suffix_rules,
                selected_group=[self.symbol],
                unavailable_pricing=[],
                evidence_generations={self.symbol: "a" * 64},
                pricing_generations={self.symbol: "b" * 64},
                evidence_quality=quality,
            )
        self.assertEqual(original, evidence)
        return result

    def test_schema_three_active_stage_is_strict_and_fully_typed(self):
        self.assertEqual((ACTIVE_SUFFIX_FACT_PRODUCER,), self.policy.active_suffix_fact_producers)
        self.assertEqual(ACTIVE_SUFFIX_FACT_TYPES, dict(self.policy.active_suffix_fact_types))
        self.assertEqual(ACTIVE_SUFFIX_EFFECT, dict(self.policy.active_suffix_rules[0].effect))
        document = yaml.safe_load(QUALITY_POLICY.read_text(encoding="utf-8"))
        document["implementation"]["correlation_version"] = versions()["correlation"]
        variants = []
        unknown = copy.deepcopy(document)
        unknown["stages"]["active_suffix_assignment"]["rules"][0]["when"]["all"][0]["fact"] = "suffix.unknown"
        variants.append(unknown)
        missing = copy.deepcopy(document)
        missing["stages"]["active_suffix_assignment"]["rules"][0]["when"]["all"].pop()
        variants.append(missing)
        wrong_effect = copy.deepcopy(document)
        wrong_effect["stages"]["active_suffix_assignment"]["rules"][0]["effect"]["membership"] = (
            "joined_component"
        )
        variants.append(wrong_effect)
        wrong_position = copy.deepcopy(document)
        wrong_position["stages"]["active_suffix_assignment"]["position"] = "after_fallback"
        variants.append(wrong_position)
        with tempfile.TemporaryDirectory() as directory:
            for index, variant in enumerate(variants):
                path = Path(directory) / f"invalid-active-{index}.yaml"
                path.write_text(yaml.safe_dump(variant, sort_keys=False), encoding="utf-8")
                with self.subTest(index=index), self.assertRaises(PolicyLabError):
                    load_policy(path)

    def test_positive_certificate_is_deterministic_bounded_and_single_asset(self):
        evidence, view, report, quality = self.fixture()
        first_certificates, first_trace = self.prepare(evidence, view, report, quality)
        second_certificates, second_trace = self.prepare(evidence, view, report, quality)

        self.assertEqual(first_certificates, second_certificates)
        self.assertEqual(first_trace, second_trace)
        self.assertEqual(1, len(first_certificates))
        certificate = first_certificates[0]
        self.assertEqual(self.current_asset_id, certificate.asset_id)
        self.assertEqual(self.anchor_date, certificate.anchor_date.isoformat())
        self.assertEqual("a" * 64, certificate.evidence_sha256)
        self.assertEqual("b" * 64, certificate.pricing_generation)
        self.assertEqual(self.end_exclusive, certificate.end_exclusive.isoformat())
        self.assertFalse(first_trace["trace_truncated"])
        self.assertEqual(64, len(first_trace["certificate_sha256"]))
        certificate_trace = first_trace["certificates"][0]
        self.assertEqual("a" * 64, certificate_trace["evidence_sha256"])
        self.assertEqual("b" * 64, certificate_trace["pricing_generation"])
        self.assertEqual(self.end_exclusive, certificate_trace["pricing_end_exclusive"])
        evaluation = first_trace["evaluations"][0]
        self.assertEqual(self.policy.active_suffix_rules[0].rule_id, evaluation["matched_rule_id"])
        for fact, expected in self.policy.active_suffix_rules[0].conditions:
            self.assertIs(evaluation["facts"][fact], expected, fact)
        for fact in (
            "suffix.unresolved_relevant_occupancy_present",
            "suffix.competing_occupancy_on_or_after_anchor",
            "suffix.post_anchor_occupancy_closure_exact",
            "suffix.structural_candidate_link_present",
            "suffix.uuid_candidate_edge_present",
        ):
            self.assertTrue(evaluation["fact_evidence"][fact])

        dynamic = active_suffix_row_rule(first_certificates)
        before = dynamic.apply(
            SimpleNamespace(
                requested_symbol=self.symbol,
                session_date=certificate.anchor_date.replace(month=1, day=31),
            )
        )
        at_anchor = dynamic.apply(
            SimpleNamespace(requested_symbol=self.symbol, session_date=certificate.anchor_date)
        )
        self.assertIsNone(before)
        self.assertEqual(self.current_asset_id, at_anchor.asset_id)
        self.assertFalse(at_anchor.use_rule_identity_basis)
        self.assertFalse(at_anchor.component_only)
        self.assertEqual(ACTIVE_SUFFIX_ROW_RULE_NAME, at_anchor.correlation_method)
        self.assertIsNone(
            dynamic.apply(
                SimpleNamespace(
                    requested_symbol=self.symbol,
                    session_date=certificate.end_exclusive,
                )
            )
        )
        self.assertIsNone(
            dynamic.apply(
                SimpleNamespace(
                    requested_symbol=self.symbol,
                    session_date=certificate.anchor_date.replace(day=3),
                )
            )
        )
        ordered = with_active_suffix_rule(self.policy.row_rules, first_certificates)
        self.assertEqual(ACTIVE_SUFFIX_ROW_RULE_NAME, ordered[-2].name)
        self.assertEqual("unresolved_uuid_candidates", ordered[-1].name)

    def test_incomplete_ambiguous_or_conflicting_facts_abstain(self):
        variants = {}

        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"] = evidence["corporate_actions"][:-1]
        variants["one active anchor"] = (evidence, view, report, quality, "suffix.repeated_active_anchors")

        evidence, view, report, quality = self.fixture()
        evidence["query"]["corporate_actions_data_quality"] = "all"
        variants["incomplete query"] = (evidence, view, report, quality, "suffix.complete_covering_query")

        evidence, view, report, quality = self.fixture()
        evidence["asset_lookups"].append(
            self._lookup("cusip", self.current_cusip, self.current_asset_id, "active")
        )
        variants["ambiguous lookup"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.current_cusip_route_exact",
        )

        evidence, view, report, quality = self.fixture()
        evidence["unresolved_cusips"].append("333333333")
        variants["unresolved CUSIP"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.unresolved_relevant_occupancy_present",
        )

        evidence, view, report, quality = self.fixture()
        report["uncorrelated"]["segments"][0]["candidate_securities"].pop()
        variants["row candidates differ"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.row_candidates_exact",
        )

        evidence, view, report, quality = self.fixture()
        quality["quarantine_count"] = 1
        quality["candidate_evidence_sha256"][self.symbol] = "changed"
        variants["projected evidence changed"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.evidence_projection_unchanged",
        )

        evidence, view, report, quality = self.fixture()
        view.views[ADJUSTMENTS[-1]] = view.views[ADJUSTMENTS[-1]][:-1]
        variants["view grid differs"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.adjustment_sessions_consistent",
        )

        evidence, view, report, quality = self.fixture()
        evidence["symbol_occupancy_evidence"][0]["evidence"].append(
            {
                "action_id": "late-rival-occupancy",
                "action_type": "cash_dividend",
                "symbol": self.symbol,
                "cusip": self.old_cusip,
                "process_date": self.anchor_date,
            }
        )
        variants["late rival occupancy"] = (
            evidence,
            view,
            report,
            quality,
            "suffix.competing_occupancy_on_or_after_anchor",
        )

        for label, (evidence, view, report, quality, failed_fact) in variants.items():
            with self.subTest(label=label):
                certificates, trace = self.prepare(evidence, view, report, quality)
                self.assertFalse(certificates)
                self.assertIsNone(trace["evaluations"][0]["matched_rule_id"])
                self.assertIsNot(
                    trace["evaluations"][0]["facts"][failed_fact],
                    dict(self.policy.active_suffix_rules[0].conditions)[failed_fact],
                )

    def test_overlap_interleaving_structural_and_uuid_vetoes(self):
        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"][1]["rate"] = "1.01"
        certificates, trace = self.prepare(evidence, view, report, quality)
        self.assertFalse(certificates)
        self.assertFalse(trace["evaluations"][0]["facts"]["suffix.boundary_overlap_consistent"])

        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"].extend(
            [
                self._dividend("old-interleaving", self.old_cusip, "2024-02-15", "1.1"),
                self._dividend("current-later", self.current_cusip, "2024-03-15", "1.3"),
            ]
        )
        certificates, trace = self.prepare(evidence, view, report, quality)
        self.assertFalse(certificates)
        self.assertFalse(trace["evaluations"][0]["facts"]["suffix.histories_non_interleaving"])

        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"].append(
            {
                "id": "old-blank-structure",
                "action_type": "reverse_split",
                "symbol": self.symbol,
                "ex_date": "2023-01-03",
            }
        )
        certificates, _ = self.prepare(evidence, view, report, quality)
        self.assertEqual(1, len(certificates))

        for label, action in (
            (
                "requested label after anchor",
                {
                    "id": "late-blank-structure",
                    "action_type": "reverse_split",
                    "symbol": self.symbol,
                    "ex_date": "2024-02-15",
                },
            ),
            (
                "candidate endpoint at any date",
                {
                    "id": "old-endpoint-structure",
                    "action_type": "reverse_split",
                    "symbol": "OTHER",
                    "cusip": self.old_cusip,
                    "ex_date": "2023-01-03",
                },
            ),
        ):
            evidence, view, report, quality = self.fixture()
            evidence["corporate_actions"].append(action)
            with self.subTest(label=label):
                certificates, trace = self.prepare(evidence, view, report, quality)
                self.assertFalse(certificates)
                self.assertTrue(trace["evaluations"][0]["facts"]["suffix.structural_candidate_link_present"])

        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"].append(
            {
                "id": "conflicting-structural-dates",
                "action_type": "reverse_split",
                "symbol": self.symbol,
                "effective_date": "2023-01-03",
                "ex_date": "2024-02-15",
                "process_date": "2024-02-15",
            }
        )
        certificates, trace = self.prepare(evidence, view, report, quality)
        self.assertFalse(certificates)
        self.assertIsNone(trace["evaluations"][0]["facts"]["suffix.structural_candidate_link_present"])

        evidence, view, report, quality = self.fixture()
        edge = SimpleNamespace(
            action_id="candidate-edge",
            predecessor_asset_id=self.old_asset_id,
            successor_asset_id="outside-asset",
        )
        certificates, trace = self.prepare(evidence, view, report, quality, edge_result=([edge], []))
        self.assertFalse(certificates)
        self.assertTrue(trace["evaluations"][0]["facts"]["suffix.uuid_candidate_edge_present"])

        evidence, view, report, quality = self.fixture()
        certificates, trace = self.prepare(
            evidence,
            view,
            report,
            quality,
            edge_result=([], [{"reason_code": "malformed_edge"}]),
        )
        self.assertFalse(certificates)
        self.assertIsNone(trace["evaluations"][0]["facts"]["suffix.uuid_candidate_edge_present"])

    def test_every_post_anchor_occupancy_route_must_close_to_current_uuid(self):
        def add_occupancy(evidence, action, *, cusip, asset_id):
            evidence["corporate_actions"].append(action)
            occupant = next(
                (
                    item
                    for item in evidence["symbol_occupancy_evidence"]
                    if (item["cusip"], item["asset_id"]) == (cusip, asset_id)
                ),
                None,
            )
            if occupant is None:
                occupant = {
                    "symbol": self.symbol,
                    "cusip": cusip,
                    "asset_id": asset_id,
                    "evidence": [],
                }
                evidence["symbol_occupancy_evidence"].append(occupant)
            occupant["evidence"].append(
                {
                    "action_id": action["id"],
                    "action_type": action["action_type"],
                    "symbol": self.symbol,
                    "cusip": cusip,
                    "process_date": action["process_date"],
                }
            )

        evidence, view, report, quality = self.fixture()
        current_gain = {
            "id": "current-capital-gain",
            "action_type": "capital_gains_distribution",
            "symbol": self.symbol,
            "cusip": self.current_cusip,
            "ex_date": "2024-03-15",
            "process_date": "2024-03-15",
        }
        add_occupancy(
            evidence,
            current_gain,
            cusip=self.current_cusip,
            asset_id=self.current_asset_id,
        )
        certificates, trace = self.prepare(evidence, view, report, quality)
        self.assertEqual(1, len(certificates))
        self.assertTrue(trace["evaluations"][0]["facts"]["suffix.post_anchor_occupancy_closure_exact"])

        for label, cusip, asset_id in (
            ("rival", self.old_cusip, self.old_asset_id),
            ("third", "333333333", "third-asset"),
        ):
            evidence, view, report, quality = self.fixture()
            if label == "third":
                evidence["asset_lookups"].append(self._lookup("cusip", cusip, asset_id, "inactive"))
            action = {
                "id": f"{label}-capital-gain",
                "action_type": "capital_gains_distribution",
                "symbol": self.symbol,
                "cusip": cusip,
                "ex_date": "2024-03-15",
                "process_date": "2024-03-15",
            }
            add_occupancy(evidence, action, cusip=cusip, asset_id=asset_id)
            with self.subTest(label=label):
                certificates, trace = self.prepare(evidence, view, report, quality)
                facts = trace["evaluations"][0]["facts"]
                self.assertFalse(certificates)
                self.assertFalse(facts["suffix.post_anchor_occupancy_closure_exact"])
                self.assertTrue(facts["suffix.competing_occupancy_on_or_after_anchor"])

        evidence, view, report, quality = self.fixture()
        evidence["corporate_actions"].append(
            {
                "id": "blank-post-anchor-route",
                "action_type": "capital_gains_distribution",
                "symbol": self.symbol,
                "cusip": None,
                "ex_date": "2024-03-15",
                "process_date": "2024-03-15",
            }
        )
        certificates, trace = self.prepare(evidence, view, report, quality)
        self.assertFalse(certificates)
        facts = trace["evaluations"][0]["facts"]
        self.assertIsNone(facts["suffix.post_anchor_occupancy_closure_exact"])
        reference = trace["evaluations"][0]["fact_evidence"]["suffix.post_anchor_occupancy_closure_exact"][0]
        self.assertEqual(["blank-post-anchor-route"], reference["unknown_action_ids"])

    def test_schema_one_has_no_active_suffix_behavior(self):
        baseline = _load_running_single_phase_policy(POLICY)
        evidence, view, report, quality = self.fixture()
        certificates, trace = prepare_active_suffix_policy(
            {self.symbol: evidence},
            {self.symbol: view},
            {self.symbol: report},
            baseline.active_suffix_rules,
            selected_group=[self.symbol],
            unavailable_pricing=[],
            evidence_generations={self.symbol: "a" * 64},
            pricing_generations={self.symbol: "b" * 64},
            evidence_quality=quality,
        )
        self.assertFalse(certificates)
        self.assertEqual([], trace["fact_producers"])
        self.assertEqual([], trace["evaluations"])


class PolicyInjectionTests(TestCase):
    def setUp(self):
        self.policy = _load_running_single_phase_policy(POLICY)
        self.lineage = correlation_fixtures.RowCorrelationTests._overlapping_asof_lineage()
        self.lineage["symbol_disposition"] = {"classification": EXCHANGE_TEST_SYMBOL}
        self.rows = [{"t": "2024-01-03T12:00:00Z"}]
        self.source = correlation_fixtures.correlation_source("CURRENT", asof="2026-10-15")

    def test_current_policy_is_identical_to_default_correlation(self):
        default_intervals = build_ticker_ownership_intervals(self.lineage)
        policy_intervals = build_ticker_ownership_intervals(
            self.lineage,
            rules=self.policy.ownership_rules,
        )
        self.assertEqual(default_intervals, policy_intervals)

        default = correlate_download_rows(
            self.rows,
            self.source,
            self.lineage,
            default_intervals[0],
        )
        candidate = correlate_download_rows(
            self.rows,
            self.source,
            self.lineage,
            policy_intervals[0],
            row_rules=self.policy.row_rules,
        )
        self.assertEqual(default, candidate)
        self.assertEqual(
            "exchange_test_symbol_exclusion",
            candidate["uncorrelated"][0]["correlation_rule"]["name"],
        )

    def test_v58_profile_is_single_phase_and_cannot_enter_v59_fallbacks(self):
        policy = load_policy(POLICY)
        evidence = correlation_fixtures.RowCorrelationTests._overlapping_asof_lineage()
        evidence["symbol_disposition"] = {"classification": EXCHANGE_TEST_SYMBOL}
        inputs = inputs_for(
            {"CURRENT": bars(["2024-01-03"])},
            asofs={"CURRENT": "2026-10-15"},
        )

        with (
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.reconcile_duplicate_foreign_occupancy"
            ) as reconciliation,
            patch("libs.alpaca_lineage_policy_lab.fallbacks.may_have_action_anchored_current_suffix") as suffix,
        ):
            first = transform.correlate(Path("."), inputs, {"CURRENT": evidence}, policy=policy)
            second = transform.correlate(Path("."), inputs, {"CURRENT": evidence}, policy=policy)

        reconciliation.assert_not_called()
        suffix.assert_not_called()
        self.assertEqual(first, second)
        report = first["CURRENT"]
        self.assertEqual(
            ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES[:3]),
            report["ownership_interval_rule_order"],
        )
        self.assertNotIn(
            "action_anchored_current_ticker_occupancy",
            {item["name"] for item in report["ownership_interval_rule_order"]},
        )
        self.assertEqual(0, report["correlated"]["row_count"])
        self.assertEqual(len(ADJUSTMENTS), report["uncorrelated"]["row_count"])
        self.assertTrue(
            all(
                segment["correlation_rule"]["name"] == "exchange_test_symbol_exclusion"
                for segment in report["uncorrelated"]["segments"]
            )
        )

    def test_rule_removal_changes_result_and_diff_deterministically(self):
        document = yaml.safe_load(POLICY.read_text(encoding="utf-8"))
        document["implementation"]["correlation_version"] = versions()["correlation"]
        rules = document["stages"]["row_assignment"]["rules"]
        rules.remove("exchange_test_symbol_exclusion")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "without-exclusion.yaml"
            path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
            policy = load_policy(path)

        intervals = build_ticker_ownership_intervals(self.lineage, rules=policy.ownership_rules)[0]
        baseline = correlate_download_rows(self.rows, self.source, self.lineage, intervals)
        first = correlate_download_rows(
            self.rows,
            self.source,
            self.lineage,
            intervals,
            row_rules=policy.row_rules,
        )
        second = correlate_download_rows(
            self.rows,
            self.source,
            self.lineage,
            intervals,
            row_rules=policy.row_rules,
        )
        self.assertEqual(first, second)
        self.assertFalse(first["correlated"])
        self.assertEqual(
            "requested_symbol_dated_uuid_interval",
            first["uncorrelated"][0]["correlation_rule"]["name"],
        )
        self.assertNotIn("exchange_test_symbol_excluded", first["uncorrelated"][0]["reason_codes"])

        key = ("CURRENT", "raw", self.rows[0]["t"])
        before = {key: baseline["uncorrelated"][0]}
        after = {key: first["uncorrelated"][0]}
        first_diff = _difference(before, after, 20)
        self.assertEqual(first_diff, _difference(before, after, 20))
        self.assertEqual(1, first_diff["count"])
        self.assertNotEqual(first_diff["baseline_sha256"], first_diff["candidate_sha256"])

    def test_fixed_point_bound_is_policy_controlled_and_candidate_fed(self):
        view = SimpleNamespace(downloads={}, local={"downloads": []}, reader=None)
        seen = []
        pass_number = 0

        def loaders(_inputs, previous, _cache):
            seen.append(copy.deepcopy(previous))
            return None, None, None

        def never_stable(*_args, **_kwargs):
            nonlocal pass_number
            pass_number += 1
            return {"pass": pass_number}

        with (
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.rule_loaders",
                side_effect=loaders,
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.fallbacks.build_security_correlation_report",
                side_effect=never_stable,
            ) as build,
            self.assertRaisesRegex(LineageError, "fixed point"),
        ):
            transform.correlate(
                Path("/unused-policy-lab-store"),
                {"AAA": view},
                {"AAA": {}},
                policy=self.policy,
            )
        self.assertEqual(self.policy.max_passes, build.call_count)
        self.assertEqual({}, seen[0])
        self.assertEqual({"AAA": {"pass": 1}}, seen[1])
        self.assertEqual({"AAA": {"pass": 2}}, seen[2])

    def test_cache_cannot_cross_policy_boundaries(self):
        altered = tuple(rule for rule in self.policy.row_rules if rule.name != "exchange_test_symbol_exclusion")
        with self.assertRaisesRegex(SecurityCorrelationError, "policy-bound correlation cache"):
            correlate_download_rows(
                self.rows,
                self.source,
                self.lineage,
                build_ticker_ownership_intervals(self.lineage)[0],
                cache=CorrelationCache(),
                row_rules=altered,
            )
        cache = CorrelationCache(
            ownership_interval_rules=self.policy.ownership_rules,
            row_correlation_rules=self.policy.row_rules,
        )
        with self.assertRaisesRegex(SecurityCorrelationError, "cache row policy"):
            correlate_download_rows(
                self.rows,
                self.source,
                self.lineage,
                build_ticker_ownership_intervals(self.lineage)[0],
                cache=cache,
                row_rules=altered,
            )


class PolicyAuditTests(TestCase):
    def setUp(self):
        self.policy = load_policy(CURRENT_QUALITY_POLICY)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name)
        self.marker = self.store / "unchanged"
        self.marker.write_text("read-only", encoding="utf-8")
        self.source_generation = "b" * 64
        self.index = {
            "projection_state": {"schema_version": 1, "revision": 7, "complete": True},
            "symbols": {
                "AAA": {
                    "sha256": "a" * 64,
                    "source_sha256": self.source_generation,
                    "dependencies": [],
                    "asset_ids": ["first-asset", "second-asset"],
                    "group_members": ["AAA"],
                    "versions": versions(),
                    "projection": {
                        "source_rows": 8,
                        "unresolved_rows": 8,
                        "pricing_request": {
                            "symbol": "AAA",
                            "end_exclusive": "2026-10-24T00:00:00Z",
                            "asof": "2026-10-24",
                        },
                    },
                }
            },
        }
        self.receipt = {
            "symbol": "AAA",
            "versions": versions(),
            "input_sources": {"AAA": self.source_generation},
            "unavailable_pricing": {},
            "evidence": {},
            "dependencies": [],
            "boundaries": {},
            "row_partition_complete": True,
        }
        self.compare_report = {
            "group": ["AAA"],
            "evidence_quality": {
                "quarantine_count": 0,
                "quarantines": [],
                "trace_truncated": False,
            },
            "active_suffix": {
                "certificate_count": 1,
                "certificate_sha256": "c" * 64,
                "certificates": [{"rule_id": "GENERIC_SUFFIX"}],
                "trace_truncated": False,
            },
            "comparison": {
                "parity": False,
                "decision_parity": False,
                "diagnostic_snapshot_match": True,
                "exact_snapshot_parity": False,
                "migration_compatible": False,
                "migration_classification": None,
                "differences": {
                    "semantic": {
                        "count": 8,
                        "baseline_sha256": "d" * 64,
                        "candidate_sha256": "e" * 64,
                        "examples": [],
                    },
                    "observation": {
                        "count": 0,
                        "baseline_sha256": "f" * 64,
                        "candidate_sha256": "f" * 64,
                        "examples": [],
                    },
                    "explanation": {
                        "count": 0,
                        "baseline_sha256": "1" * 64,
                        "candidate_sha256": "1" * 64,
                        "examples": [],
                    },
                    "group_diagnostics": {
                        "count": 0,
                        "baseline_sha256": "2" * 64,
                        "candidate_sha256": "2" * 64,
                        "examples": [],
                    },
                },
                "memberships": {"match": False, "baseline": [], "candidate": []},
                "row_counts": {
                    "match": False,
                    "baseline": {
                        "total": 8,
                        "correlated": 0,
                        "unresolved": 8,
                        "excluded": 0,
                    },
                    "candidate": {
                        "total": 8,
                        "correlated": 8,
                        "unresolved": 0,
                        "excluded": 0,
                    },
                    "baseline_invariant": True,
                    "candidate_invariant": True,
                },
                "candidate_partition_complete": True,
                "mapping_corrections": {},
            },
        }

    @contextmanager
    def patched_audit(self, *, token_side_effect=None):
        @contextmanager
        def gate(*_args, **_kwargs):
            yield

        @contextmanager
        def locks(_store, symbols):
            yield set(symbols)

        def selected_path(root, relative, **_kwargs):
            return Path(root) / relative

        view = SimpleNamespace(source_sha256=self.source_generation, local={})
        with (
            patch("libs.alpaca_lineage_policy_lab.audit._hard_timeout", return_value=nullcontext()),
            patch("libs.alpaca_lineage_policy_lab.audit.check_store") as check_store,
            patch("libs.alpaca_lineage_policy_lab.audit.publication_lock", side_effect=gate),
            patch("libs.alpaca_lineage_policy_lab.audit.path_at", side_effect=selected_path),
            patch("libs.alpaca_lineage_policy_lab.audit.regular_path", side_effect=selected_path),
            patch(
                "libs.alpaca_lineage_policy_lab.audit.read_index",
                return_value=copy.deepcopy(self.index),
            ) as read_index,
            patch(
                "libs.alpaca_lineage_policy_lab.audit.read_receipt",
                return_value=copy.deepcopy(self.receipt),
            ) as read_receipt,
            patch("libs.alpaca_lineage_policy_lab.audit._quality_prefilter", return_value=False),
            patch("libs.alpaca_lineage_policy_lab.audit._suffix_prefilter", return_value=True),
            patch(
                "libs.alpaca_lineage_policy_lab.audit._shared_pricing_locks", side_effect=locks
            ) as pricing_locks,
            patch(
                "libs.alpaca_lineage_policy_lab.audit.assert_index_current",
                side_effect=token_side_effect,
            ) as assert_current,
            patch("libs.alpaca_lineage_policy_lab.audit.read_update", return_value=None) as read_update,
            patch(
                "libs.alpaca_lineage_policy_lab.audit.load_source",
                return_value={"symbol": "AAA"},
            ) as load_source,
            patch(
                "libs.alpaca_lineage_policy_lab.audit.load_pricing",
                return_value=view,
            ) as load_pricing,
            patch(
                "libs.alpaca_lineage_policy_lab.audit.read_boundaries",
                return_value={},
            ) as read_boundaries,
            patch("libs.alpaca_lineage_policy_lab.audit.check_unavailable_pricing") as unavailable,
            patch(
                "libs.alpaca_lineage_policy_lab.audit._compare_loaded_group",
                return_value=copy.deepcopy(self.compare_report),
            ) as compare_loaded,
            patch("libs.alpaca_lineage_build.storage.atomic_json") as write_lineage,
            patch("libs.alpaca_pricing_download.storage.write_file") as write_pricing,
        ):
            yield {
                "check_store": check_store,
                "read_index": read_index,
                "read_receipt": read_receipt,
                "pricing_locks": pricing_locks,
                "assert_current": assert_current,
                "read_update": read_update,
                "load_source": load_source,
                "load_pricing": load_pricing,
                "read_boundaries": read_boundaries,
                "unavailable": unavailable,
                "compare_loaded": compare_loaded,
                "write_lineage": write_lineage,
                "write_pricing": write_pricing,
            }

    def test_selector_prefilter_deduplicates_groups_and_includes_missing_hints(self):
        index = copy.deepcopy(self.index)
        shared = {
            "sha256": "1" * 64,
            "source_sha256": "2" * 64,
            "dependencies": [],
            "asset_ids": [],
            "versions": versions(),
            "projection": {"unresolved_rows": 0},
        }
        index["symbols"] = {
            "AAA": {**shared, "group_members": ["AAA", "BBB"]},
            "BBB": {**shared, "group_members": ["AAA", "BBB"]},
            "CCC": {
                **shared,
                "group_members": ["CCC"],
                "asset_ids": ["one", "two"],
                "projection": {"unresolved_rows": 1},
            },
            "EEE": {**shared, "group_members": ["DDD", "EEE"]},
            "FFF": {
                **shared,
                "group_members": ["FFF"],
                "projection": {},
            },
        }

        groups, incomplete = _selector_groups(index)
        selected = _index_prefilter(index, groups, self.policy)

        self.assertEqual(4, len(groups))
        self.assertEqual(1, incomplete)
        self.assertEqual(
            [("AAA", "BBB"), ("CCC",), ("FFF",)],
            selected,
        )

    def test_schema_four_index_gate_uses_phase_one_shape_not_published_unresolved_count(self):
        published = load_policy(V59_POLICY)
        corrected = load_policy(CURRENT_QUALITY_POLICY)
        shared = {
            "sha256": "1" * 64,
            "source_sha256": "2" * 64,
            "dependencies": [],
            "versions": versions(),
            "projection": {"unresolved_rows": 0},
        }
        index = {
            "projection_state": {"schema_version": 1, "revision": 7, "complete": True},
            "symbols": {
                "EXACT2": {
                    **shared,
                    "group_members": ["EXACT2"],
                    "asset_ids": ["old", "current"],
                },
                "THREE": {
                    **shared,
                    "group_members": ["THREE"],
                    "asset_ids": ["old", "middle", "current"],
                },
                "LIVE": {
                    **shared,
                    "group_members": ["LIVE", "UNAVAILABLE"],
                    "asset_ids": ["old", "current"],
                },
            },
        }
        groups, _ = _selector_groups(index)

        self.assertEqual(
            [("EXACT2",), ("LIVE", "UNAVAILABLE"), ("THREE",)],
            _index_prefilter(index, groups, published),
        )
        self.assertEqual(
            [("EXACT2",)],
            _index_prefilter(index, groups, corrected),
        )

    def test_published_receipt_prefilter_checks_every_available_source_and_allows_unavailable_members(self):
        published = load_policy(V59_POLICY)
        corrected = load_policy(CURRENT_QUALITY_POLICY)
        receipts = {
            "AAA": {"evidence": {"symbol": "AAA"}},
            "BBB": {"evidence": {"symbol": "BBB"}},
        }
        complete_group = _PreparedGroup(
            symbols=("AAA", "BBB"),
            receipts=receipts,
            unavailable={},
            receipt_generations={"AAA": "a" * 64, "BBB": "b" * 64},
            evidence_generations={"AAA": "e" * 64, "BBB": "f" * 64},
            pricing_generations={"AAA": "c" * 64, "BBB": "d" * 64},
        )
        incomplete_group = _PreparedGroup(
            symbols=("AAA",),
            receipts={"AAA": receipts["AAA"]},
            unavailable={"MISSING": {"reason": "unavailable"}},
            receipt_generations={"AAA": "a" * 64},
            evidence_generations={"AAA": "e" * 64},
            pricing_generations={"AAA": "c" * 64},
        )
        index = {"symbols": {}}

        with patch(
            "libs.alpaca_lineage_policy_lab.audit.may_have_action_anchored_current_suffix",
            side_effect=lambda evidence: evidence.get("symbol") in {"AAA", "BBB"},
        ) as candidate:
            self.assertTrue(_suffix_prefilter(complete_group, index, published))
            self.assertTrue(_suffix_prefilter(incomplete_group, index, published))
            self.assertFalse(_suffix_prefilter(complete_group, index, corrected))
            self.assertFalse(_suffix_prefilter(incomplete_group, index, corrected))

        self.assertGreaterEqual(candidate.call_count, 3)

    def test_audit_reads_every_selected_boundary_once_and_matches_comparison(self):
        with self.patched_audit() as calls:
            report = audit(self.store, self.policy)

        self.assertEqual("read-only", self.marker.read_text(encoding="utf-8"))
        self.assertTrue(report["complete"])
        self.assertEqual("differences", report["audit_conclusion"])
        self.assertEqual(1, report["scope"]["selected_receipt_count"])
        self.assertEqual(1, report["scope"]["evaluated_group_count"])
        self.assertEqual(
            _compact_group_result(
                self.compare_report,
                ("optional_fourth_ownership_fallback",),
            ),
            report["groups"][0],
        )
        calls["check_store"].assert_called_once_with(self.store)
        calls["read_index"].assert_called_once()
        calls["read_receipt"].assert_called_once()
        calls["pricing_locks"].assert_called_once_with(self.store, ["AAA"])
        self.assertEqual(2, calls["assert_current"].call_count)
        calls["read_update"].assert_called_once()
        calls["load_source"].assert_called_once()
        calls["load_pricing"].assert_called_once()
        calls["read_boundaries"].assert_called_once()
        calls["compare_loaded"].assert_called_once()
        calls["write_lineage"].assert_not_called()
        calls["write_pricing"].assert_not_called()

    def test_schema_four_summary_counts_fallback_decisions_intervals_and_certificates(self):
        report = copy.deepcopy(self.compare_report)
        report["active_suffix"]["certificate_count"] = 2
        report["active_suffix"]["certificates"] = [{"rule_id": "ONE"}, {"rule_id": "TWO"}]
        report["fallback_execution"] = {
            "profile": "corrected_v59",
            "producer_dependency": "independent_raw_evidence_projection_unchanged",
            "frozen_plan_sha256": "f" * 64,
            "phase_one": {},
            "reconciliation": {
                "producer": "reconcile_duplicate_foreign_occupancy_v2",
                "decision_count": 3,
                "decisions": [{"action_id": str(index)} for index in range(3)],
                "rate_evaluations": [],
            },
            "ownership_fallback": {
                "producer": "action_anchored_current_ticker_occupancy_v2",
                "selected_sources": ["AAA"],
                "legacy_reconciliation_enabled_sources": [],
                "applied_intervals": [{"interval": index} for index in range(12)],
            },
            "second_fixed_point_executed": True,
            "phase_two": {},
            "phase_invariants": {},
        }
        compact = _compact_group_result(report, ("optional_fourth_ownership_fallback",))

        self.assertEqual(3, compact["fallback_execution"]["reconciliation"]["decision_count"])
        self.assertEqual(12, compact["fallback_execution"]["ownership_fallback"]["applied_interval_count"])
        self.assertEqual(
            10,
            len(compact["fallback_execution"]["ownership_fallback"]["applied_intervals"]),
        )
        completed = _completed_report(
            self.policy,
            100,
            self.index,
            [("AAA",)],
            0,
            [("AAA",)],
            1,
            [],
            ["AAA"],
            [compact],
        )
        self.assertEqual(3, completed["summary"]["reconciliation_decision_count"])
        self.assertEqual(12, completed["summary"]["ownership_fallback_interval_count"])
        self.assertEqual(2, completed["summary"]["active_suffix_certificate_count"])

    def test_summary_factors_exact_decisions_and_diagnostics_without_weakening_parity(self):
        exact_report = copy.deepcopy(self.compare_report)
        exact_report["comparison"].update(
            parity=True,
            decision_parity=True,
            diagnostic_snapshot_match=True,
            exact_snapshot_parity=True,
            migration_compatible=True,
            migration_classification=None,
        )
        exact_report["comparison"]["memberships"]["match"] = True
        exact_report["comparison"]["row_counts"]["match"] = True
        exact_report["comparison"]["row_counts"]["candidate"] = copy.deepcopy(
            exact_report["comparison"]["row_counts"]["baseline"]
        )
        for difference in exact_report["comparison"]["differences"].values():
            difference.update(count=0, candidate_sha256=difference["baseline_sha256"])

        diagnostic_report = copy.deepcopy(exact_report)
        diagnostic_report["group"] = ["BBB"]
        diagnostic_report["comparison"].update(
            parity=False,
            diagnostic_snapshot_match=False,
            exact_snapshot_parity=False,
            migration_compatible=True,
            migration_classification="migration_preserved_rule_order_provenance",
        )
        diagnostic_report["comparison"]["differences"]["group_diagnostics"].update(
            count=2,
            candidate_sha256="3" * 64,
        )

        decision_report = copy.deepcopy(exact_report)
        decision_report["group"] = ["CCC"]
        decision_report["comparison"].update(
            parity=False,
            decision_parity=False,
            exact_snapshot_parity=False,
            migration_compatible=False,
        )
        decision_report["comparison"]["differences"]["semantic"].update(
            count=3,
            candidate_sha256="4" * 64,
        )

        exact = _compact_group_result(exact_report, ())
        diagnostic = _compact_group_result(diagnostic_report, ())
        decision = _compact_group_result(decision_report, ())

        diagnostic_only = _completed_report(
            self.policy,
            100,
            self.index,
            [("BBB",)],
            0,
            [("BBB",)],
            1,
            [],
            [],
            [diagnostic],
        )
        self.assertFalse(diagnostic_only["summary"]["parity"])
        self.assertTrue(diagnostic_only["summary"]["decision_parity"])
        self.assertFalse(diagnostic_only["summary"]["diagnostic_snapshot_match"])
        self.assertFalse(diagnostic_only["summary"]["exact_snapshot_parity"])
        self.assertTrue(diagnostic_only["summary"]["migration_compatible"])
        self.assertEqual(
            "decision_parity_with_migrated_diagnostics",
            diagnostic_only["audit_conclusion"],
        )

        aggregate = _completed_report(
            self.policy,
            100,
            self.index,
            [("AAA",), ("BBB",), ("CCC",)],
            0,
            [("AAA",), ("BBB",), ("CCC",)],
            3,
            [],
            [],
            [exact, diagnostic, decision],
        )
        summary = aggregate["summary"]
        self.assertEqual("differences", aggregate["audit_conclusion"])
        self.assertEqual(2, summary["changed_group_count"])
        self.assertEqual(1, summary["decision_mismatch_group_count"])
        self.assertEqual(1, summary["diagnostic_mismatch_group_count"])
        self.assertEqual(1, summary["migrated_compatible_group_count"])
        self.assertEqual(1, summary["migration_incompatible_group_count"])
        self.assertEqual(
            {"total": 3, "group_count": 1},
            summary["dimension_differences"]["semantic"],
        )
        self.assertEqual(
            {"total": 2, "group_count": 1},
            summary["dimension_differences"]["group_diagnostics"],
        )
        self.assertEqual(
            {"total": 0, "group_count": 0},
            summary["dimension_differences"]["observation"],
        )
        self.assertEqual(
            {"total": 0, "group_count": 0},
            summary["dimension_differences"]["explanation"],
        )

        exact_only = _completed_report(
            self.policy,
            100,
            self.index,
            [("AAA",)],
            0,
            [("AAA",)],
            1,
            [],
            [],
            [exact],
        )
        self.assertEqual("exact_parity", exact_only["audit_conclusion"])
        self.assertTrue(exact_only["summary"]["parity"])
        self.assertNotEqual(exact_only["semantic_sha256"], diagnostic_only["semantic_sha256"])

        from libs.alpaca_lineage_policy_lab import command_line

        output = io.StringIO()
        with (
            patch.object(command_line, "load_policy", return_value=self.policy),
            patch.object(command_line, "audit", return_value=diagnostic_only),
            patch("sys.stdout", output),
        ):
            exit_code = command_line.main(
                [
                    "audit",
                    "--store",
                    str(self.store),
                    "--policy",
                    str(CURRENT_QUALITY_POLICY),
                ]
            )
        self.assertEqual(1, exit_code)
        self.assertFalse(__import__("json").loads(output.getvalue())["summary"]["parity"])

    def test_migration_preserved_rule_order_classification_is_exact_and_fails_closed(self):
        policy = load_policy(CURRENT_POLICY)
        legacy_versions = {**versions(), "lineage": 33, "correlation": 59}
        legacy_versions.pop("correlation_policy_sha256")
        first_three = ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES[:3])
        first_four = ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES)
        receipt = {
            "schema_version": 33,
            "versions": legacy_versions,
            "evidence_issues": [],
            "ownership_interval_rule_order": first_three,
            "row_correlation_rule_order": [{"order": 1, "name": "row"}],
            "row_partition_complete": True,
        }
        report = {
            **copy.deepcopy(receipt),
            "ownership_interval_rule_order": first_four,
        }
        current_receipt = {
            **copy.deepcopy(receipt),
            "schema_version": 34,
            "versions": versions(),
            "correlation_policy": policy.provenance(),
        }
        current_report = {
            **copy.deepcopy(current_receipt),
            "ownership_interval_rule_order": first_four,
        }
        fallback = {
            "reconciliation": {"decision_count": 0},
            "ownership_fallback": {"selected_sources": [], "applied_intervals": []},
            "second_fixed_point_executed": False,
        }

        self.assertTrue(
            _migration_preserved_rule_order_provenance(
                policy,
                {"AAA": receipt},
                {"AAA": report},
                fallback,
            )
        )
        self.assertTrue(
            _migration_preserved_rule_order_provenance(
                policy,
                {"AAA": current_receipt},
                {"AAA": current_report},
                fallback,
            )
        )

        mixed_receipts = {
            "AAA": receipt,
            "BBB": current_receipt,
        }
        mixed_reports = {
            "AAA": report,
            "BBB": current_report,
        }
        self.assertFalse(
            _migration_preserved_rule_order_provenance(
                policy,
                mixed_receipts,
                mixed_reports,
                fallback,
            )
        )

        forged_current_receipts = []
        for field, value in (
            ("policy_role", "candidate"),
            ("semantic_sha256", "0" * 64),
            ("file_sha256", "not-a-sha256"),
        ):
            forged = copy.deepcopy(current_receipt)
            forged["correlation_policy"][field] = value
            forged_current_receipts.append(forged)
        policyless = copy.deepcopy(current_receipt)
        policyless.pop("correlation_policy")
        forged_current_receipts.append(policyless)
        wrong_schema = copy.deepcopy(current_receipt)
        wrong_schema["schema_version"] = 33
        forged_current_receipts.append(wrong_schema)

        for forged in forged_current_receipts:
            with self.subTest(forged=forged):
                self.assertFalse(
                    _migration_preserved_rule_order_provenance(
                        policy,
                        {"AAA": forged},
                        {"AAA": current_report},
                        fallback,
                    )
                )

        mutations = []
        changed = copy.deepcopy(receipt)
        changed["ownership_interval_rule_order"] = first_four
        mutations.append(({"AAA": changed}, {"AAA": report}, fallback))
        changed = copy.deepcopy(receipt)
        changed["row_partition_complete"] = False
        mutations.append(({"AAA": changed}, {"AAA": report}, fallback))
        changed_report = copy.deepcopy(report)
        changed_report["evidence_issues"] = [{"reason_code": "different"}]
        mutations.append(({"AAA": receipt}, {"AAA": changed_report}, fallback))
        for field, value in (
            ("second_fixed_point_executed", True),
            ("reconciliation", {"decision_count": 1}),
            (
                "ownership_fallback",
                {"selected_sources": ["AAA"], "applied_intervals": []},
            ),
            (
                "ownership_fallback",
                {"selected_sources": [], "applied_intervals": [{"symbol": "AAA"}]},
            ),
        ):
            changed_fallback = copy.deepcopy(fallback)
            changed_fallback[field] = value
            mutations.append(({"AAA": receipt}, {"AAA": report}, changed_fallback))

        for receipts, reports, trace in mutations:
            with self.subTest(receipts=receipts, reports=reports, trace=trace):
                self.assertFalse(
                    _migration_preserved_rule_order_provenance(
                        policy,
                        receipts,
                        reports,
                        trace,
                    )
                )
        self.assertFalse(
            _migration_preserved_rule_order_provenance(
                load_policy(CURRENT_QUALITY_POLICY),
                {"AAA": receipt},
                {"AAA": report},
                fallback,
            )
        )

    def test_selector_token_change_fails_closed_after_evaluation(self):
        with (
            self.patched_audit(token_side_effect=(None, LineageError("selector changed"))) as calls,
            self.assertRaisesRegex(LineageError, "selector changed"),
        ):
            audit(self.store, self.policy)
        self.assertEqual(2, calls["assert_current"].call_count)
        calls["write_lineage"].assert_not_called()
        calls["write_pricing"].assert_not_called()

    def test_shared_pricing_locks_are_deduplicated_sorted_and_read_only(self):
        calls = []

        @contextmanager
        def lock(root, *, exclusive):
            calls.append((root.name, exclusive))
            yield

        with (
            patch("libs.alpaca_lineage_policy_lab.audit._preflight_file_descriptors") as preflight,
            patch("libs.alpaca_lineage_policy_lab.audit.ticker_lock", side_effect=lock),
            _shared_pricing_locks(self.store, ["ZZZ", "AAA", "ZZZ"]) as locked,
        ):
            self.assertEqual({"AAA", "ZZZ"}, locked)
        preflight.assert_called_once_with(2)
        self.assertEqual([("AAA", False), ("ZZZ", False)], calls)

    def test_timeout_is_incomplete_and_has_no_audit_conclusion(self):
        with (
            patch("libs.alpaca_lineage_policy_lab.audit._hard_timeout", return_value=nullcontext()),
            patch(
                "libs.alpaca_lineage_policy_lab.audit._run_audit",
                side_effect=_AuditTimedOut("group evaluation"),
            ),
        ):
            report = audit(self.store, self.policy, timeout_seconds=1)
        self.assertEqual("timed_out", report["status"])
        self.assertFalse(report["complete"])
        self.assertIsNone(report["audit_conclusion"])
        self.assertEqual("group evaluation", report["phase"])

    def test_timeout_ceiling_is_not_part_of_completed_semantic_digest(self):
        with self.patched_audit():
            short = audit(self.store, self.policy, timeout_seconds=100)
        with self.patched_audit():
            long = audit(self.store, self.policy, timeout_seconds=200)

        self.assertEqual(100, short["timeout_seconds"])
        self.assertEqual(200, long["timeout_seconds"])
        self.assertEqual(short["semantic_sha256"], long["semantic_sha256"])


class PolicyComparisonTests(TestCase):
    @staticmethod
    def view_and_boundaries(*, split_october):
        instants = (
            "2025-12-31T05:00:00Z",
            "2026-01-02T05:00:00Z",
            "2026-10-01T04:00:00Z",
            "2026-10-02T04:00:00Z",
        )
        view = SimpleNamespace(views={name: tuple((instant,) for instant in instants) for name in ADJUSTMENTS})

        def segment(start, end):
            return {
                "branch": "uncorrelated",
                "from": start,
                "to": end,
                "reason_codes": ["deliberately_unresolved"],
            }

        ranges = {
            "completed_years": [("2025-12-31", "2025-12-31")],
            "current_year": [("2026-01-02", "2026-01-02")],
            "current_month": (
                [("2026-10-01", "2026-10-01"), ("2026-10-02", "2026-10-02")]
                if split_october
                else [("2026-10-01", "2026-10-02")]
            ),
        }
        boundaries = {
            kind: {
                "symbol": "AAA",
                "adjustments": [
                    {
                        "adjustment": adjustment,
                        "segments": [segment(start, end) for start, end in selected],
                    }
                    for adjustment in ADJUSTMENTS
                ],
            }
            for kind, selected in ranges.items()
        }
        return view, boundaries

    def test_normalization_ignores_equivalent_segment_splits(self):
        view, split = self.view_and_boundaries(split_october=True)
        _, combined = self.view_and_boundaries(split_october=False)
        self.assertEqual(
            _normalize_boundaries("AAA", split, view, "2026-10"),
            _normalize_boundaries("AAA", combined, view, "2026-10"),
        )

    def test_normalization_rejects_gaps_and_overlaps(self):
        view, boundaries = self.view_and_boundaries(split_october=True)
        missing = copy.deepcopy(boundaries)
        missing["current_month"]["adjustments"][0]["segments"].pop()
        with self.assertRaisesRegex(PolicyLabError, "do not exhaust"):
            _normalize_boundaries("AAA", missing, view, "2026-10")

        overlapping = copy.deepcopy(boundaries)
        segments = overlapping["current_month"]["adjustments"][0]["segments"]
        segments[0]["to"] = "2026-10-02"
        with self.assertRaisesRegex(PolicyLabError, "overlapping assignment"):
            _normalize_boundaries("AAA", overlapping, view, "2026-10")

    def test_group_loader_is_scoped_read_only_and_generation_bound(self):
        policy_input = {"document": "selected-source"}
        generation = document_digest(policy_input)
        receipt = {
            "symbol": "AAA",
            "versions": versions(),
            "input_sources": {"AAA": generation},
            "evidence": {},
            "anchor_month": "2026-10",
            "row_partition_complete": True,
        }
        entry = {"sha256": "a" * 64}
        snapshot = {
            "receipts": {"AAA": receipt},
            "boundaries": {"AAA": {}},
            "entries": {"AAA": entry},
            "locked": {"AAA"},
        }
        selected_input = object()
        calls = []

        @contextmanager
        def selected_snapshot(_store, select):
            calls.append(select(Path("/trusted/lineage")))
            yield snapshot

        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory)
            marker = store / "marker"
            marker.write_text("unchanged", encoding="utf-8")
            with (
                patch(
                    "libs.alpaca_lineage_policy_lab.comparison.select_inputs",
                    return_value={"AAA": entry},
                ) as select_inputs,
                patch(
                    "libs.alpaca_lineage_policy_lab.comparison.reader_snapshot",
                    side_effect=selected_snapshot,
                ),
                patch(
                    "libs.alpaca_lineage_policy_lab.comparison.unavailable_pricing",
                    return_value={},
                ),
                patch("libs.alpaca_lineage_policy_lab.comparison.check_unavailable_pricing") as unavailable,
                patch("libs.alpaca_lineage_policy_lab.comparison.read_update", return_value=None),
                patch(
                    "libs.alpaca_lineage_policy_lab.comparison.load_source",
                    return_value=policy_input,
                ) as load_source,
                patch(
                    "libs.alpaca_lineage_policy_lab.comparison.load_pricing",
                    return_value=selected_input,
                ) as load_pricing,
            ):
                with _load_group(store, "AAA") as selected:
                    self.assertIs(selected_input, selected["inputs"]["AAA"])
                    self.assertEqual(["AAA"], selected["selected_group"])
            self.assertEqual("unchanged", marker.read_text(encoding="utf-8"))

        select_inputs.assert_called_once_with(Path("/trusted/lineage"), "AAA")
        self.assertEqual([{"AAA": entry}], calls)
        unavailable.assert_called_once_with(store, {}, {"AAA"})
        load_source.assert_called_once()
        load_pricing.assert_called_once_with(store, "AAA", source=policy_input)

        changed_snapshot = copy.deepcopy(snapshot)
        changed_snapshot["receipts"]["AAA"]["input_sources"] = {"AAA": "f" * 64}

        @contextmanager
        def changed_generation(_store, _select):
            yield changed_snapshot

        with (
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.reader_snapshot",
                side_effect=changed_generation,
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.unavailable_pricing",
                return_value={},
            ),
            patch("libs.alpaca_lineage_policy_lab.comparison.check_unavailable_pricing"),
            patch("libs.alpaca_lineage_policy_lab.comparison.read_update", return_value=None),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.load_source",
                return_value=policy_input,
            ),
            patch("libs.alpaca_lineage_policy_lab.comparison.load_pricing") as load_pricing,
            self.assertRaisesRegex(PolicyLabError, "generation changed"),
        ):
            with _load_group(Path("/unused"), "AAA"):
                pass
        load_pricing.assert_not_called()

    def test_compare_keeps_baseline_out_of_candidate_inputs_and_enforces_counts(self):
        policy = load_policy(POLICY)
        evidence = {"identities": [], "corporate_actions": [], "symbol_occupancy_evidence": []}
        rule_order = row_correlation_rule_order(rules=policy.row_rules)
        ownership_order = ownership_interval_rule_order(rules=policy.ownership_rules)
        receipt = {
            # The current 33/59 publication is comparison data only; it is not
            # evidence for the historical v58 candidate execution.
            "versions": versions(),
            "evidence": evidence,
            "dependencies": [],
            "anchor_month": "2026-10",
            "source_rows": 1,
            "correlated_rows": 1,
            "unresolved_rows": 0,
            "evidence_issues": [],
            "ownership_interval_rule_order": ownership_order,
            "row_correlation_rule_order": rule_order,
            "row_partition_complete": True,
        }
        report = {
            "source_row_count": 1,
            "correlated": {"row_count": 1, "segments": []},
            "uncorrelated": {"row_count": 0, "segments": []},
            "evidence_issues": [],
            "ownership_interval_rule_order": ownership_order,
            "row_correlation_rule_order": rule_order,
            "row_partition_complete": True,
        }
        key = ("AAA", "raw", "2026-10-01T04:00:00Z")
        dimensions = {
            "semantic": {
                key: {
                    "branch": "correlated",
                    "security_identity": {
                        "security_id": "security-id",
                        "member_alpaca_asset_ids": ["asset-id"],
                    },
                }
            },
            "explanation": {key: {"correlation_rule": rule_order[-2]}},
            "observation": {key: {"observation_selection": "selected", "observation_tickers": ["AAA"]}},
        }
        selected = {
            "inputs": {"AAA": object()},
            "receipts": {"AAA": receipt},
            "boundaries": {"AAA": {"published": "baseline-only"}},
            "receipt_generations": {"AAA": "a" * 64},
            "evidence_generations": {"AAA": "e" * 64},
            "pricing_generations": {"AAA": "b" * 64},
            "unavailable_pricing": {},
            "selected_group": ["AAA"],
        }

        @contextmanager
        def loaded(*_args, **_kwargs):
            yield selected

        evidence_quality = {
            "quarantine_count": 0,
            "fact_producers": [],
            "raw_evidence_sha256": {"AAA": "e" * 64},
            "candidate_evidence_sha256": {"AAA": "e" * 64},
        }
        active_suffix = {
            "certificate_count": 0,
            "fact_producers": [],
            "certificates": [],
        }

        def evaluated(*_args, execution_trace, evidence_projection, **_kwargs):
            execution_trace.update(
                {
                    "evidence_quality": evidence_quality,
                    "active_suffix": active_suffix,
                }
            )
            evidence_projection.update({"AAA": evidence})
            return {"AAA": report}

        candidate_receipt = {**receipt, "boundary": "candidate"}
        with (
            patch("libs.alpaca_lineage_policy_lab.comparison._load_group", side_effect=loaded),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.correlate",
                side_effect=evaluated,
            ) as correlate,
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.select_observations",
                return_value=({"AAA": report}, {}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.make_publication",
                return_value=(candidate_receipt, {"candidate": "in-memory"}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison._group_dimensions",
                side_effect=(copy.deepcopy(dimensions), copy.deepcopy(dimensions)),
            ),
        ):
            result = compare("unused-store", "AAA", policy)

        self.assertTrue(result["comparison"]["parity"])
        self.assertEqual(0, result["evidence_quality"]["quarantine_count"])
        self.assertEqual([], result["evidence_quality"]["fact_producers"])
        self.assertEqual(0, result["active_suffix"]["certificate_count"])
        self.assertEqual([], result["active_suffix"]["fact_producers"])
        self.assertTrue(result["comparison"]["row_counts"]["baseline_invariant"])
        self.assertTrue(result["comparison"]["row_counts"]["candidate_invariant"])
        candidate_evidence = correlate.call_args.args[2]
        self.assertEqual({"AAA": evidence}, candidate_evidence)
        self.assertNotIn("published", str(candidate_evidence))
        self.assertIs(policy, correlate.call_args.kwargs["policy"])

        receipt["source_rows"] = 2
        with (
            patch("libs.alpaca_lineage_policy_lab.comparison._load_group", side_effect=loaded),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.correlate",
                side_effect=evaluated,
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.select_observations",
                return_value=({"AAA": report}, {}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.make_publication",
                return_value=(candidate_receipt, {"candidate": "in-memory"}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison._group_dimensions",
                side_effect=(copy.deepcopy(dimensions), copy.deepcopy(dimensions)),
            ),
        ):
            invalid = compare("unused-store", "AAA", policy)
        self.assertFalse(invalid["comparison"]["parity"])
        self.assertFalse(invalid["comparison"]["row_counts"]["baseline_invariant"])

    def test_compare_uses_shared_evaluator_trace_when_suffix_certificate_matches(self):
        policy = _load_running_single_phase_policy(QUALITY_POLICY)
        evidence = {"raw": "candidate-input"}
        rule_order = row_correlation_rule_order(rules=policy.row_rules)
        ownership_order = ownership_interval_rule_order(rules=policy.ownership_rules)
        receipt = {
            "versions": {**versions(), "correlation": policy.implementation_correlation_version},
            "evidence": evidence,
            "dependencies": [],
            "anchor_month": "2026-10",
            "source_rows": 1,
            "correlated_rows": 1,
            "unresolved_rows": 0,
            "evidence_issues": [],
            "ownership_interval_rule_order": ownership_order,
            "row_correlation_rule_order": rule_order,
            "row_partition_complete": True,
        }
        initial_report = {
            "source_row_count": 1,
            "correlated": {"row_count": 0, "segments": []},
            "uncorrelated": {"row_count": 1, "segments": []},
            "evidence_issues": [],
            "ownership_interval_rule_order": ownership_order,
            "row_correlation_rule_order": rule_order,
            "row_partition_complete": True,
        }
        candidate_report = {
            **initial_report,
            "correlated": {"row_count": 1, "segments": []},
            "uncorrelated": {"row_count": 0, "segments": []},
        }
        selected = {
            "inputs": {"AAA": object()},
            "receipts": {"AAA": receipt},
            "boundaries": {"AAA": {}},
            "receipt_generations": {"AAA": "a" * 64},
            "evidence_generations": {"AAA": "e" * 64},
            "pricing_generations": {"AAA": "b" * 64},
            "unavailable_pricing": [],
            "selected_group": ["AAA"],
        }
        key = ("AAA", "raw", "2026-10-01T04:00:00Z")
        dimensions = {
            "semantic": {
                key: {
                    "branch": "correlated",
                    "security_identity": {
                        "security_id": "security-id",
                        "member_alpaca_asset_ids": ["asset-id"],
                    },
                }
            },
            "explanation": {key: {}},
            "observation": {key: {}},
        }
        quality_report = {
            "quarantine_count": 0,
            "raw_evidence_sha256": {"AAA": "same"},
            "candidate_evidence_sha256": {"AAA": "same"},
        }
        active_report = {"certificate_count": 1, "certificates": [{"bounded": True}]}

        @contextmanager
        def loaded(*_args, **_kwargs):
            yield selected

        def evaluated(*_args, execution_trace, evidence_projection, **_kwargs):
            execution_trace.update(
                {
                    "evidence_quality": quality_report,
                    "active_suffix": active_report,
                }
            )
            evidence_projection.update({"AAA": evidence})
            return {"AAA": candidate_report}

        with (
            patch("libs.alpaca_lineage_policy_lab.comparison._load_group", side_effect=loaded),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.correlate",
                side_effect=evaluated,
            ) as correlate,
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.select_observations",
                return_value=({"AAA": candidate_report}, {}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.make_publication",
                return_value=({}, {}),
            ),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison._group_dimensions",
                side_effect=(copy.deepcopy(dimensions), copy.deepcopy(dimensions)),
            ),
        ):
            result = compare("unused-store", "AAA", policy)

        correlate.assert_called_once()
        self.assertIs(policy, correlate.call_args.kwargs["policy"])
        self.assertEqual({"AAA": evidence}, correlate.call_args.args[2])
        self.assertEqual(active_report, result["active_suffix"])


if __name__ == "__main__":
    import unittest

    unittest.main()
