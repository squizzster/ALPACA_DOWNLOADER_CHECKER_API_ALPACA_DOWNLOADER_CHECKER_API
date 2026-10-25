"""Conformance tests for the production-owned YAML correlation boundary."""

import ast
import copy
import inspect
import subprocess
import sys
import tempfile
import textwrap
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_end_of_day_security_correlation as correlation_fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_name_change_evidence import bars, inputs_for
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_lineage_build import context as lineage_context
from libs.alpaca_lineage_build import pipeline, policy_engine, storage, transform
from libs.alpaca_lineage_build.contract import (
    SUPPORTED_VERSIONS,
    VERSION,
    CorrelationPolicyError,
    LineageError,
    versions,
)
from libs.alpaca_lineage_build.correlation_policy import (
    CANONICAL_POLICY_PATH,
    PRODUCTION_EXECUTION_PROFILE,
    PRODUCTION_POLICY_SEMANTIC_SHA256,
    authorize_production_policy,
    expected_policy_identity,
    load_authorized_policy,
    load_policy,
)
from libs.alpaca_lineage_build.migration import (
    _locked_sources_for_migration,
    can_promote_observation_decision,
    migrate_correlation_policy,
)
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.refresh import refresh_existing
from libs.alpaca_lineage_build.universe_update import run_update
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage import EXCHANGE_TEST_SYMBOL

ROOT = Path(__file__).resolve().parents[1]
V58_POLICY = ROOT / "resources/alpaca_security_correlation_v58_legacy_policy.yaml"
V59_POLICY = ROOT / "resources/alpaca_security_correlation_v59_legacy_policy.yaml"
CORRECTED_POLICY = ROOT / "resources/alpaca_security_correlation_evidence_quality_policy.yaml"
LINEAGE_PACKAGE = ROOT / "libs/alpaca_lineage_build"


class ProductionPolicyAuthorizationTests(TestCase):
    def test_canonical_policy_is_the_exact_current_authorized_identity(self):
        policy = load_authorized_policy()
        self.assertIs(policy, authorize_production_policy(policy))
        self.assertEqual(CANONICAL_POLICY_PATH.resolve(), Path(policy.source).resolve())
        self.assertEqual(PRODUCTION_EXECUTION_PROFILE, policy.execution_profile)
        self.assertEqual(PRODUCTION_POLICY_SEMANTIC_SHA256, policy.semantic_sha256)
        self.assertEqual(
            expected_policy_identity(), {key: policy.provenance()[key] for key in expected_policy_identity()}
        )
        self.assertEqual(64, len(policy.file_sha256))
        self.assertEqual(policy.semantic_sha256, versions()["correlation_policy_sha256"])

    def test_production_rejects_every_lab_profile_and_mutated_identity(self):
        canonical = load_authorized_policy()
        for path in (V58_POLICY, V59_POLICY, CORRECTED_POLICY):
            with self.subTest(path=path.name), self.assertRaises(CorrelationPolicyError):
                authorize_production_policy(load_policy(path))

        mutations = (
            {"policy_version": "999.0.0"},
            {"policy_role": "published_reference"},
            {"execution_profile": "correlation_v59_two_phase"},
            {"implementation_correlation_version": 59},
            {"implementation_lineage_version": 33},
            {"semantic_sha256": "f" * 64},
            {"file_sha256": "not-a-digest"},
            {"source": str(V59_POLICY)},
        )
        for values in mutations:
            with self.subTest(values=values), self.assertRaises(CorrelationPolicyError):
                authorize_production_policy(replace(canonical, **values))

        # File bytes are audit provenance, not executable meaning. A formatting-only
        # canonical edit remains authorized while publishing its new byte digest.
        reformatted = replace(canonical, file_sha256="f" * 64)
        self.assertIs(reformatted, authorize_production_policy(reformatted))

    def test_production_authorization_binds_the_exact_executable_profile(self):
        canonical = load_authorized_policy()
        corrected = load_policy(CORRECTED_POLICY)
        fake_row = replace(canonical.row_rules[0], apply=lambda _context: None)
        fake_ownership = replace(
            canonical.ownership_rules[0],
            apply=lambda _context, _intervals, _order, _name: None,
        )
        mutations = {
            "reordered row rules": {
                "row_rules": tuple(reversed(canonical.row_rules)),
            },
            "same-name fake row implementation": {
                "row_rules": (fake_row, *canonical.row_rules[1:]),
            },
            "reordered ownership rules": {
                "ownership_rules": tuple(reversed(canonical.ownership_rules)),
            },
            "same-name fake ownership implementation": {
                "ownership_rules": (fake_ownership, *canonical.ownership_rules[1:]),
            },
            "established ownership rules": {
                "established_ownership_rules": canonical.established_ownership_rules[1:],
            },
            "fixed-point bound": {"max_passes": canonical.max_passes + 1},
            "terminal rule": {"terminal_rule": "different_terminal"},
            "fallback profile": {"fallback_profile": corrected.fallback_profile},
            "reconciliation producer": {
                "reconciliation_producer": corrected.reconciliation_producer,
            },
            "ownership fallback producer": {
                "ownership_fallback_producer": corrected.ownership_fallback_producer,
            },
            "producer dependency": {"producer_dependency": corrected.producer_dependency},
            "corrected typed rules": {
                "evidence_fact_producers": corrected.evidence_fact_producers,
                "evidence_fact_types": corrected.evidence_fact_types,
                "evidence_rules": corrected.evidence_rules,
                "active_suffix_fact_producers": corrected.active_suffix_fact_producers,
                "active_suffix_fact_types": corrected.active_suffix_fact_types,
                "active_suffix_rules": corrected.active_suffix_rules,
            },
        }
        with (
            patch.object(policy_engine, "evaluate_policy") as evaluate,
            patch.object(storage, "read_index") as read_index,
        ):
            for label, values in mutations.items():
                with self.subTest(label=label), self.assertRaisesRegex(CorrelationPolicyError, "not authorized"):
                    authorize_production_policy(replace(canonical, **values))
        evaluate.assert_not_called()
        read_index.assert_not_called()

    def test_engine_has_no_implicit_policy_or_live_hardcoded_call_site(self):
        for function in (transform.correlate, policy_engine.evaluate_policy):
            parameter = inspect.signature(function).parameters["policy"]
            self.assertIs(inspect.Parameter.empty, parameter.default)

        with self.assertRaisesRegex(CorrelationPolicyError, "explicit compiled policy"):
            policy_engine.evaluate_policy(Path("/unused"), {}, {}, policy=None)

        calls = []
        for path in sorted(LINEAGE_PACKAGE.glob("*.py")):
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("alpaca_lineage_policy_lab", source, path.name)
            tree = ast.parse(source, filename=str(path))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "correlate"
                ):
                    calls.append((path.name, node.lineno))
                    self.assertIn("policy", {item.arg for item in node.keywords}, (path.name, node.lineno))
        self.assertTrue(calls, "expected at least one live correlation call site")

    def test_handoff_and_recovery_modules_do_not_import_the_yaml_compiler(self):
        for name in ("contract.py", "storage.py", "reader.py", "resume.py"):
            path = LINEAGE_PACKAGE / name
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imports = {
                node.module
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module is not None
            }
            imported_names = {
                alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
            }
            self.assertNotIn("correlation_policy", imports, name)
            self.assertNotIn("libs.alpaca_lineage_build.correlation_policy", imported_names, name)
            self.assertNotIn("yaml", imported_names, name)

    def test_lab_is_an_adapter_to_the_shared_core_and_keeps_all_profiles(self):
        from libs.alpaca_lineage_policy_lab import fallbacks as lab_engine
        from libs.alpaca_lineage_policy_lab.policy import load_policy as load_lab_policy

        self.assertIs(policy_engine, lab_engine)
        self.assertEqual("correlation_v58_single_phase", load_lab_policy(V58_POLICY).execution_profile)
        self.assertEqual("correlation_v59_two_phase", load_lab_policy(V59_POLICY).execution_profile)
        self.assertEqual("corrected_v59", load_lab_policy(CORRECTED_POLICY).fallback_profile)

    def test_current_policy_preserves_frozen_v59_assignment_semantics(self):
        evidence = correlation_fixtures.RowCorrelationTests._overlapping_asof_lineage()
        evidence["symbol_disposition"] = {"classification": EXCHANGE_TEST_SYMBOL}

        def evaluate(policy):
            reports = policy_engine.evaluate_policy(
                Path("/unused-policy-store"),
                inputs_for(
                    {"CURRENT": bars(["2024-01-03"])},
                    asofs={"CURRENT": "2026-10-15"},
                ),
                {"CURRENT": copy.deepcopy(evidence)},
                policy=policy,
            )
            return {
                symbol: {key: value for key, value in report.items() if key != "correlation_policy"}
                for symbol, report in reports.items()
            }

        current = load_authorized_policy()
        frozen = load_policy(V59_POLICY)
        self.assertEqual(frozen.semantic_sha256, current.semantic_sha256)
        self.assertEqual(evaluate(frozen), evaluate(current))


class ProductionPolicyOperationTests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )

    def seed_prices(self, *symbols):
        for symbol in symbols:
            download_pricing(
                self.store,
                symbol,
                "2026-10-14",
                client=FakeClient([bar("2026-10-01")]),
            )

    def seed_lineage(self, *symbols):
        self.seed_prices(*symbols)
        result = run_lineage(
            self.store,
            list(symbols),
            identity_client=IdentityClient({symbol: asset(symbol) for symbol in symbols}),
        )
        self.assertEqual(0, result["failed"], result)

    def interrupt_current_publication(self, marker):
        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        prior = storage.read_receipt(root, "AAA", index)
        receipt = {**prior, "update_obligation_id": marker}
        with (
            storage.pricing_locks(self.store, receipt["input_sources"]),
            patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted")),
            self.assertRaisesRegex(RuntimeError, "interrupted"),
        ):
            storage.publish(
                self.store,
                root,
                index,
                {"AAA": receipt},
                {"AAA": {}},
                {},
                previous={"AAA": prior},
            )
        self.assertTrue((root / ".pending.json").exists())

    def test_one_operation_loads_once_and_threads_one_policy_to_every_group(self):
        self.seed_prices("AAA", "BBB")
        seen = []
        real_correlate = pipeline.correlate

        def observe(*args, **kwargs):
            seen.append(kwargs["policy"])
            return real_correlate(*args, **kwargs)

        with (
            patch.object(
                sys.modules["libs.alpaca_lineage_build.correlation_policy"],
                "load_authorized_policy",
                wraps=load_authorized_policy,
            ) as load,
            patch.object(storage, "recover", wraps=storage.recover) as recover,
            patch.object(pipeline, "correlate", side_effect=observe),
        ):
            result = run_lineage(
                self.store,
                ["AAA", "BBB"],
                identity_client=IdentityClient({symbol: asset(symbol) for symbol in ("AAA", "BBB")}),
            )

        self.assertEqual(0, result["failed"], result)
        load.assert_called_once_with()
        recover.assert_called_once()
        self.assertEqual(2, len(seen))
        self.assertTrue(all(policy is seen[0] for policy in seen))

        expected = seen[0].provenance()
        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        for symbol in ("AAA", "BBB"):
            with self.subTest(symbol=symbol):
                entry = index["symbols"][symbol]
                receipt = storage.read_receipt(root, symbol, index)
                self.assertEqual(expected, receipt["correlation_policy"])
                self.assertEqual(expected, entry["correlation_policy"])
                self.assertEqual(
                    expected["semantic_sha256"],
                    receipt["versions"]["correlation_policy_sha256"],
                )

    def test_injected_candidate_fails_after_recovery_and_before_planning_or_network(self):
        self.seed_prices("AAA")
        context = lineage_context.OperationContext(self.store)
        context._correlation_policy = load_policy(CORRECTED_POLICY)
        identity = IdentityClient({})

        with (
            patch.object(storage, "read_index", side_effect=AssertionError("planned from lineage index")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("evaluated correlation")),
            self.assertRaisesRegex(CorrelationPolicyError, "not authorized"),
        ):
            run_lineage(self.store, [], identity_client=identity, context=context)

        self.assertTrue(context._lineage_recovery_complete)
        self.assertEqual(0, identity.request_count)

    def test_direct_refresh_rejects_candidate_before_summary_or_rest(self):
        self.seed_prices("AAA")
        context = lineage_context.OperationContext(self.store)
        context._correlation_policy = load_policy(CORRECTED_POLICY)
        identity = IdentityClient({"AAA": asset("AAA")})
        pricing = SimpleNamespace(_get=lambda *_args, **_kwargs: self.fail("REST request"))

        with (
            patch.object(context, "select_summary", side_effect=AssertionError("selected summary")),
            patch(
                "libs.alpaca_lineage_build.refresh.verify",
                side_effect=AssertionError("ran live REST check"),
            ),
            self.assertRaisesRegex(CorrelationPolicyError, "not authorized"),
        ):
            refresh_existing(
                self.store,
                pricing_client=pricing,
                identity_client=identity,
                context=context,
            )
        self.assertEqual(0, identity.request_count)

    def test_daily_path_recovers_before_policy_then_stops_before_planning_or_rest(self):
        self.seed_prices("AAA")
        context = lineage_context.OperationContext(self.store)
        context._correlation_policy = load_policy(CORRECTED_POLICY)
        identity = IdentityClient({"AAA": asset("AAA")})
        pricing = SimpleNamespace(request_count=0)
        events = []

        def read_state(*_args, **_kwargs):
            events.append("state")
            return {"schema_version": 1, "work": {}, "registration_pending": []}

        def recover_serving(*, recover=False, **_kwargs):
            self.assertTrue(recover)
            events.append("serving_recovery")

        def recovered(*_args, **_kwargs):
            events.append("publication_recovery")
            return {"pricing_recovered": [], "lineage_recovered": False, "failed": 0}

        real_accept = context.accept_correlation_policy

        def authorize():
            events.append("authorize")
            return real_accept()

        with (
            patch("libs.alpaca_lineage_build.universe_update.accept_context", return_value=context),
            patch(
                "libs.alpaca_lineage_build.universe_update.universe_state.update_lock",
                return_value=nullcontext(),
            ),
            patch(
                "libs.alpaca_lineage_build.universe_update.universe_state.read_state",
                side_effect=read_state,
            ),
            patch.object(context, "select_serving", side_effect=recover_serving),
            patch(
                "libs.alpaca_lineage_build.universe_update.recover_publications",
                side_effect=recovered,
            ),
            patch.object(context, "accept_correlation_policy", side_effect=authorize),
            patch(
                "libs.alpaca_lineage_build.universe_update.storage.writer_lock",
                side_effect=AssertionError("planned durable work"),
            ),
            patch(
                "libs.alpaca_lineage_build.universe_update.observe_catalogue",
                side_effect=AssertionError("made catalogue REST request"),
            ),
            self.assertRaisesRegex(CorrelationPolicyError, "not authorized"),
        ):
            run_update(
                self.store,
                "2026-10-14",
                pricing_client=pricing,
                identity_client=identity,
            )

        self.assertEqual(
            ["state", "serving_recovery", "publication_recovery", "authorize"],
            events,
        )
        self.assertEqual(0, identity.request_count)
        self.assertEqual(0, pricing.request_count)

    def test_frozen_journal_recovers_before_missing_malformed_or_mismatched_current_policy(self):
        self.seed_lineage("AAA")
        root = self.store / "end_of_day/lineage"

        for label in ("missing", "malformed", "mismatched"):
            with self.subTest(label=label):
                marker = f"recovered-before-{label}-policy"
                self.interrupt_current_publication(marker)
                context = lineage_context.OperationContext(self.store)
                identity = IdentityClient({"AAA": asset("AAA")})
                with (
                    patch.object(
                        sys.modules["libs.alpaca_lineage_build.correlation_policy"],
                        "load_authorized_policy",
                        side_effect=CorrelationPolicyError(label),
                    ),
                    patch.object(
                        storage,
                        "read_index",
                        side_effect=AssertionError("planned from the recovered selector"),
                    ),
                    patch.object(pipeline, "read_checks", side_effect=AssertionError("planned checks")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("evaluated correlation")),
                    self.assertRaisesRegex(CorrelationPolicyError, label),
                ):
                    run_lineage(self.store, ["AAA"], identity_client=identity, context=context)

                self.assertFalse((root / ".pending.json").exists())
                self.assertTrue(context._lineage_recovery_complete)
                self.assertEqual(0, identity.request_count)
                index = storage.read_index(root)
                receipt = storage.read_receipt(root, "AAA", index)
                self.assertEqual(marker, receipt["update_obligation_id"])

    def test_frozen_recovery_and_reader_work_without_yaml_compiler_then_new_work_fails(self):
        marker = "stdlib-only-frozen-recovery"
        self.seed_lineage("AAA")
        self.interrupt_current_publication(marker)
        code = textwrap.dedent(
            f"""
            import builtins
            import sys
            from pathlib import Path

            sys.path.insert(0, {str(ROOT)!r})
            real_import = builtins.__import__

            def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
                if name in {{"yaml", "correlation_policy"}} or name.endswith(".correlation_policy"):
                    raise ImportError("policy compiler intentionally unavailable")
                return real_import(name, globals, locals, fromlist, level)

            builtins.__import__ = blocked_import
            from libs.alpaca_lineage_build import contract, reader, storage
            from libs.alpaca_lineage_build.context import (
                OperationContext,
                recover_and_accept_correlation_policy,
            )
            from libs.alpaca_lineage_build import resume

            store = Path({str(self.store)!r})
            root = store / "end_of_day/lineage"
            context = OperationContext(store)
            try:
                recover_and_accept_correlation_policy(store, context)
            except ImportError as error:
                assert "intentionally unavailable" in str(error)
            else:
                raise AssertionError("new work accepted without the policy compiler")

            assert context._lineage_recovery_complete
            assert not (root / ".pending.json").exists()
            index = storage.read_index(root)
            receipt = storage.read_receipt(root, "AAA", index)
            assert receipt["update_obligation_id"] == {marker!r}
            assert contract.reusable_decision(receipt)
            assert reader.read_lineage(store, "AAA")["receipt"] == receipt
            assert callable(resume.can_resume)
            """
        )
        completed = subprocess.run(
            [sys.executable, "-I", "-B", "-c", code],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)


class PolicyRecoveryAndMigrationTests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )

    @staticmethod
    def legacy_versions():
        value = {**versions(), "lineage": 33, "correlation": 59}
        value.pop("correlation_policy_sha256")
        return value

    def group_client(self):
        first = asset("AAA")
        second = {**asset("BBB"), "status": "inactive", "tradable": False}
        action = {
            "action_type": "stock_merger",
            "id": "group-merger",
            "process_date": "2026-09-01",
            "effective_date": "2026-09-01",
            "acquirer_symbol": "AAA",
            "acquiree_symbol": "BBB",
            "acquirer_cusip": "111111111",
            "acquiree_cusip": "222222222",
            "old_rate": 1,
            "new_rate": 1,
        }
        return IdentityClient(
            {
                "AAA": first,
                "BBB": second,
                "111111111": first,
                "222222222": second,
            },
            [action],
        )

    def seed_group(self):
        for symbol in ("AAA", "BBB"):
            download_pricing(
                self.store,
                symbol,
                "2026-10-14",
                client=FakeClient([bar("2026-10-01")]),
            )
        result = run_lineage(self.store, ["AAA"], identity_client=self.group_client())
        self.assertEqual(0, result["failed"], result)
        index = storage.read_index(self.root)
        self.assertEqual({"AAA", "BBB"}, set(index["symbols"]["AAA"]["group_members"]))

    def complete_empty_checks(self):
        from libs.alpaca_pricing_download.update_checks import (
            begin_checks,
            complete_checks,
            read_checks,
        )

        checks = read_checks(self.store)
        if checks["epoch"] is None:
            checks = begin_checks(self.store)
        self.assertEqual({}, checks["checks"])
        complete_checks(self.store, checks["epoch"])
        self.assertTrue(read_checks(self.store)["complete"])

    def install_legacy_group(self, mutate=None):
        index = storage.read_index(self.root)
        receipts = {
            symbol: copy.deepcopy(storage.read_receipt(self.root, symbol, index)) for symbol in ("AAA", "BBB")
        }
        legacy = self.legacy_versions()
        for symbol, receipt in receipts.items():
            receipt.update(schema_version=33, versions=copy.deepcopy(legacy))
            receipt.pop("correlation_policy")
            if mutate is not None:
                mutate(symbol, receipt)
        updates = {}
        for symbol, receipt in receipts.items():
            entry = copy.deepcopy(index["symbols"][symbol])
            entry.update(sha256=storage.document_digest(receipt), versions=copy.deepcopy(receipt["versions"]))
            entry.pop("correlation_policy")
            updates[symbol] = entry
        journal = {
            "schema_version": 33,
            "boundary": "alpaca_lineage_transaction",
            "documents": {f"{symbol}/lineage.manifest.json": receipt for symbol, receipt in receipts.items()},
            "retire": [],
            "index_updates": updates,
            "sources": {symbol: receipt["source_sha256"] for symbol, receipt in receipts.items()},
            "unavailable_pricing": [],
            "notices": {},
        }
        with storage.publication_lock(self.root, exclusive=True):
            storage._install(self.root, journal, compare=False)
        return receipts

    def test_frozen_journal_policy_consistency_and_legacy_recovery_compatibility(self):
        policy = load_authorized_policy().provenance()
        current = {
            "schema_version": VERSION,
            "correlation_policy": policy,
            "index_updates": {"AAA": {"correlation_policy": policy}},
            "documents": {"AAA/lineage.manifest.json": {"correlation_policy": policy}},
        }
        storage._validate_journal_policy(current)
        for target in ("journal", "index", "receipt"):
            with self.subTest(target=target):
                changed = copy.deepcopy(current)
                altered = {**policy, "file_sha256": "f" * 64}
                if target == "journal":
                    changed["correlation_policy"] = altered
                elif target == "index":
                    changed["index_updates"]["AAA"]["correlation_policy"] = altered
                else:
                    changed["documents"]["AAA/lineage.manifest.json"]["correlation_policy"] = altered
                with self.assertRaisesRegex(LineageError, "policy provenance"):
                    storage._validate_journal_policy(changed)

        for schema_version in SUPPORTED_VERSIONS:
            if schema_version >= VERSION:
                continue
            with self.subTest(schema_version=schema_version):
                legacy = {
                    "schema_version": schema_version,
                    "index_updates": {"AAA": {}},
                    "documents": {"AAA/lineage.manifest.json": {}},
                }
                storage._validate_journal_policy(legacy)
                legacy["correlation_policy"] = policy
                with self.assertRaisesRegex(LineageError, "legacy.*policy provenance"):
                    storage._validate_journal_policy(legacy)

    def test_only_exact_v33_v59_receipt_is_promotable_under_current_policy(self):
        policy = load_authorized_policy()
        receipt = {
            "schema_version": 33,
            "versions": self.legacy_versions(),
        }
        self.assertTrue(can_promote_observation_decision(receipt, policy))
        for changed in (
            {**receipt, "schema_version": 32},
            {**receipt, "versions": {**receipt["versions"], "correlation": 58}},
            {**receipt, "correlation_policy": policy.provenance()},
        ):
            with self.subTest(changed=changed):
                self.assertFalse(can_promote_observation_decision(changed, policy))
        self.assertFalse(can_promote_observation_decision(receipt, load_policy(V59_POLICY)))

    def test_exact_legacy_complete_group_promotes_without_prices_rules_or_network(self):
        self.seed_group()
        before = self.install_legacy_group()
        pricing = snapshot(self.store / "end_of_day/pricing")
        client = self.group_client()
        client.request_count = 0
        client.calls.clear()
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("decoded prices")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("replayed rules")),
        ):
            result = run_lineage(self.store, ["AAA"], identity_client=client)
        self.assertEqual(0, result["failed"], result)
        self.assertTrue(result["results"][0]["reused_decision"])
        self.assertEqual(0, client.request_count)
        self.assertEqual(pricing, snapshot(self.store / "end_of_day/pricing"))

        policy = load_authorized_policy().provenance()
        index = storage.read_index(self.root)
        for symbol in ("AAA", "BBB"):
            with self.subTest(symbol=symbol):
                receipt = storage.read_receipt(self.root, symbol, index)
                self.assertEqual(VERSION, receipt["schema_version"])
                self.assertEqual(versions(), receipt["versions"])
                self.assertEqual(policy, receipt["correlation_policy"])
                self.assertEqual(before[symbol]["boundaries"], receipt["boundaries"])

    def test_one_ineligible_member_forces_whole_group_recalculation_without_partial_promotion(self):
        self.seed_group()

        def invalidate(symbol, receipt):
            if symbol == "BBB":
                receipt["versions"]["correlation"] = 58

        before = self.install_legacy_group(invalidate)
        client = self.group_client()
        client.request_count = 0
        client.calls.clear()
        with patch.object(
            pipeline, "correlate", side_effect=RuntimeError("stop before publication")
        ) as correlate:
            result = run_lineage(self.store, ["AAA"], identity_client=client)
        self.assertEqual(1, result["failed"], result)
        correlate.assert_called_once()
        self.assertEqual({"AAA", "BBB"}, set(correlate.call_args.args[1]))
        self.assertEqual(0, client.request_count)

        index = storage.read_index(self.root)
        for symbol in ("AAA", "BBB"):
            with self.subTest(symbol=symbol):
                receipt = storage.read_receipt(self.root, symbol, index)
                self.assertEqual(before[symbol]["versions"], receipt["versions"])
                self.assertNotIn("correlation_policy", receipt)

    def test_dedicated_migration_changes_only_provenance_and_queues_serving_work(self):
        from libs.alpaca_serving_catalogue import work as serving_work

        self.seed_group()
        legacy_receipts = self.install_legacy_group()
        self.complete_empty_checks()
        serving_work.acknowledge(self.store, serving_work.read(self.store))
        self.assertEqual({}, serving_work.read(self.store))
        pricing_before = snapshot(self.store / "end_of_day/pricing")
        progress = []

        with (
            patch.object(
                policy_engine,
                "evaluate_policy",
                side_effect=AssertionError("executed correlation policy"),
            ),
            patch.object(
                storage,
                "read_boundaries",
                side_effect=AssertionError("opened assignment boundary"),
            ),
            patch.object(
                pipeline,
                "run_lineage",
                side_effect=AssertionError("entered live lineage pipeline"),
            ),
            patch(
                "libs.alpaca_lineage_build.observations.select_observations",
                side_effect=AssertionError("selected observations"),
            ),
        ):
            result = migrate_correlation_policy(
                self.store,
                progress=lambda done, total: progress.append((done, total)),
            )

        self.assertTrue(result["complete"], result)
        self.assertEqual(1, result["selected_groups"])
        self.assertEqual(2, result["selected_receipts"])
        self.assertEqual(1, result["promoted_groups"])
        self.assertEqual(2, result["promoted_receipts"])
        self.assertEqual(0, result["already_current_groups"])
        self.assertEqual(0, result["failed_groups"])
        self.assertEqual("pending_update", result["serving_catalogue_action"])
        self.assertEqual(0, result["provider_requests"])
        self.assertEqual(0, result["identity_rule_executions"])
        self.assertEqual(0, result["pricing_payload_reads"])
        self.assertEqual(0, result["pricing_writes"])
        self.assertEqual([(1, 1)], progress)
        self.assertEqual(pricing_before, snapshot(self.store / "end_of_day/pricing"))

        policy = load_authorized_policy().provenance()
        index = storage.read_index(self.root)
        for symbol in ("AAA", "BBB"):
            with self.subTest(symbol=symbol):
                receipt = storage.read_receipt(self.root, symbol, index)
                expected = {
                    **legacy_receipts[symbol],
                    "schema_version": VERSION,
                    "versions": versions(),
                    "correlation_policy": policy,
                }
                self.assertEqual(expected, receipt)
        pending = serving_work.read(self.store)
        self.assertEqual(
            {symbol: index["symbols"][symbol]["sha256"] for symbol in ("AAA", "BBB")},
            pending,
        )

        selector = copy.deepcopy(index["projection_state"])
        repeated = migrate_correlation_policy(self.store)
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(0, repeated["promoted_groups"])
        self.assertEqual(1, repeated["already_current_groups"])
        self.assertEqual("pending_update", repeated["serving_catalogue_action"])
        self.assertEqual(selector, storage.read_index(self.root)["projection_state"])
        self.assertEqual(pending, serving_work.read(self.store))

        serving_work.acknowledge(self.store, pending)
        completed = migrate_correlation_policy(self.store)
        self.assertEqual("none", completed["serving_catalogue_action"])

        from libs.alpaca_serving_catalogue.contract import CatalogueError

        with patch.object(
            serving_work,
            "read",
            side_effect=CatalogueError("serving work coverage is absent"),
        ):
            bootstrap = migrate_correlation_policy(self.store)
        self.assertTrue(bootstrap["complete"])
        self.assertEqual("bootstrap_required", bootstrap["serving_catalogue_action"])
        self.assertIsNone(bootstrap["serving_pending_symbols"])

    def test_dedicated_migration_cli_branches_before_credentials_or_clients(self):
        from libs.alpaca_cli import _store_operation_mode, build_parser, dispatch

        args = build_parser().parse_args(["lineage", "migrate-policy", "--store", str(self.store)])
        expected = {
            "complete": True,
            "failed": 0,
            "promoted_groups": 0,
        }
        with (
            patch("libs.alpaca_cli.clients", side_effect=AssertionError("constructed clients")) as clients,
            patch(
                "libs.alpaca_lineage_build.migration.migrate_correlation_policy",
                return_value=expected,
            ) as migrate,
        ):
            self.assertEqual(expected, dispatch(args))

        self.assertEqual("exclusive", _store_operation_mode(args))
        clients.assert_not_called()
        migrate.assert_called_once()
        self.assertEqual(self.store, migrate.call_args.args[0])

    def test_dedicated_migration_rejects_mixed_or_malformed_groups_before_source_work(self):
        self.seed_group()
        self.install_legacy_group()
        self.complete_empty_checks()
        index = storage.read_index(self.root)
        mixed = copy.deepcopy(index["symbols"]["AAA"])
        mixed["versions"] = versions()
        mixed["correlation_policy"] = load_authorized_policy().provenance()
        with storage.publication_lock(self.root, exclusive=True):
            storage._update_index(self.root, {"AAA": mixed})
        before = storage.read_index(self.root)

        with (
            patch.object(
                storage,
                "read_receipt",
                side_effect=AssertionError("opened mixed-group receipt"),
            ) as read_receipt,
            patch(
                "libs.alpaca_pricing_download.storage.load_source",
                side_effect=AssertionError("opened pricing source"),
            ) as load_source,
        ):
            result = migrate_correlation_policy(self.store)
        self.assertFalse(result["complete"])
        self.assertEqual(1, result["failed_groups"])
        self.assertEqual(0, result["promoted_groups"])
        read_receipt.assert_not_called()
        load_source.assert_not_called()
        self.assertEqual(before, storage.read_index(self.root))

        malformed = copy.deepcopy(before["symbols"]["BBB"])
        malformed["group_members"] = ["BBB"]
        with storage.publication_lock(self.root, exclusive=True):
            storage._update_index(self.root, {"BBB": malformed})
        malformed_before = storage.read_index(self.root)
        with (
            patch.object(
                storage,
                "read_receipt",
                side_effect=AssertionError("opened malformed-group receipt"),
            ),
            patch(
                "libs.alpaca_pricing_download.storage.load_source",
                side_effect=AssertionError("opened malformed-group source"),
            ),
            self.assertRaisesRegex(LineageError, "group members disagree"),
        ):
            migrate_correlation_policy(self.store)
        self.assertEqual(malformed_before, storage.read_index(self.root))

    def test_dedicated_migration_requires_empty_checks_before_selection_or_publication(self):
        from libs.alpaca_pricing_download.update_checks import queue_check

        self.seed_group()
        self.install_legacy_group()
        self.complete_empty_checks()
        queue_check(self.store, "AAA")
        before = storage.read_index(self.root)
        with (
            patch.object(
                storage,
                "read_index",
                side_effect=AssertionError("selected lineage with pending checks"),
            ) as read_index,
            patch.object(
                storage,
                "publish",
                side_effect=AssertionError("published lineage with pending checks"),
            ) as publish,
            self.assertRaisesRegex(LineageError, "completed empty lineage update-check queue"),
        ):
            migrate_correlation_policy(self.store)
        read_index.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(before, storage.read_index(self.root))

    def test_dedicated_migration_rejects_notice_source_change_and_unavailable_appearance(self):
        from libs.alpaca_pricing_download import storage as pricing_storage

        self.seed_group()
        self.install_legacy_group()
        self.complete_empty_checks()
        before = storage.read_index(self.root)

        with (
            patch.object(
                pricing_storage,
                "read_update",
                return_value={"pending": "newer pricing"},
            ),
            patch.object(
                pricing_storage,
                "load_source",
                side_effect=AssertionError("opened source after pending notice"),
            ) as load_source,
            patch.object(
                storage,
                "publish",
                side_effect=AssertionError("published after pending notice"),
            ) as publish,
        ):
            notice = migrate_correlation_policy(self.store)
        self.assertFalse(notice["complete"])
        self.assertEqual(1, notice["failed_groups"])
        self.assertIn("pricing update notice is pending", notice["failure_examples"][0]["reason"])
        load_source.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(before, storage.read_index(self.root))

        real_sources = {
            symbol: pricing_storage.load_source(
                self.store / "end_of_day/pricing" / symbol,
                symbol,
            )
            for symbol in ("AAA", "BBB")
        }
        changed_source = {**real_sources["AAA"], "changed_after_lineage": True}
        with (
            patch.object(pricing_storage, "read_update", return_value=None),
            patch.object(
                pricing_storage,
                "load_source",
                side_effect=lambda _root, symbol: changed_source if symbol == "AAA" else real_sources[symbol],
            ),
            patch.object(
                storage,
                "publish",
                side_effect=AssertionError("published changed source"),
            ) as publish,
        ):
            changed = migrate_correlation_policy(self.store)
        self.assertFalse(changed["complete"])
        self.assertEqual(1, changed["failed_groups"])
        self.assertIn("selected pricing generation changed", changed["failure_examples"][0]["reason"])
        publish.assert_not_called()
        self.assertEqual(before, storage.read_index(self.root))

        available_source = {"symbol": "AAA", "generation": "selected"}
        generation = storage.document_digest(available_source)
        unavailable = {"MISSING": {"reason": "not requestable"}}
        with (
            patch.object(pricing_storage, "read_update", return_value=None),
            patch.object(
                pricing_storage,
                "load_source",
                side_effect=(available_source, None),
            ),
        ):
            selected = _locked_sources_for_migration(
                self.store,
                ("AAA", "MISSING"),
                {"AAA": generation},
                unavailable,
                {"AAA", "MISSING"},
            )
        self.assertEqual({"AAA": available_source}, selected)

        with (
            patch.object(pricing_storage, "read_update", return_value=None),
            patch.object(
                pricing_storage,
                "load_source",
                side_effect=(available_source, {"symbol": "MISSING"}),
            ),
            self.assertRaisesRegex(LineageError, "selected pricing availability changed"),
        ):
            _locked_sources_for_migration(
                self.store,
                ("AAA", "MISSING"),
                {"AAA": generation},
                unavailable,
                {"AAA", "MISSING"},
            )

    def test_dedicated_migration_interruption_recovers_frozen_group_then_is_idempotent(self):
        self.seed_group()
        self.install_legacy_group()
        self.complete_empty_checks()
        with (
            patch.object(
                storage,
                "finish_publication",
                side_effect=RuntimeError("interrupted policy promotion"),
            ),
            self.assertRaisesRegex(RuntimeError, "interrupted policy promotion"),
        ):
            migrate_correlation_policy(self.store)
        self.assertTrue((self.root / ".pending.json").exists())

        with (
            patch.object(
                policy_engine,
                "evaluate_policy",
                side_effect=AssertionError("replayed policy after recovery"),
            ),
            patch.object(
                storage,
                "read_boundaries",
                side_effect=AssertionError("opened boundaries after recovery"),
            ),
        ):
            recovered = migrate_correlation_policy(self.store)
        self.assertTrue(recovered["complete"], recovered)
        self.assertTrue(recovered["recovered_publication"])
        self.assertEqual(0, recovered["promoted_groups"])
        self.assertEqual(1, recovered["already_current_groups"])
        self.assertFalse((self.root / ".pending.json").exists())
        index = storage.read_index(self.root)
        self.assertEqual(
            {versions()["correlation"]},
            {index["symbols"][symbol]["versions"]["correlation"] for symbol in ("AAA", "BBB")},
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
