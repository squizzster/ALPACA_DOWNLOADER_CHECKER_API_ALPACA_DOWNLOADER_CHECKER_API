"""Bounded recovery replaces selected defective decisions without upstream audits."""

import json
import unittest
from collections import Counter
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import test_alpaca_lineage_continuation as continuation_fixture
from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from libs import alpaca_cli
from libs.alpaca_lineage_build import pipeline, pricing, storage, universe_update
from libs.alpaca_lineage_build.retained_recovery import RECOVERY_BOUNDARY, load_selection, recover
from libs.alpaca_pricing_download import handoff
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue.producer import maintain


class RetainedOwnershipRecoveryTests(unittest.TestCase):
    def selection(self, store, groups):
        root = store / "end_of_day/lineage"
        index = storage.read_index(root)
        value = {
            "schema_version": 1,
            "boundary": RECOVERY_BOUNDARY,
            "store": str(store.absolute()),
            "correlation_policy_sha256": next(iter(index["symbols"].values()))["correlation_policy"][
                "semantic_sha256"
            ],
            "groups": [
                {
                    "members": sorted(members),
                    "pricing_generations": {
                        name: index["symbols"][name]["source_sha256"]
                        for name in members
                        if name in index["symbols"]
                    },
                    "receipt_generations": {
                        name: index["symbols"][name]["sha256"] for name in members if name in index["symbols"]
                    },
                }
                for members in groups
            ],
        }
        path = store.parent / "selection.json"
        path.write_text(json.dumps(value))
        return path

    @contextmanager
    def no_upstream_work(self):
        with ExitStack() as stack:
            for owner, name in (
                (pipeline, "download_pricing"),
                (pipeline, "acquire"),
                (pipeline, "acquire_unpriced"),
                (pipeline, "_source_generation_changes"),
                (pipeline, "local_symbols"),
                (pricing_storage, "publish"),
                (pricing_storage, "read_validator"),
                (universe_update, "observe_catalogue"),
            ):
                stack.enter_context(patch.object(owner, name, side_effect=AssertionError(name)))
            yield

    def test_offline_cli_plan_uses_only_the_compact_selector(self):
        with fixture() as case:
            path = self.selection(case.store, [("AAA",)])
            args = alpaca_cli.build_parser().parse_args(
                ["lineage", "recover", "--selection", str(path), "--store", str(case.store), "--plan"]
            )
            self.assertIsNone(alpaca_cli._store_operation_mode(args))
            with (
                self.no_upstream_work(),
                patch.object(alpaca_cli, "clients", side_effect=AssertionError("provider clients")),
                patch.object(storage, "read_receipt", side_effect=AssertionError("receipt read")),
                patch.object(pipeline, "_current_sources", side_effect=AssertionError("source read")),
                patch.object(storage, "publish", side_effect=AssertionError("published")),
            ):
                result = alpaca_cli.dispatch(args)
            self.assertEqual((result["pending_groups"], result["completed_groups"]), (1, 0))
            self.assertEqual(result["provider_requests"], 0)
            self.assertEqual(result["symbols"], ["AAA"])
            args.plan = False
            self.assertEqual(alpaca_cli._store_operation_mode(args), "exclusive")

    def test_current_revision_is_recomputed_once_and_retry_skips_bodies(self):
        with fixture() as case:
            path = self.selection(case.store, [("AAA",)])
            with (
                self.no_upstream_work(),
                patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate,
                patch.object(pricing, "load_published_boundary", wraps=pricing.load_published_boundary) as reads,
            ):
                result = recover(case.store, path)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["recomputed_groups"], 1)
            self.assertEqual(correlate.call_count, 1)
            self.assertEqual(
                Counter(call.args[2] for call in reads.call_args_list),
                {
                    "completed_years": 1,
                    "current_year": 1,
                    "current_month": 1,
                },
            )
            with (
                self.no_upstream_work(),
                patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reread")),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed prices reread")),
                patch.object(
                    pipeline, "correlate", side_effect=AssertionError("completed calculation repeated")
                ),
            ):
                repeated = recover(case.store, path)
            self.assertTrue(repeated["complete"], repeated)
            self.assertEqual(repeated["previously_completed_groups"], 1)
            self.assertEqual(repeated["recomputed_groups"], 0)

    def test_changed_bindings_fail_before_receipt_or_payload_reads(self):
        for change in ("receipt", "source", "members", "policy", "locked_source"):
            with self.subTest(change=change), fixture() as case:
                path = self.selection(case.store, [("AAA",)])
                index = storage.read_index(case.store / "end_of_day/lineage")
                entry = index["symbols"]["AAA"]
                if change == "receipt":
                    entry["sha256"] = "a" * 64
                elif change == "source":
                    entry["source_sha256"] = "a" * 64
                elif change == "members":
                    entry["group_members"] = ["AAA", "BBB"]
                elif change == "policy":
                    data = json.loads(path.read_text())
                    data["correlation_policy_sha256"] = "a" * 64
                    path.write_text(json.dumps(data))
                source_reader = pipeline._current_sources

                def changed_source(*args, **kwargs):
                    sources = source_reader(*args, **kwargs)
                    sources["AAA"]["latest_request"]["asof"] = "2026-10-01"
                    return sources

                with (
                    self.no_upstream_work(),
                    patch.object(storage, "refresh_index", return_value=index),
                    patch.object(pipeline, "_current_sources", side_effect=changed_source)
                    if change == "locked_source"
                    else ExitStack(),
                    patch.object(storage, "read_receipt", side_effect=AssertionError("stale receipt read")),
                    patch.object(pipeline, "load_pricing", side_effect=AssertionError("stale payload read")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("stale inputs correlated")),
                ):
                    if change == "locked_source":
                        result = recover(case.store, path)
                        self.assertEqual(result["failed"], 1)
                        self.assertIn("locked recovery inputs changed", result["results"][0]["error"])
                    else:
                        with self.assertRaisesRegex(RuntimeError, "changed"):
                            recover(case.store, path)

    def test_journal_recovery_before_and_after_install_never_repeats_calculation(self):
        class Interrupted(BaseException):
            pass

        for after in (False, True):
            with self.subTest(after_install=after), fixture() as case:
                path = self.selection(case.store, [("AAA",)])
                install = storage._install

                def interrupt(*args, **kwargs):
                    if after:
                        install(*args, **kwargs)
                    raise Interrupted()

                with self.no_upstream_work(), patch.object(storage, "_install", side_effect=interrupt):
                    with self.assertRaises(Interrupted):
                        recover(case.store, path)
                with (
                    self.no_upstream_work(),
                    patch.object(
                        pipeline, "load_pricing", side_effect=AssertionError("journal reloaded prices")
                    ),
                    patch.object(
                        pipeline, "correlate", side_effect=AssertionError("journal repeated correlation")
                    ),
                ):
                    result = recover(case.store, path)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["previously_completed_groups"], 1)

    def test_peer_failure_preserves_successful_group_on_retry(self):
        with fixture() as case:
            download_pricing(case.store, "BBB", "2026-10-17", client=FakeClient(case.old))
            pipeline.run_lineage(case.store, ["BBB"], identity_client=IdentityClient({"BBB": asset("BBB")}))
            self.assertTrue(maintain(case.store)["complete"])
            path = self.selection(case.store, [("AAA",), ("BBB",)])
            correlate = pipeline.correlate

            def fail_peer(store, inputs, evidence, **kwargs):
                if "BBB" in inputs:
                    raise RuntimeError("peer failure")
                return correlate(store, inputs, evidence, **kwargs)

            with self.no_upstream_work(), patch.object(pipeline, "correlate", side_effect=fail_peer):
                failed = recover(case.store, path)
            self.assertEqual(failed["failed"], 1)
            load = pipeline.load_pricing

            def unfinished_only(store, name, **kwargs):
                self.assertEqual(name, "BBB")
                return load(store, name, **kwargs)

            with (
                self.no_upstream_work(),
                patch.object(pipeline, "load_pricing", side_effect=unfinished_only),
                patch.object(pipeline, "correlate", wraps=correlate) as evaluated,
            ):
                result = recover(case.store, path)
            self.assertTrue(result["complete"], result)
            self.assertEqual(evaluated.call_count, 1)
            self.assertEqual(result["previously_completed_groups"], 1)

    def test_partial_materialization_fails_before_correlation_or_publication(self):
        with fixture() as case:
            path = self.selection(case.store, [("AAA",)])
            load = pipeline.load_pricing

            def partial(*args, **kwargs):
                view = load(*args, **kwargs)
                view.materialization_scope = "retained_suffix"
                return view

            with (
                self.no_upstream_work(),
                patch.object(pipeline, "load_pricing", side_effect=partial),
                patch.object(pipeline, "correlate", side_effect=AssertionError("partial input correlated")),
                patch.object(storage, "publish", side_effect=AssertionError("partial input published")),
            ):
                result = recover(case.store, path)
            self.assertEqual(result["failed"], 1)
            self.assertIn("requires complete pricing history", result["results"][0]["error"])

    def test_accepted_empty_source_does_not_reopen_its_validator_for_summary_refresh(self):
        with fixture() as case:
            download_pricing(case.store, "EMPTY", "2026-10-17", client=FakeClient([]))
            pipeline.run_lineage(
                case.store, ["EMPTY"], identity_client=IdentityClient({"EMPTY": asset("EMPTY")})
            )
            self.assertTrue(maintain(case.store)["complete"])
            path = self.selection(case.store, [("EMPTY",)])
            with self.no_upstream_work():
                result = recover(case.store, path)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["results"][0]["pricing_rows_processed"], 0)

    def test_unknown_selection_version_store_and_overlapping_groups_fail_closed(self):
        for change in ("version", "store", "duplicate"):
            with self.subTest(change=change), fixture() as case:
                path = self.selection(case.store, [("AAA",)])
                value = json.loads(path.read_text())
                if change == "version":
                    value["schema_version"] = 2
                elif change == "store":
                    value["store"] += "-other"
                else:
                    value["groups"].append(value["groups"][0])
                path.write_text(json.dumps(value))
                with self.no_upstream_work(), self.assertRaisesRegex(RuntimeError, "invalid"):
                    recover(case.store, path)

    def test_full_group_recovery_uses_real_name_change_donor_and_restores_all_views(self):
        case = continuation_fixture.LineageContinuationTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        names, client = case.prepare_asof_group_evidence_change()
        correlate = pipeline.correlate

        def shortened(store, inputs, evidence, **kwargs):
            reports = correlate(store, inputs, evidence, **kwargs)
            report = reports["CURRENT"]
            segments = report["correlated"]["segments"]
            report["uncorrelated"]["segments"].extend(
                {
                    **row,
                    "canonical_security": None,
                    "security_identity": None,
                    "reason_codes": ["outside_supported_name_change_history"],
                }
                for row in segments
            )
            report["uncorrelated"]["row_count"] += report["correlated"]["row_count"]
            report["correlated"] = {"segments": [], "row_count": 0}
            return reports

        with patch.object(pipeline, "correlate", side_effect=shortened):
            case.case.run_build(symbols=["CURRENT"], client=client)
        self.assertGreater(case.case.receipt("CURRENT")["unresolved_rows"], 0)
        from libs.alpaca_lineage_build.universe_update import rebuild_universe

        rebuild_universe(case.case.store)
        path = self.selection(case.case.store, [names])
        read_document = handoff.read_document

        def selected_document(path, *args, **kwargs):
            self.assertNotIn("validator", str(path))
            self.assertNotIn("comparison", str(path))
            return read_document(path, *args, **kwargs)

        with (
            self.no_upstream_work(),
            patch.object(pipeline, "correlate", wraps=correlate) as evaluated,
            patch.object(pricing, "load_published_boundary", wraps=pricing.load_published_boundary) as reads,
            patch.object(handoff, "read_document", side_effect=selected_document),
        ):
            # This store deliberately has no serving catalogue: selected recovery
            # must report that boundary rather than silently bootstrap a universe.
            result = pipeline.run_lineage(
                case.case.store,
                [min(names)],
                identity_client=IdentityClient({}),
                retained_rebuild=True,
                retained_recovery=load_selection(path, case.case.store),
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(evaluated.call_count, 1)
        self.assertEqual(reads.call_count, 3 * len(names))
        case.assert_asof_current_history()
