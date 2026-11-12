"""Prepared recovery installs completed output and never replays its producer."""

import copy
import json
import unittest
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import assignment_handoff, group_engine, pipeline, storage, transform
from libs.alpaca_lineage_build.contract import COMPUTATION_REVISION
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.prepared_recovery import CANDIDATE_BOUNDARY
from libs.alpaca_lineage_build.retained_recovery import load_selection, recover
from libs.alpaca_pricing_download import storage as pricing_storage


def candidate_for(store):
    root = store / "end_of_day/lineage"
    index = storage.read_index(root)
    old = storage.read_receipt(root, "AAA", index)
    view = pipeline.load_pricing(store, "AAA")
    policy = load_authorized_policy()
    reports = transform.correlate(store, {"AAA": view}, {"AAA": old["evidence"]}, policy=policy)
    reports, corrections = select_observations({"AAA": view}, reports)
    assert not corrections
    # A completed producer-specific result must survive publication verbatim;
    # recalculating the ordinary candidate would lose this explanation.
    for segment in reports["AAA"]["correlated"]["segments"]:
        segment["warnings"] = [*segment.get("warnings", []), "prepared producer explanation"]
    receipt, boundaries = transform.make_publication(view, old["evidence"], reports["AAA"], old["dependencies"])
    return {
        "boundary": CANDIDATE_BOUNDARY,
        "version": 1,
        "store": str(store.absolute()),
        "members": ["AAA"],
        "policy": policy.as_report_object(),
        "computation_revision": COMPUTATION_REVISION,
        "receipt_generations": {"AAA": index["symbols"]["AAA"]["sha256"]},
        "pricing_generations": {"AAA": view.source_sha256},
        "baseline_receipts": {"AAA": old},
        "candidate_receipts": {"AAA": receipt},
        "candidate_boundaries": {"AAA": boundaries},
        "comparison": {"comparison": {"mapping_corrections": {}, "candidate_partition_complete": True}},
        "review": {"version": 1, "status": "approved", "conclusion": "Complete synthetic producer result."},
    }


class PreparedRecoveryTests(unittest.TestCase):
    def save(self, store, value):
        path = store.parent / "prepared.json"
        path.write_text(json.dumps(value))
        return path

    @contextmanager
    def no_replay(self):
        with ExitStack() as stack:
            for owner, name in (
                (pipeline, "load_pricing"),
                (pipeline, "correlate"),
                (pipeline, "select_observations"),
                (group_engine, "make_publication"),
                (pipeline, "download_pricing"),
                (pipeline, "acquire"),
                (pipeline, "local_symbols"),
                (transform, "make_publication"),
                (assignment_handoff, "produce"),
                (assignment_handoff, "comparison"),
                (assignment_handoff, "_valid_run"),
                (pricing_storage, "read_validator"),
                (pricing_storage, "publish"),
                (storage, "selected_document"),
            ):
                stack.enter_context(patch.object(owner, name, side_effect=AssertionError(name)))
            yield

    def test_prepared_output_reaches_reader_and_retry_skips_completed_work(self):
        with fixture() as case:
            value = candidate_for(case.store)
            path = self.save(case.store, value)
            selection = load_selection(path, case.store)
            with self.no_replay():
                result = recover(case.store, None, selection=selection)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["published_prepared_groups"], 1)
            self.assertEqual(result["recomputed_groups"], 0)
            root = case.store / "end_of_day/lineage"
            index = storage.read_index(root)
            receipt = storage.read_receipt(root, "AAA", index)
            boundaries = storage.read_boundaries(root, receipt)
            for document in boundaries.values():
                for view in document["adjustments"]:
                    for segment in view["segments"]:
                        if segment["branch"] == "correlated":
                            self.assertIn("prepared producer explanation", segment["warnings"])
            reader = StandaloneReader(case.store)
            pid = reader.resolve_ticker("AAA")["matches"][0]["public_id"]
            history = reader.get_histories("AAA", public_id=pid)
            self.assertTrue(history)
            selection = load_selection(path, case.store)
            with (
                self.no_replay(),
                patch.object(
                    pipeline, "_current_sources", side_effect=AssertionError("completed source reread")
                ),
                patch.object(storage, "publish", side_effect=AssertionError("completed output republished")),
            ):
                repeated = recover(case.store, None, selection=selection)
            self.assertTrue(repeated["complete"], repeated)
            self.assertEqual(repeated["previously_completed_groups"], 1)
            self.assertEqual(repeated["published_prepared_groups"], 0)

    def test_unchanged_references_carry_their_already_read_assignments(self):
        with fixture() as case:
            value = candidate_for(case.store)
            root = case.store / "end_of_day/lineage"
            prior = value["baseline_receipts"]["AAA"]
            from libs.alpaca_lineage_build.source_work import assignment_documents

            accepted = assignment_documents(root, prior, {})
            kind = "completed_years"
            receipt = value["candidate_receipts"]["AAA"]
            receipt["boundaries"][kind] = prior["boundaries"][kind]
            receipt["assignment_handoffs"][kind] = prior["assignment_handoffs"][kind]
            value["candidate_boundaries"]["AAA"].pop(kind, None)
            value["baseline_boundaries"] = {"AAA": accepted}
            path = self.save(case.store, value)
            selection = load_selection(path, case.store)
            with self.no_replay():
                result = recover(case.store, None, selection=selection)
            self.assertTrue(result["complete"], result)
            self.assertEqual(1, result["published_prepared_groups"])

    def test_journal_restart_before_and_after_install_retains_completed_construction(self):
        class Interrupted(BaseException):
            pass

        for after in (False, True):
            with self.subTest(after_install=after), fixture() as case:
                path = self.save(case.store, candidate_for(case.store))
                selection = load_selection(path, case.store)
                install = storage._install

                def interrupt(*args, **kwargs):
                    if after:
                        install(*args, **kwargs)
                    raise Interrupted()

                with self.no_replay(), patch.object(storage, "_install", side_effect=interrupt):
                    with self.assertRaises(Interrupted):
                        recover(case.store, None, selection=selection)
                # A fresh invocation accepts the transport schema once. Recovery
                # then consumes the frozen journal, including prepared projections.
                selection = load_selection(path, case.store)
                with self.no_replay():
                    result = recover(case.store, None, selection=selection)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["published_prepared_groups"], 0)
                self.assertEqual(result["previously_completed_groups"], 1)

    def test_changed_selector_mapping_and_policy_reject_before_producer_work(self):
        for change in ("receipt", "pricing", "membership", "mapping", "policy"):
            with self.subTest(change=change), fixture() as case:
                value = candidate_for(case.store)
                if change == "policy":
                    value["policy"]["semantic_sha256"] = "a" * 64
                    value["candidate_receipts"]["AAA"]["correlation_policy"]["semantic_sha256"] = "a" * 64
                path = self.save(case.store, value)
                selection = load_selection(path, case.store)
                index = storage.read_index(case.store / "end_of_day/lineage")
                entry = index["symbols"]["AAA"]
                if change == "receipt":
                    entry["sha256"] = "a" * 64
                elif change == "pricing":
                    entry["source_sha256"] = "a" * 64
                elif change == "membership":
                    entry["group_members"] = ["AAA", "BBB"]
                select_sources = pipeline._current_sources

                def changed_mapping(*args, **kwargs):
                    sources = select_sources(*args, **kwargs)
                    sources["AAA"]["latest_request"]["asof"] = "2026-10-01"
                    return sources

                with (
                    self.no_replay(),
                    patch.object(storage, "refresh_index", return_value=index),
                    patch.object(pipeline, "_current_sources", side_effect=changed_mapping)
                    if change == "mapping"
                    else ExitStack(),
                    patch.object(storage, "publish", side_effect=AssertionError("stale publication")),
                ):
                    with self.assertRaisesRegex(RuntimeError, "changed"):
                        recover(case.store, None, selection=selection)

    def test_incomplete_or_unreviewed_output_fails_without_reconstruction(self):
        for change in ("unreviewed", "missing_source", "missing_projection", "mapping_correction"):
            with self.subTest(change=change), fixture() as case:
                value = candidate_for(case.store)
                if change == "unreviewed":
                    value.pop("review")
                elif change == "missing_source":
                    value["candidate_receipts"] = {}
                elif change == "missing_projection":
                    value["candidate_receipts"]["AAA"]["assignment_handoffs"]["completed_years"].pop(
                        "_projection"
                    )
                else:
                    value["comparison"]["comparison"]["mapping_corrections"] = {"AAA": {}}
                with self.assertRaises(RuntimeError):
                    load_selection(self.save(case.store, copy.deepcopy(value)), case.store)
