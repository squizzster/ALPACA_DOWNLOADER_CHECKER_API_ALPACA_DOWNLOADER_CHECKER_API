"""Retired application ingress and preserved installation of frozen output."""

from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import (
    application_inventory,
    observations,
    pipeline,
    projection,
    storage,
    universe_state,
)
from libs.alpaca_lineage_build.contract import VERSION, LineageError, document_digest
from libs.alpaca_pricing_download import check_summary, update_checks
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_serving_catalogue import policy as serving_policy
from libs.alpaca_serving_catalogue import work as serving_work
from libs.alpaca_serving_catalogue.producer import maintain


class ApplicationInventoryIngressTests(TestCase):
    def test_fresh_preparation_publication_and_acknowledgement_reject_before_io(self):
        for changed in ({}, {"required": "new"}, {"source": "changed"}, {"receipt": "changed"}):
            operations = (
                lambda: application_inventory.prepare(None, None, None, None, None, changed),
                lambda: application_inventory.publish_locked(None, None, None, changed, None, None),
                lambda: application_inventory.accept_metadata_completion(None, None, changed, None, None),
            )
            for operation in operations:
                with (
                    self.subTest(changed=changed, operation=operation),
                    patch.object(Path, "open", side_effect=AssertionError("retired inventory opened storage")),
                    patch.object(storage, "publish", side_effect=AssertionError("factual inventory published")),
                    patch.object(
                        universe_state, "record_outcome", side_effect=AssertionError("factual work acknowledged")
                    ),
                    self.assertRaisesRegex(LineageError, "generic lineage recovery"),
                ):
                    operation()


class FrozenApplicationInventoryJournalTests(TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.store, self.root = self.case.store, self.case.root
        self.pricing = {name: self.case.download(symbol=name)["source_sha256"] for name in ("COMMON", "UNIT")}
        assets = {name: fixture.asset(name, "same-identity") for name in self.pricing}
        self.case.run_build(symbols=list(assets), client=fixture.IdentityClient(assets))
        self.index = storage.read_index(self.root)
        self.receipts = {name: storage.read_receipt(self.root, name, self.index) for name in self.pricing}
        # This literal models a historical producer's COMPLETED output. It is
        # installed by its frozen journal, never sent to new inference or any
        # retired application preparation/publication function.
        old = self.receipts["UNIT"]
        self.closed = {
            **old,
            "evidence": {**old["evidence"], "sourced_fact_applications": {"legacy-completed": "a" * 64}},
        }
        self.closed_generation = document_digest(self.closed)
        self.closed_entry = {
            **self.index["symbols"]["UNIT"],
            "sha256": self.closed_generation,
            "projection": {
                **self.index["symbols"]["UNIT"]["projection"],
                "sourced_fact_applications": {"legacy-completed": "a" * 64},
            },
        }
        before = {name: self.index["symbols"][name]["sha256"] for name in self.pricing}
        self.journal = {
            "schema_version": VERSION,
            "boundary": "alpaca_lineage_transaction",
            "correlation_policy": old["correlation_policy"],
            "documents": {"UNIT/lineage.manifest.json": self.closed},
            "retire": [],
            "index_updates": {"UNIT": self.closed_entry},
            "sources": self.pricing,
            "unavailable_pricing": [],
            "notices": {},
            "export_input_changes": {},
            "projection_only": True,
            "serving_handoff": {
                "version": 1,
                "lineage_before": before,
                "lineage_after": {**before, "UNIT": self.closed_generation},
                "pricing": self.pricing,
            },
        }

    def stage(self):
        storage.atomic_json(self.root / ".pending.json", self.journal)

    def forbid_replay(self, stack):
        for owner, name in (
            (application_inventory, "prepare"),
            (application_inventory, "publish_locked"),
            (application_inventory, "accept_metadata_completion"),
            (pipeline, "correlate"),
            (pipeline, "load_pricing"),
            (observations, "select_observations"),
            (storage, "read_receipt"),
            (storage, "read_boundaries"),
            (storage, "read_checks"),
            (storage, "load_source"),
            (storage, "document_digest"),
            (projection, "project_receipt"),
            (check_summary, "invalidate"),
            (universe_state, "record_outcome"),
        ):
            stack.enter_context(
                patch.object(owner, name, side_effect=AssertionError("completed work repeated: " + name))
            )

    def recover(self):
        with storage.writer_lock(self.store), ExitStack() as stack:
            self.forbid_replay(stack)
            return storage.recover(self.store, self.root)

    def test_frozen_output_installs_with_original_assignments_membership_and_numerical_choices(self):
        self.stage()
        self.assertTrue(self.recover())
        index = storage.read_index(self.root)
        final = storage.read_receipt(self.root, "UNIT", index)
        self.assertEqual(self.closed, final)
        self.assertEqual(self.closed_generation, index["symbols"]["UNIT"]["sha256"])
        self.assertEqual(self.receipts["UNIT"]["assignment_handoffs"], final["assignment_handoffs"])
        self.assertEqual(self.receipts["UNIT"]["identity_memberships"], final["identity_memberships"])
        self.assertEqual(self.index["symbols"]["COMMON"], index["symbols"]["COMMON"])
        self.assertFalse((self.root / ".pending.json").exists())

    def test_interrupted_installation_reuses_the_completed_journal_without_rebuilding(self):
        self.stage()
        atomic = storage.atomic_json

        def interrupted(path, *args, **kwargs):
            if path.name == "lineage.manifest.json":
                raise OSError("frozen installation interrupted")
            return atomic(path, *args, **kwargs)

        with (
            patch.object(storage, "atomic_json", side_effect=interrupted),
            self.assertRaisesRegex(OSError, "interrupted"),
        ):
            self.recover()
        self.assertTrue((self.root / ".pending.json").exists())
        self.assertEqual(self.index["symbols"]["UNIT"], storage.read_index(self.root)["symbols"]["UNIT"])
        self.assertTrue(self.recover())
        self.assertEqual(self.closed_generation, storage.read_index(self.root)["symbols"]["UNIT"]["sha256"])

    def test_index_transaction_rollback_keeps_serving_handoff_with_the_frozen_retry(self):
        self.stage()
        record = serving_work.record_publication

        def interrupted(*args, **kwargs):
            record(*args, **kwargs)
            raise OSError("frozen index transaction interrupted")

        with (
            patch.object(serving_work, "record_publication", side_effect=interrupted),
            self.assertRaisesRegex(OSError, "interrupted"),
        ):
            self.recover()
        self.assertEqual((), serving_work.read_handoffs(self.store, {"UNIT": self.closed_generation}))
        self.assertEqual(self.index["symbols"]["UNIT"], storage.read_index(self.root)["symbols"]["UNIT"])
        self.assertTrue(self.recover())
        self.assertEqual(1, len(serving_work.read_handoffs(self.store, {"UNIT": self.closed_generation})))

    def test_new_pricing_notice_checks_and_summary_survive_frozen_metadata_installation(self):
        path = pricing_storage.update_path(self.store / "end_of_day/pricing/UNIT", published=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"from":"2026-10-13","to":"2026-10-14"}')
        notice = path.read_bytes()
        update_checks.queue_check(self.store, "UNIT")
        checks = update_checks.read_checks(self.store)
        token = check_summary.begin_rebuild(self.store)
        check_summary.finish_rebuild(self.store, token, [])
        summary = check_summary.read_snapshot(self.store)
        self.stage()
        self.assertTrue(self.recover())
        self.assertEqual(notice, path.read_bytes())
        self.assertEqual(checks, update_checks.read_checks(self.store))
        self.assertEqual(summary, check_summary.read_snapshot(self.store))

    def test_serving_consumes_closed_output_handoff_without_reassessment(self):
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        self.stage()
        self.assertTrue(self.recover())
        with (
            patch.object(
                serving_policy, "evaluate", side_effect=AssertionError("completed serving outcome repeated")
            ),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("assignments reopened")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("prices reopened")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)

    def test_malformed_frozen_provenance_fails_before_installation(self):
        changed = deepcopy(self.journal)
        changed["documents"]["UNIT/lineage.manifest.json"]["correlation_policy"] = {}
        storage.atomic_json(self.root / ".pending.json", changed)
        with patch.object(storage, "atomic_json", side_effect=AssertionError("invalid journal installed")):
            with self.assertRaisesRegex(LineageError, "policy provenance is inconsistent"):
                self.recover()
        self.assertEqual(self.index["symbols"]["UNIT"], storage.read_index(self.root)["symbols"]["UNIT"])
