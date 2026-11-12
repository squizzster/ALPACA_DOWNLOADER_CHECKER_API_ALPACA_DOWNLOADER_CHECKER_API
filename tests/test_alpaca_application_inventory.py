"""Inventory-only repair consumes selected peer markers and preserves data work."""

import json
from contextlib import ExitStack
from copy import deepcopy
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
    work_contract,
)
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.recovery_handoffs import operation, recall
from libs.alpaca_pricing_download import check_summary, update_checks
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_serving_catalogue import policy as serving_policy
from libs.alpaca_serving_catalogue import work as serving_work
from libs.alpaca_serving_catalogue.producer import maintain


class ApplicationInventoryPublicationTests(TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.store, self.root = self.case.store, self.case.root
        self.fingerprint = "a" * 64
        self.required = {"UNIT": {"accepted-component-facts": self.fingerprint}}
        self.pricing = {}
        for name in ("COMMON", "UNIT"):
            result = self.case.download(symbol=name)
            self.pricing[name] = result["source_sha256"]
        acquire = pipeline.acquire

        def accepted_metadata(view, *args, **kwargs):
            report = acquire(view, *args, **kwargs)
            if view.symbol == "COMMON":
                report = {
                    **report,
                    "sourced_fact_applications": {"accepted-component-facts": self.fingerprint},
                }
            return report

        assets = {name: fixture.asset(name, "same-identity") for name in self.pricing}
        with patch.object(pipeline, "acquire", side_effect=accepted_metadata):
            self.case.run_build(symbols=list(assets), client=fixture.IdentityClient(assets))
        self.index = storage.read_index(self.root)
        self.receipts = {name: storage.read_receipt(self.root, name, self.index) for name in self.pricing}
        self.generations = {name: self.index["symbols"][name]["sha256"] for name in self.pricing}
        self.assertNotIn("sourced_fact_applications", self.receipts["UNIT"]["evidence"])

    def prepare(self):
        return application_inventory.prepare(
            self.store, self.index, self.receipts, self.generations, self.pricing, self.required
        )

    def forbid_upstream(self, stack):
        for owner, name in (
            (pipeline, "correlate"),
            (pipeline, "load_pricing"),
            (observations, "select_observations"),
            (storage, "read_receipt"),
            (storage, "read_boundaries"),
            (storage, "read_checks"),
            (storage, "read_update"),
            (storage, "acknowledge_checks"),
            (storage, "acknowledge_update"),
            (projection, "project_receipt"),
            (check_summary, "invalidate"),
        ):
            stack.enter_context(
                patch.object(owner, name, side_effect=AssertionError("unnecessary work: " + name))
            )

    def publish(self, plan):
        return application_inventory.publish_locked(
            self.store, self.root, self.index, plan, self.pricing, self.required
        )

    def test_only_inventory_changes_and_returned_publication_is_reused(self):
        plan = self.prepare()
        old_peer = self.index["symbols"]["COMMON"]
        old_unit = self.receipts["UNIT"]
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
            operation(self.store),
            ExitStack() as stack,
        ):
            self.forbid_upstream(stack)
            result = self.publish(plan)
            self.assertEqual(
                recall(self.store, "receipt", ("UNIT", result["lineage_generations"]["UNIT"])),
                plan["receipts"]["UNIT"],
            )
            with patch.object(storage, "publish", side_effect=AssertionError("returned publication repeated")):
                self.assertIs(self.publish(plan), result)
        self.assertEqual(result["updated"], ["UNIT"])
        self.assertEqual(result["acknowledged_work"], [])
        self.assertEqual(result["authorities"], {"UNIT": {"accepted-component-facts": "COMMON"}})
        self.assertIs(self.index["symbols"]["COMMON"], old_peer)
        self.assertEqual(self.index["symbols"]["UNIT"]["source_sha256"], self.pricing["UNIT"])
        new_unit = plan["receipts"]["UNIT"]
        self.assertIs(new_unit["assignment_handoffs"], old_unit["assignment_handoffs"])
        self.assertIs(new_unit["boundaries"], old_unit["boundaries"])
        self.assertEqual(new_unit.get("update_obligation_id"), old_unit.get("update_obligation_id"))
        self.assertNotIn("sourced_fact_applications", old_unit["evidence"])

    def test_current_application_marker_is_required_and_another_component_cannot_lend_it(self):
        self.receipts["UNIT"] = {**self.receipts["UNIT"], "input_sources": {"UNIT": self.pricing["UNIT"]}}
        with self.assertRaisesRegex(LineageError, "no accepted component marker"):
            self.prepare()

    def test_changed_pricing_or_receipt_binding_rejects_before_publication(self):
        for boundary in ("pricing", "receipt"):
            with self.subTest(boundary=boundary):
                plan = self.prepare()
                if boundary == "pricing":
                    self.pricing["COMMON"] = "changed-pricing"
                else:
                    self.index["symbols"]["COMMON"] = {
                        **self.index["symbols"]["COMMON"],
                        "sha256": "changed-receipt",
                    }
                with patch.object(storage, "publish", side_effect=AssertionError("stale input published")):
                    with self.assertRaisesRegex(LineageError, "generation changed"):
                        self.publish(plan)
                self.pricing["COMMON"] = self.receipts["COMMON"]["source_sha256"]
                self.index["symbols"]["COMMON"] = {
                    **self.index["symbols"]["COMMON"],
                    "sha256": self.generations["COMMON"],
                }

    def test_renewed_application_obligation_is_not_consumed_by_an_old_plan(self):
        plan = self.prepare()
        self.required["UNIT"] = {"accepted-component-facts": "b" * 64}
        with patch.object(storage, "publish", side_effect=AssertionError("renewed work published")):
            with self.assertRaisesRegex(LineageError, "requirement changed"):
                self.publish(plan)

    def test_mode_rejects_every_change_outside_application_inventory(self):
        mutations = (
            lambda receipt: receipt.update(source_sha256="changed-price"),
            lambda receipt: receipt.update(processed_through="2026-10-15T00:00:00Z"),
            lambda receipt: receipt.update(update_obligation_id="new-correction-obligation"),
            lambda receipt: receipt["evidence"].update(sourced_fact_intents={"new-intent": False}),
            lambda receipt: receipt["evidence"].update(queried_symbols=["unselected-label"]),
            lambda receipt: receipt.update(boundaries={}),
            lambda receipt: receipt.update(identity_memberships=[]),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                plan = self.prepare()
                changed = deepcopy(plan["receipts"]["UNIT"])
                mutation(changed)
                outputs = {**plan["receipts"], "UNIT": changed}
                with patch.object(storage, "atomic_json", side_effect=AssertionError("invalid output written")):
                    with self.assertRaisesRegex(LineageError, "changed accepted lineage inputs"):
                        storage.publish(
                            self.store,
                            self.root,
                            self.index,
                            outputs,
                            {name: {} for name in outputs},
                            {},
                            previous=self.receipts,
                            unchanged_decision=True,
                            application_inventory_recovery=True,
                        )

    def test_summary_and_renewed_pricing_notice_and_check_survive(self):
        plan = self.prepare()
        notice_path = pricing_storage.update_path(self.store / "end_of_day/pricing/UNIT", published=True)
        notice_path.parent.mkdir(parents=True, exist_ok=True)
        notice_path.write_text('{"from":"2026-10-13","to":"2026-10-14"}')
        notice = notice_path.read_bytes()
        update_checks.queue_check(self.store, "UNIT")
        checks = update_checks.read_checks(self.store)
        token = check_summary.begin_rebuild(self.store)
        check_summary.finish_rebuild(self.store, token, [])
        summary = check_summary.read_snapshot(self.store)
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
            ExitStack() as stack,
        ):
            self.forbid_upstream(stack)
            self.publish(plan)
        self.assertEqual(notice_path.read_bytes(), notice)
        self.assertEqual(update_checks.read_checks(self.store), checks)
        self.assertEqual(check_summary.read_snapshot(self.store), summary)

    def test_interrupted_journal_recovers_without_rebuilding_inventory_or_assignments(self):
        plan = self.prepare()
        atomic = storage.atomic_json

        def interrupted(path, *args, **kwargs):
            if path.name == "lineage.manifest.json":
                raise OSError("inventory installation interrupted")
            return atomic(path, *args, **kwargs)

        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
            patch.object(storage, "atomic_json", side_effect=interrupted),
            self.assertRaisesRegex(OSError, "installation interrupted"),
        ):
            self.publish(plan)
        self.assertTrue((self.root / ".pending.json").exists())
        self.assertEqual(storage.read_index(self.root)["symbols"]["UNIT"]["sha256"], self.generations["UNIT"])
        with (
            storage.writer_lock(self.store),
            patch.object(
                application_inventory, "prepare", side_effect=AssertionError("inventory proof repeated")
            ),
            patch.object(
                storage, "document_digest", side_effect=AssertionError("completed generation recalculated")
            ),
            patch.object(observations, "select_observations", side_effect=AssertionError("prices reassessed")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("assignments reopened")),
            patch.object(check_summary, "invalidate", side_effect=AssertionError("summary invalidated")),
        ):
            self.assertTrue(storage.recover(self.store, self.root))
        self.assertFalse((self.root / ".pending.json").exists())
        final = storage.read_index(self.root)
        self.assertEqual(
            final["symbols"]["UNIT"]["projection"]["sourced_fact_applications"],
            self.required["UNIT"],
        )

    def test_serving_consumes_unchanged_decision_handoff_without_assessment(self):
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        plan = self.prepare()
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
        ):
            result = self.publish(plan)
        handoffs = serving_work.read_handoffs(self.store, result["lineage_generations"])
        self.assertEqual(len(handoffs), 1)
        with (
            patch.object(
                serving_policy, "evaluate", side_effect=AssertionError("accepted serving outcome reassessed")
            ),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("accepted assignments reopened")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
        ):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)

    def metadata_policy(self):
        universe_state.initialize(self.store)
        lookup = next(
            lookup
            for lookup in self.receipts["UNIT"]["evidence"]["asset_lookups"]
            if lookup["lookup_kind"] == "symbol" and lookup["lookup"] == "UNIT"
        )
        work = universe_state.put_work_group(
            self.store,
            {
                "UNIT": {
                    "obligation_id": "exact-metadata-obligation",
                    "status": "pending",
                    "end_exclusive": "2026-10-20",
                    "pricing_end_exclusive": "2026-10-20",
                    "reasons": ["sourced_facts", "lineage_evidence"],
                    "required_group": sorted(self.receipts),
                    "required_lookups": {"UNIT": lookup},
                    "required_sourced_facts": self.required["UNIT"],
                    "acquire": False,
                    "invalidate_pricing": False,
                    "required_history_check": False,
                }
            },
        )
        return work_contract.WorkPolicy(self.store, self.index, work, [], "2026-10-20")

    def test_metadata_job_completes_old_coverage_without_claiming_a_price_update(self):
        plan = self.prepare()
        policy = self.metadata_policy()
        expected = deepcopy(policy.work)
        old_end = self.index["symbols"]["UNIT"]["projection"]["processed_through"]
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
        ):
            self.publish(plan)
            self.assertIsNone(policy.satisfying_entry("UNIT", self.index["symbols"], {}))
            result = application_inventory.accept_metadata_completion(
                policy, self.index, plan, expected, self.pricing
            )
            self.assertEqual(result, {"accepted_metadata_work": ["UNIT"], "price_coverage_asserted": False})
            with (
                patch.object(
                    work_contract, "symbol_lookups", side_effect=AssertionError("held lookup scope recalculated")
                ),
                patch.object(
                    work_contract, "lookup_fact", side_effect=AssertionError("lookup applicability re-proved")
                ),
                patch.object(
                    policy, "accept_completion", side_effect=AssertionError("completion references recalculated")
                ),
            ):
                self.assertIs(
                    application_inventory.accept_metadata_completion(
                        policy, self.index, plan, expected, self.pricing
                    ),
                    result,
                )
            policy.acknowledge()
        self.assertEqual(
            universe_state.read_state(self.store, symbols=["UNIT"])["work"]["UNIT"]["status"], "complete"
        )
        self.assertEqual(self.index["symbols"]["UNIT"]["projection"]["processed_through"], old_end)
        self.assertLess(old_end[:10], policy.end)

    def test_pricing_append_correction_future_and_incomplete_scope_are_not_metadata_jobs(self):
        plan = self.prepare()
        policy = self.metadata_policy()
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
        ):
            self.publish(plan)
        baseline = deepcopy(policy.work["UNIT"])
        changes = (
            {"acquire": True},
            {"invalidate_pricing": True},
            {"required_history_check": True},
            {"pricing_start": "2016-01-01"},
            {"correction_id": "actual-correction"},
            {"pricing_correction": {"completed": "not metadata"}},
            {"reasons": ["advance_summary_coverage"]},
            {"end_exclusive": "2026-10-21"},
            {"pricing_end_exclusive": "2026-10-21"},
            {"required_group": [*self.receipts, "MISSING"]},
        )
        for change in changes:
            with self.subTest(change=change):
                policy.work["UNIT"] = {**deepcopy(baseline), **change}
                with self.assertRaisesRegex(work_contract.WorkDeferred, "not an admitted metadata-only"):
                    application_inventory.accept_metadata_completion(
                        policy, self.index, plan, deepcopy(policy.work), self.pricing
                    )
                self.assertEqual(policy.completed_decisions, {})
        self.assertEqual(
            universe_state.read_state(self.store, symbols=["UNIT"])["work"]["UNIT"]["status"], "pending"
        )

    def test_metadata_completion_rejects_changed_obligation_lookup_and_peer_generation(self):
        plan = self.prepare()
        policy = self.metadata_policy()
        expected = deepcopy(policy.work)
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
        ):
            self.publish(plan)
        policy.work["UNIT"] = {**policy.work["UNIT"], "obligation_id": "renewed"}
        with self.assertRaisesRegex(work_contract.WorkDeferred, "obligation changed"):
            application_inventory.accept_metadata_completion(policy, self.index, plan, expected, self.pricing)
        policy.work["UNIT"] = deepcopy(expected["UNIT"])
        policy.work["UNIT"]["required_lookups"]["UNIT"]["asset"]["id"] = "new-asset-mapping"
        with self.assertRaisesRegex(work_contract.WorkDeferred, "held metadata differs"):
            application_inventory.accept_metadata_completion(
                policy, self.index, plan, deepcopy(policy.work), self.pricing
            )
        policy.work["UNIT"] = deepcopy(expected["UNIT"])
        self.index["symbols"]["COMMON"] = {**self.index["symbols"]["COMMON"], "sha256": "actual-changed-peer"}
        with self.assertRaisesRegex(work_contract.WorkDeferred, "selected generation changed"):
            application_inventory.accept_metadata_completion(policy, self.index, plan, expected, self.pricing)
        self.assertEqual(policy.completed_decisions, {})

    def test_renewed_durable_work_survives_old_metadata_completion_acknowledgement(self):
        plan = self.prepare()
        policy = self.metadata_policy()
        expected = deepcopy(policy.work)
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
        ):
            self.publish(plan)
            application_inventory.accept_metadata_completion(policy, self.index, plan, expected, self.pricing)
            renewed = {**policy.work["UNIT"], "obligation_id": "renewed-after-completion"}
            universe_state.put_work_group(self.store, {"UNIT": renewed})
            with self.assertRaisesRegex(LineageError, "work changed before acknowledgement"):
                policy.acknowledge()
        current = universe_state.read_state(self.store, symbols=["UNIT"])["work"]["UNIT"]
        self.assertEqual(current["obligation_id"], renewed["obligation_id"])
        self.assertEqual(current["status"], "pending")

    def test_prepared_packet_crosses_json_without_repeating_its_projection(self):
        from libs.alpaca_pipeline.codec import Decoder, Encoder

        plan = self.prepare()
        encoder = Encoder()
        packed = encoder.encode(plan)
        document = json.loads(json.dumps({"plan": packed, "objects": encoder.objects}))
        plan = Decoder(document["objects"]).decode(document["plan"])
        self.assertIs(
            plan["receipts"]["UNIT"]["assignment_handoffs"],
            plan["previous"]["UNIT"]["assignment_handoffs"],
        )
        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
            patch.object(
                application_inventory, "prepare", side_effect=AssertionError("completed projection repeated")
            ),
            ExitStack() as stack,
        ):
            self.forbid_upstream(stack)
            result = self.publish(plan)
        self.assertEqual(result["updated"], ["UNIT"])

    def test_selector_and_serving_handoff_roll_back_then_journal_recovers_newer_work_untouched(self):
        plan = self.prepare()
        record = serving_work.record_publication

        def fail_transaction(*args, **kwargs):
            record(*args, **kwargs)
            raise OSError("inventory index transaction interrupted")

        with (
            storage.writer_lock(self.store),
            storage.pricing_locks(self.store, self.pricing, known=self.index["symbols"]),
            patch.object(serving_work, "record_publication", side_effect=fail_transaction),
            self.assertRaisesRegex(OSError, "index transaction interrupted"),
        ):
            self.publish(plan)
        journal = json.loads((self.root / ".pending.json").read_text())
        new_generations = {name: entry["sha256"] for name, entry in journal["index_updates"].items()}
        self.assertEqual(serving_work.read_handoffs(self.store, new_generations), ())
        self.assertEqual(storage.read_index(self.root)["symbols"]["UNIT"]["sha256"], self.generations["UNIT"])
        notice_path = pricing_storage.update_path(self.store / "end_of_day/pricing/UNIT", published=True)
        notice_path.parent.mkdir(parents=True, exist_ok=True)
        notice_path.write_text('{"from":"2026-10-14","to":"2026-10-15"}')
        notice = notice_path.read_bytes()
        update_checks.queue_check(self.store, "UNIT")
        newer_checks = update_checks.read_checks(self.store)
        with (
            storage.writer_lock(self.store),
            patch.object(
                storage, "document_digest", side_effect=AssertionError("completed header hashed again")
            ),
            patch.object(
                application_inventory,
                "prepare",
                side_effect=AssertionError("projection repeated after rollback"),
            ),
            patch.object(check_summary, "invalidate", side_effect=AssertionError("summary invalidated")),
        ):
            self.assertTrue(storage.recover(self.store, self.root))
        self.assertEqual(notice_path.read_bytes(), notice)
        self.assertEqual(update_checks.read_checks(self.store), newer_checks)
        self.assertEqual(len(serving_work.read_handoffs(self.store, new_generations)), 1)
