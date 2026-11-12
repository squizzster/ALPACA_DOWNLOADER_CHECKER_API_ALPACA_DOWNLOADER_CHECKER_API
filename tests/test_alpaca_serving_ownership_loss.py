"""Unexplained mapped-history loss cannot silently shorten an accepted history."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, PublicIdUnavailableError, _standalone_group, standalone
from libs.alpaca_serving_catalogue import policy, producer, storage, work
from libs.alpaca_serving_catalogue.offline import select_offline
from libs.alpaca_serving_catalogue.ownership_loss import AcceptedOwnershipLost, require_retained_owners
from libs.alpaca_serving_catalogue.producer import maintain


class OwnershipLossGateTests(TestCase):
    def fixture(self):
        old = {
            "pricing_generation": "old",
            "views": {"raw": {"ownership": [{"rows": 3, "owner": ["id", ["OLD"]]}]}},
        }
        previous = [{"history_validation": {"inputs": {"NEW": old}, "prefix_records": [{"public_id": "id"}]}}]
        segment = {
            "branch": "uncorrelated",
            "reason_codes": ["outside_supported_name_change_history"],
            "candidate_securities": [],
            "row_start": 1,
            "row_end": 2,
            "from": "2026-10-01",
            "to": "2026-10-02",
        }
        group = SimpleNamespace(
            summary={"securities": [{"public_id": "id"}]},
            assignments={"NEW": [("raw", "current_month", segment)]},
            preserves_pricing_prefix=lambda *args, **kwargs: True,
        )
        return group, previous, {"NEW": {"pricing_generation": "new"}}, {"NEW": {"raw": {"current_month": 0}}}

    def test_exact_positions_and_missing_capability_are_reported(self):
        with self.assertRaises(AcceptedOwnershipLost) as raised:
            require_retained_owners(*self.fixture())
        scope = raised.exception.outcome["lost_scopes"][0]
        self.assertEqual(("NEW", 1, 2), (scope["source"], scope["row_start"], scope["row_end"]))
        self.assertEqual("accepted_ownership_recovery_required", raised.exception.outcome["outcome"])

    def test_actual_invalidation_and_unselected_positions_keep_existing_routes(self):
        for change in ("membership", "pricing", "owner", "reason", "candidate", "new_rows"):
            with self.subTest(change=change):
                group, previous, inputs, offsets = self.fixture()
                segment = group.assignments["NEW"][0][2]
                if change == "membership":
                    group.summary["securities"][0]["public_id"] = "new-membership"
                elif change == "pricing":
                    group.preserves_pricing_prefix = lambda *args, **kwargs: False
                elif change == "owner":
                    segment["branch"] = "correlated"
                elif change == "reason":
                    segment["reason_codes"] = ["conflicting_identity_evidence"]
                elif change == "candidate":
                    segment["candidate_securities"] = [{"asset_id": "different-owner"}]
                else:
                    segment.update(row_start=4, row_end=5)
                require_retained_owners(group, previous, inputs, offsets)

    def test_offline_scope_includes_overlapping_uuid_but_excludes_rights(self):
        group, previous, inputs, offsets = self.fixture()
        group.summary["securities"].extend(
            [{"public_id": "singleton"}, {"public_id": "linked"}, {"public_id": "rights"}]
        )
        for pid, uuids in (
            ("id", ["old", "new"]),
            ("singleton", ["new", "linked-uuid"]),
            ("linked", ["linked-uuid"]),
            ("rights", ["right-uuid"]),
        ):
            group.assignments["NEW"].append(
                (
                    "raw",
                    "current_month",
                    {
                        "branch": "correlated",
                        "security_identity": {"security_id": pid, "member_alpaca_asset_ids": uuids},
                    },
                )
            )
        previous[0].update(
            locator="NEW", policy={}, references={}, decision_generation="accepted", securities=[], decisions=[]
        )
        previous[0]["history_validation"]["identities"] = {"id": {}}
        blocked, envelope = select_offline(group, previous, inputs, offsets)
        self.assertEqual(blocked, {"id", "singleton", "linked"})
        self.assertEqual(envelope["reason"], "accepted_ownership_recovery_required")


class OwnershipLossPublicationTests(NativeCase):
    def test_ordinary_maintenance_marks_offline_and_keeps_checkpoint_without_history_reads(self):
        self.bootstrap()
        prior = self.catalogue_groups("TEST")["TEST"]
        generation = storage.selector_state(self.store)["generation"]

        def lose_prefix(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if not view["segments"]:
                        continue
                    original = view["segments"][0]
                    lost, retained = deepcopy(original), deepcopy(original)
                    lost.update(
                        branch="uncorrelated",
                        row_end=2,
                        row_count=2,
                        to=self.bars[1]["t"][:10],
                        last_timestamp=self.bars[1]["t"],
                        reason_codes=["outside_supported_name_change_history"],
                        candidate_securities=[],
                    )
                    lost.pop("security_identity", None)
                    retained.update(
                        row_start=3,
                        row_count=original["row_count"] - 2,
                        first_timestamp=self.bars[2]["t"],
                        **{"from": self.bars[2]["t"][:10]},
                    )
                    view["segments"] = [lost, retained, *view["segments"][1:]]

        self.publish_change(lose_prefix)
        with (
            patch.object(_standalone_group, "_rows", side_effect=AssertionError("date payload read")),
            patch.object(standalone, "_rows", side_effect=AssertionError("historical price read")),
            patch.object(policy, "select_prefixes", side_effect=AssertionError("downstream reuse attempted")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["failed"])
        self.assertNotEqual(generation, storage.selector_state(self.store)["generation"])
        completed = self.catalogue_groups("TEST")["TEST"]
        self.assertFalse(completed["securities"])
        offline = completed["offline_ownership"]
        self.assertEqual("accepted_ownership_recovery_required", offline["reason"])
        self.assertEqual(prior["history_validation"], offline["checkpoints"][0]["history_validation"])
        self.assertFalse(work.read(self.store))
        with self.assertRaises(PublicIdUnavailableError):
            DataLibrary(store=self.store).get_security(prior["securities"][0]["public_id"])
        with patch.object(producer, "evaluate", side_effect=AssertionError("offline decision repeated")):
            self.assertTrue(maintain(self.store)["complete"])

    def test_healthy_export_continues_and_changed_inputs_restore_offline_identity(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        original = self.catalogue_groups("TEST")["TEST"]
        from libs.alpaca_lineage_build import storage as lineage

        index = lineage.read_index(self.root)
        receipt = lineage.read_receipt(self.root, "TEST", index)
        boundaries = lineage.read_boundaries(self.root, receipt)

        def lose(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if not view["segments"]:
                        continue
                    old = view["segments"][0]
                    lost, kept = deepcopy(old), deepcopy(old)
                    lost.update(
                        branch="uncorrelated",
                        row_end=2,
                        row_count=2,
                        to=self.bars[1]["t"][:10],
                        last_timestamp=self.bars[1]["t"],
                        reason_codes=["outside_supported_name_change_history"],
                        candidate_securities=[],
                    )
                    lost.pop("security_identity", None)
                    kept.update(
                        row_start=3,
                        row_count=old["row_count"] - 2,
                        first_timestamp=self.bars[2]["t"],
                        **{"from": self.bars[2]["t"][:10]},
                    )
                    view["segments"] = [lost, kept, *view["segments"][1:]]

        self.publish_change(lose)
        with patch.object(standalone, "_rows", side_effect=AssertionError("offline prices read")):
            self.assertTrue(maintain(self.store)["complete"])
        export = DataLibrary(store=self.store).write_csv_export(self.directory / "healthy")
        self.assertEqual(export["identities"], 1)
        self.assertEqual(len(data.list_securities()), 2)
        # A newly published corrected owning decision invalidates the negative
        # result. Its frozen checkpoint still supplies numerical approval.
        current_index = lineage.read_index(self.root)
        receipt["assignment_handoffs"] = {}  # New producer builds the restored projections.
        with lineage.pricing_locks(self.store, ["TEST"]):
            lineage.publish(self.store, self.root, current_index, {"TEST": receipt}, {"TEST": boundaries}, {})
        with patch.object(standalone, "_rows", side_effect=AssertionError("accepted prices rechecked")):
            restored = maintain(self.store)
        self.assertTrue(restored["complete"], restored)
        self.assertEqual(restored["checked_bars"], 0)
        self.assertEqual(len(DataLibrary(store=self.store).list_securities()), 2)
        self.assertEqual(
            original["history_validation"], self.catalogue_groups("TEST")["TEST"]["history_validation"]
        )

    def test_same_group_independent_identity_survives_offline_commit_and_interruption(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        from libs.alpaca_lineage_build import storage as lineage

        index = lineage.read_index(self.root)
        receipts = {name: lineage.read_receipt(self.root, name, index) for name in ("TEST", "OTHER")}
        sources = {name: r["source_sha256"] for name, r in receipts.items()}
        for receipt in receipts.values():
            receipt["input_sources"] = sources
            receipt["dependencies"] = ["OTHER", "TEST"]
        with lineage.pricing_locks(self.store, sources):
            lineage.publish(self.store, self.root, index, receipts, {name: {} for name in receipts}, {})
        self.assertTrue(maintain(self.store)["complete"])
        original = self.catalogue_groups("OTHER")["OTHER"]
        owner = producer.evaluate

        def lose_one_identity(group, previous, **kwargs):
            from libs.alpaca_serving_catalogue import offline

            with patch.object(
                offline,
                "retained_owner_losses",
                return_value=[{"public_id": original["securities"][0]["public_id"]}],
            ):
                return owner(group, previous, **kwargs)

        self.publish_change(lambda r, b: r["evidence"].update(warnings=["changed selected evidence"]))

        class Interrupted(BaseException):
            pass

        with (
            patch.object(producer, "evaluate", side_effect=lose_one_identity),
            patch.object(work, "acknowledge", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        completed = self.catalogue_groups("OTHER")["OTHER"]
        self.assertEqual(len(completed["securities"]), 1)
        self.assertEqual(len(completed["offline_ownership"]["public_ids"]), 1)
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("offline computation replayed")),
            patch.object(storage, "read_groups", side_effect=AssertionError("checkpoint reread")),
            patch.object(standalone, "_rows", side_effect=AssertionError("price reread")),
        ):
            self.assertTrue(maintain(self.store)["complete"])
        result = DataLibrary(store=self.store).write_csv_export(self.directory / "same-group-healthy")
        self.assertEqual(result["identities"], 1)
