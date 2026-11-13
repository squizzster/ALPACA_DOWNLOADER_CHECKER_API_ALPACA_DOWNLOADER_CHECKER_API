"""Selected producer transfer repair retains completed numerical approvals."""

import copy
import inspect
import unittest
from unittest.mock import patch

import test_alpaca_public_common_pipeline as fixture

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_lineage_build import membership_transfer_recovery, recovery_handoffs, storage
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download import check_summary
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.alpaca_serving_catalogue.producer import maintain


class TransferRecoveryTests(unittest.TestCase):
    def setUp(self):
        fixture.PublicCommonPipelineTests.setUp(self)
        self.assertEqual(
            0,
            fixture.install_completed_common(self)["failed"],
        )
        index = storage.read_index(self.root)
        before = {s: storage.read_receipt(self.root, s, index) for s in ("SPAC", "BUSINESS")}
        broken = copy.deepcopy(before)
        successor = broken["BUSINESS"]["membership_only_rebind"]
        self.assertIsNotNone(successor)
        broken["SPAC"]["membership_only_rebind"]["identities"].update(successor["identities"])
        broken["BUSINESS"]["membership_only_rebind"] = None
        for receipt in broken.values():
            receipt.pop("membership_transfer_handoff_version", None)
        with storage.writer_lock(self.store), storage.pricing_locks(self.store, set(broken)):
            storage.publish(self.store, self.root, index, broken, {s: {} for s in broken}, {}, previous=before)

    def _recover(self, context):
        from libs.alpaca_serving_catalogue import work

        with storage.writer_lock(self.store):
            return membership_transfer_recovery.recover(
                self.store,
                self.root,
                storage.read_index(self.root),
                work.read(self.store),
                context.select_serving(),
            )

    def test_completed_transfer_beside_explicit_negative_peer_does_not_reopen_old_checkpoint(self):
        index = {
            "symbols": {
                name: {
                    "sha256": name + "-receipt",
                    "source_sha256": name + "-new-pricing",
                    "group_members": ["POSITIVE", "NEGATIVE"],
                    "projection": {
                        "has_membership_rebind": name == "POSITIVE",
                        "membership_transfers_complete": name == "POSITIVE",
                    },
                }
                for name in ("POSITIVE", "NEGATIVE")
            }
        }
        pending = {name: entry["sha256"] for name, entry in index["symbols"].items()}
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reopened")),
            patch.object(serving_storage, "read_groups", side_effect=AssertionError("old checkpoint reopened")),
            patch.object(storage, "pricing_locks", side_effect=AssertionError("unselected pricing owner")),
            patch.object(storage, "publish", side_effect=AssertionError("completed transfer rewritten")),
        ):
            self.assertEqual([], membership_transfer_recovery.recover(self.store, self.root, index, pending, {}))

    def test_incomplete_positive_and_unknown_peers_retain_complete_group_recovery(self):
        for state in ("incomplete_positive", "unknown_peer"):
            index = {
                "symbols": {
                    name: {
                        "sha256": name,
                        "group_members": ["POSITIVE", "PEER"],
                        "projection": {
                            "has_membership_rebind": name == "POSITIVE",
                            "membership_transfers_complete": name == "POSITIVE",
                        },
                    }
                    for name in ("POSITIVE", "PEER")
                }
            }
            if state == "incomplete_positive":
                index["symbols"]["POSITIVE"]["projection"]["membership_transfers_complete"] = False
            else:
                index["symbols"]["PEER"]["projection"].pop("has_membership_rebind")
            with (
                self.subTest(state=state),
                patch.object(
                    membership_transfer_recovery, "_recover_group", return_value=["POSITIVE", "PEER"]
                ) as owning,
            ):
                self.assertEqual(
                    ["PEER", "POSITIVE"],
                    membership_transfer_recovery.recover(
                        self.store, self.root, index, {name: name for name in index["symbols"]}, {}
                    ),
                )
            owning.assert_called_once_with(self.store, self.root, index, {"POSITIVE", "PEER"}, {})

    def test_carried_metadata_reads_only_missing_groups_and_expires_with_binding(self):
        controls = serving_storage.controls(self.store)
        keys = sorted(controls["groups"])
        self.assertEqual(2, len(keys))
        generation = controls["state"]["generation"]
        first = serving_storage.read_groups(self.store, keys[:1], generation=generation)[keys[0]]
        queries = []
        connect = serving_storage.connect_sqlite

        def traced(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(queries.append)
            return connection

        with recovery_handoffs.operation(self.store):
            recovery_handoffs.remember(self.store, "serving_group", (generation, keys[0]), first)
            self.assertIsNone(
                recovery_handoffs.recall(self.store.parent / "other", "serving_group", (generation, keys[0]))
            )
            self.assertIsNone(recovery_handoffs.recall(self.store, "serving_group", ("changed", keys[0])))
            with patch.object(serving_storage, "connect_sqlite", side_effect=traced):
                selected = serving_storage.read_groups(self.store, keys, generation=generation)
            self.assertEqual(set(keys), set(selected))
            self.assertEqual(1, sum("SELECT body FROM groups" in query for query in queries))
            with self.assertRaisesRegex(CatalogueError, "selected serving catalogue changed"):
                serving_storage.read_groups(self.store, keys[:1], generation="changed")
            recovery_handoffs.remember(self.store, "serving_group", (generation, keys[1]), selected[keys[1]])
            replacement = copy.deepcopy(selected[keys[1]])
            replacement["decision_generation"] = "f" * 64
            revised = serving_storage.commit(self.store, keys[1:], [replacement])
            self.assertIsNone(recovery_handoffs.recall(self.store, "serving_group", (generation, keys[0])))
            self.assertIsNone(
                recovery_handoffs.recall(self.store, "serving_group", (revised["generation"], keys[1]))
            )
            with patch.object(
                serving_storage, "connect_sqlite", side_effect=AssertionError("unchanged peer reread")
            ):
                self.assertEqual(
                    {keys[0]: first},
                    serving_storage.read_groups(self.store, keys[:1], generation=revised["generation"]),
                )
        self.assertIsNone(recovery_handoffs.recall(self.store, "serving_group", (generation, keys[0])))

    def test_repair_publishes_original_bindings_and_serves_without_numerical_replay(self):
        context = OperationContext(self.store)
        connect = serving_storage.connect_sqlite

        def no_checkpoint_reread(*args, **kwargs):
            if any(frame.function == "read_groups" for frame in inspect.stack()):
                raise AssertionError("completed numerical checkpoint reread")
            return connect(*args, **kwargs)

        with (
            recovery_handoffs.operation(self.store),
            patch.object(check_summary, "invalidate", side_effect=AssertionError("accepted prices invalidated")),
        ):
            self.assertEqual(["BUSINESS", "SPAC"], self._recover(context))
            index = storage.read_index(self.root)
            for name in ("SPAC", "BUSINESS"):
                receipt = storage.read_receipt(self.root, name, index)
                transfer = receipt["membership_only_rebind"]
                self.assertEqual(2, transfer["version"])
                self.assertNotIn("previous_boundaries", transfer)
                self.assertEqual("accepted_serving_checkpoint", transfer["previous_selection"]["kind"])
                validate_handoff(receipt)
                malformed = copy.deepcopy(receipt)
                malformed["membership_only_rebind"]["previous_selection"].pop("generation")
                with self.assertRaises(LineageError):
                    validate_handoff(malformed)
            with (
                patch.object(policy, "_bar_reasons", side_effect=AssertionError("numerical approval replayed")),
                patch.object(
                    standalone, "_rows", side_effect=AssertionError("approved numerical history reopened")
                ),
                patch.object(serving_storage, "connect_sqlite", side_effect=no_checkpoint_reread),
            ):
                result = maintain(self.store, context=context)
            self.assertTrue(result["complete"], result)
            self.assertEqual(0, result["checked_bars"])
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_journal_recovery_keeps_completed_transfer_and_skips_reconstruction(self):
        context = OperationContext(self.store)
        with (
            recovery_handoffs.operation(self.store),
            patch.object(storage, "finish_publication", side_effect=RuntimeError("publication interrupted")),
        ):
            with self.assertRaisesRegex(RuntimeError, "publication interrupted"):
                self._recover(context)
        with storage.writer_lock(self.store):
            self.assertTrue(storage.recover(self.store, self.root))
        with (
            patch.object(
                serving_storage, "read_groups", side_effect=AssertionError("completed donors reopened")
            ),
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed transfer reread")),
        ):
            self.assertEqual([], self._recover(context))
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("numerical approval replayed")):
            self.assertTrue(maintain(self.store)["complete"])

    def test_missing_or_changed_choice_capability_selects_owner_without_inferring_selection(self):
        read_groups = serving_storage.read_groups
        for change in ("missing", "changed_generation"):
            context = OperationContext(self.store)

            def legacy(*args, **kwargs):
                groups = copy.deepcopy(read_groups(*args, **kwargs))
                for group in groups.values():
                    history = group["history_validation"]
                    if change == "missing":
                        history.pop("observation_choice_handoffs", None)
                    else:
                        for source in history["observation_choice_handoffs"]["sources"].values():
                            source["pricing_generation"] = "f" * 64
                return groups

            with (
                self.subTest(change=change),
                patch.object(serving_storage, "read_groups", side_effect=legacy),
                patch.object(storage, "publish", side_effect=AssertionError("unknown choice published")),
                patch.object(
                    standalone, "_rows", side_effect=AssertionError("history opened for missing choice")
                ),
            ):
                with self.assertRaisesRegex(LineageError, "explicit accepted numerical choices"):
                    self._recover(context)
