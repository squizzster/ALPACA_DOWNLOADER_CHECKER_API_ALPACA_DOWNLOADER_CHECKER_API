"""Recomputed lineage changes only the serving work its new assignments require."""

import json
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, PublicIdUnavailableError, standalone
from libs.alpaca_end_of_day_security_correlation.security_identity import (
    security_identity_for_alpaca_assets,
)
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.rebuild import rebuild
from libs.alpaca_serving_catalogue import policy, producer, storage, work
from libs.common.sqlite_connections import connect_sqlite


class LineageRebuildServingTests(NativeCase):
    def identity_split(self):
        """An accepted two-UUID identity accidentally narrows for the final row."""
        identities = {}

        def split(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    selected = []
                    for segment in view["segments"]:
                        narrow = segment["security_identity"]
                        broad = security_identity_for_alpaca_assets(
                            [*narrow["member_alpaca_asset_ids"], "accepted-predecessor-uuid"],
                            [],
                            tickers=["TEST"],
                            basis="accepted_continuous_membership",
                        ).as_report_object()
                        identities.update(narrow=narrow["security_id"], broad=broad["security_id"])
                        prefix = deepcopy(segment)
                        prefix.update(
                            row_end=segment["row_end"] - 1,
                            row_count=segment["row_count"] - 1,
                            to=self.bars[-2]["t"][:10],
                            security_identity=broad,
                        )
                        final = deepcopy(segment)
                        final.update(
                            row_start=segment["row_end"],
                            row_count=1,
                            **{"from": self.bars[-1]["t"][:10]},
                        )
                        selected.extend((prefix, final))
                    view["segments"] = selected

        self.publish_change(split)
        return identities

    def repair_membership(self):
        def merge(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if len(view["segments"]) != 2:
                        continue
                    prefix, final = view["segments"]
                    prefix.update(
                        row_end=final["row_end"],
                        row_count=prefix["row_count"] + final["row_count"],
                        to=final["to"],
                    )
                    view["segments"] = [prefix]

        self.publish_change(merge)

    def test_merge_preserves_earlier_identity_approval_and_rechecks_only_new_final_row(self):
        identities = self.identity_split()
        before = self.bootstrap()
        self.assertEqual({item.public_id for item in before.list_securities()}, set(identities.values()))
        self.repair_membership()
        check = policy._bar_reasons

        def only_new_row(bar, adjustment):
            self.assertEqual(bar["session"], self.bars[-1]["t"][:10], "accepted numerical prefix replayed")
            return check(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=only_new_row):
            result = producer.maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 8)
        self.assertEqual(result["full_history_evaluations"], 0)
        self.assertEqual(result["reused_history_prefixes"], 1)
        after = DataLibrary(store=self.store)
        self.assertEqual([item.public_id for item in after.list_securities()], [identities["broad"]])
        self.assertEqual(len(after.get_history(identities["broad"]).bars), len(self.bars))
        with self.assertRaises(PublicIdUnavailableError):
            after.get_security(identities["narrow"])

    def test_republished_unchanged_assignments_keep_all_numerical_approvals(self):
        data = self.bootstrap()
        self.publish_change(lambda receipt, _: receipt.update(recomputed_from_retained_inputs=True))
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted prices reread")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted prices rechecked")),
        ):
            result = producer.maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(
            [item.public_id for item in data.list_securities()],
            [item.public_id for item in DataLibrary(store=self.store).list_securities()],
        )

    def test_changed_earlier_labels_still_invalidate_the_affected_identity_prefix(self):
        self.identity_split()
        self.bootstrap()
        self.repair_membership()

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_tickers"] = ["NEW", "TEST"]

        self.publish_change(relabel)
        result = producer.maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reused_history_prefixes"], 0)
        self.assertEqual(result["full_history_evaluations"], 1)
        self.assertEqual(result["checked_bars"], 8 * len(self.bars))

    def test_interrupted_merge_acknowledgement_reuses_the_completed_serving_decision(self):
        identities = self.identity_split()
        self.bootstrap()
        self.repair_membership()

        class Interrupted(BaseException):
            pass

        with patch.object(work, "acknowledge", side_effect=Interrupted), self.assertRaises(Interrupted):
            producer.maintain(self.store, rebuild=True)
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("completed merge replayed")),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted prices reread")),
        ):
            retry = producer.maintain(self.store, rebuild=True)
        self.assertTrue(retry["complete"], retry)
        self.assertEqual(retry["evaluated_groups"], 0)
        self.assertEqual(retry["checked_bars"], 0)
        self.assertEqual(
            [item.public_id for item in DataLibrary(store=self.store).list_securities()], [identities["broad"]]
        )

    def test_retained_universe_rebuild_upgrades_legacy_exports_without_price_rechecks(self):
        """The real broad repair consumes old computation and serving contracts."""
        self.bootstrap()
        universe_state.initialize(self.store)
        index = lineage.read_index(self.root)
        receipt = lineage.read_receipt(self.root, "TEST", index)
        receipt.pop("computation_revision")
        entry = index["symbols"]["TEST"]
        entry.pop("computation_revision")
        entry["projection"].pop("computation_revision")
        entry["sha256"] = lineage.document_digest(receipt)
        with lineage.publication_lock(self.root, exclusive=True):
            lineage.atomic_json(self.root / "TEST/lineage.manifest.json", receipt)
            lineage._update_index(self.root, {"TEST": entry})
        with closing(connect_sqlite(storage.path(self.store))) as connection, connection:
            for table in ("export_inputs", "export_input_state", "export_input_bindings"):
                connection.execute(f"DROP TABLE {table}")
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted prices rechecked")):
            result = rebuild(self.store, all_published=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["remaining_symbols"], [])
        self.assertEqual(result["provider_requests"], 0)
        self.assertEqual(result["pricing_writes"], 0)
        self.assertEqual(result["serving_catalogue"]["checked_bars"], 0)
        exported = DataLibrary(store=self.store).write_csv_export(self.directory / "rebuilt-exports")
        self.assertEqual(json.loads(Path(exported["manifest"]).read_text())["counts"]["identities"], 1)
