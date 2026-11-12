"""Final selected contributions, independently of closed/materialized work paths."""

import copy
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations
from test_alpaca_public_common_continuation import common_fact

from alpaca_end_of_day import standalone
from libs.alpaca_lineage_build import (
    membership_transfer,
    membership_transfer_recovery,
    pipeline,
    source_observations,
    source_work,
    storage,
)
from libs.alpaca_lineage_build.assignment_handoff import produce
from libs.alpaca_lineage_build.checkpoint_choices import select as checkpoint_choices
from libs.alpaca_lineage_build.contract import LineageError, document_digest
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.producer import maintain


class MembershipTransferTests(unittest.TestCase):
    def test_materialized_successor_retains_donor_and_empty_alias_does_not_advertise(self):
        self._mixed_case(pinned=False)

    def test_materialized_pinned_source_composes_only_accepted_selection(self):
        self._mixed_case(pinned=True)

    def test_owning_recovery_restores_complete_accepted_selection(self):
        self._mixed_case(pinned=False, recover=True)

    def test_uncovered_newly_owned_dates_reach_ordinary_selection(self):
        segment = {
            "adjustment": "raw",
            "row_start": 1,
            "row_end": 2,
            "first_timestamp": "2024-01-02T05:00:00Z",
            "last_timestamp": "2024-01-03T05:00:00Z",
            "security_identity": {"security_id": "joined"},
            "historical_symbol": "NEW",
        }
        report = {"correlated": {"row_count": 2, "segments": [segment]}}
        ownership = {"views": {"raw": {"ownership": [{"rows": 2, "owner": [None, None]}]}}}
        retained = {"NEW": (ownership, {"old": {"security_id": "joined"}})}
        with patch.object(
            source_observations, "select_observations", return_value=({"NEW": report}, {})
        ) as choose:
            result, _, _ = source_observations.reconcile(
                None, {"NEW": object()}, {"NEW": report}, {}, {}, {}, retained=retained
            )
        self.assertEqual(1, choose.call_count)
        self.assertEqual([segment], result["NEW"]["correlated"]["segments"])

    def _mixed_case(self, *, pinned, recover=False):
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            ),
        ):
            store = Path(temporary) / "store"
            root = store / "end_of_day/lineage"
            names = ("SPAC", "ALIAS", "BUSINESS")
            assets = {
                "SPAC": asset("SPAC", "predecessor-id"),
                "BUSINESS": asset("BUSINESS", "successor-id"),
                "ALIAS": None,
            }
            assets.update({"111111111": assets["SPAC"], "222222222": assets["BUSINESS"]})
            action = {
                "id": "later-label",
                "action_type": "name_change",
                "old_symbol": "ALIAS",
                "new_symbol": "BUSINESS",
                "old_cusip": "222222222",
                "new_cusip": "222222222",
                "process_date": "2024-01-08",
            }
            full = informative_bars("2024-01-02", "2024-01-09")
            rows = {"SPAC": full[:3], "ALIAS": full[3:4], "BUSINESS": full}
            for name in names:
                with direct_adjustment_observations():
                    download_pricing(
                        store,
                        name,
                        "2024-01-09",
                        from_date="2024-01-01",
                        client=FakeClient(rows[name]),
                        mapping_asof="2024-01-08" if pinned and name == "BUSINESS" else None,
                        refresh_all=pinned and name == "BUSINESS",
                    )
            result = pipeline.run_lineage(store, names, identity_client=IdentityClient(assets, [action]))
            self.assertEqual(0, result["failed"], result)
            before_index = storage.read_index(root)
            before = {name: storage.read_receipt(root, name, before_index) for name in names}
            self.assertGreater(before["BUSINESS"]["unresolved_rows"], 0)
            self.assertTrue(
                all(
                    run["owner"][0] is None
                    for view in before["ALIAS"]["serving_ownership"]["views"].values()
                    for run in view["ownership"]
                )
            )
            self.assertEqual(0, maintain(store, bootstrap=True)["failed"])
            accepted_serving = serving_storage.controls(store)
            accepted_groups = serving_storage.read_groups(
                store,
                accepted_serving["groups"],
                generation=accepted_serving["state"]["generation"],
            )
            donor_checkpoint = next(
                group for group in accepted_groups.values() if "SPAC" in group["history_validation"]["inputs"]
            )
            business_checkpoint = next(
                group
                for group in accepted_groups.values()
                if "BUSINESS" in group["history_validation"]["inputs"]
            )
            fact = common_fact()
            fact["facts"]["successor"]["symbol"] = "ALIAS"
            plans = []
            prepare = source_work.prepare_group_evidence_and_source_work

            def capture(*args, **kwargs):
                plan = prepare(*args, **kwargs)
                plans.append(plan.dispositions)
                return plan

            with (
                patch.object(source_work, "prepare_group_evidence_and_source_work", side_effect=capture),
                patch.object(pipeline, "prepare_group_evidence_and_source_work", side_effect=capture),
                patch.object(
                    source_observations,
                    "select_observations",
                    side_effect=AssertionError("accepted selection replayed"),
                ),
                patch.object(
                    source_observations,
                    "counterpart",
                    side_effect=AssertionError("accepted counterpart prices read"),
                ),
            ):
                result = pipeline.run_lineage(
                    store, names, identity_client=IdentityClient(assets, [action]), event_supplements=[fact]
                )
            self.assertEqual(0, result["failed"], result)
            self.assertTrue(any(plan["ALIAS"] == "A" and plan["BUSINESS"] == "B" for plan in plans), plans)
            after = {name: storage.read_receipt(root, name, storage.read_index(root)) for name in names}
            self.assertIsNone(after["ALIAS"]["membership_only_rebind"])
            transfer = after["BUSINESS"]["membership_only_rebind"]
            self.assertIsNotNone(transfer)
            self.assertEqual(
                before_index["symbols"]["BUSINESS"]["sha256"], transfer["previous_receipt_generation"]
            )
            self.assertEqual(before["BUSINESS"]["source_sha256"], transfer["previous_pricing_generation"])
            self.assertEqual(16, sum(run["row_end"] - run["row_start"] + 1 for run in transfer["runs"]))
            if recover:
                self._damage_and_recover(store, root, names, before_index, before, after)
            with (
                patch.object(
                    policy, "_bar_reasons", side_effect=AssertionError("accepted numerical checks replayed")
                ),
                patch.object(
                    standalone, "_rows", side_effect=AssertionError("accepted numerical history replayed")
                ),
            ):
                served = maintain(store)
            self.assertEqual(0, served["checked_bars"])
            self.assertTrue(served["complete"], served)
            if recover:
                self.assertEqual(1, served["reused_history_prefixes"])
                self.assertEqual(0, served["full_history_evaluations"])
                return

            arguments = dict(
                source="BUSINESS",
                old_identity_memberships=before["BUSINESS"]["identity_memberships"],
                old_serving_ownership=before["BUSINESS"]["serving_ownership"],
                old_receipt_generation=transfer["previous_receipt_generation"],
                old_pricing_generation=transfer["previous_pricing_generation"],
                old_boundaries=transfer["previous_boundaries"],
                current_receipt=after["BUSINESS"],
                current_documents=source_work.assignment_documents(root, after["BUSINESS"], {}),
                preserves_pricing=True,
            )
            self.assertEqual(transfer, membership_transfer.build(**arguments))
            preserved = dict(
                source="BUSINESS",
                old_serving_ownership=before["BUSINESS"]["serving_ownership"],
                accepted_identity_changes=transfer["identities"],
                current_receipt=after["BUSINESS"],
                current_documents=lambda: self.fail("unchanged selection reopened assignments"),
                preserves_pricing=True,
            )
            self.assertEqual({}, source_observations.preserve_selection(**preserved))
            with self.assertRaisesRegex(LineageError, "changed pricing generation"):
                source_observations.preserve_selection(**{**preserved, "preserves_pricing": False})
            corrupted = copy.deepcopy(arguments["current_documents"])
            for kind, document in corrupted.items():
                for entry in document["adjustments"]:
                    for segment in entry["segments"]:
                        if segment["branch"] != "correlated":
                            continue
                        if kind == "current_year" and segment["from"][:7] >= after["BUSINESS"]["anchor_month"]:
                            continue
                        segment["observation_selection"] = "selected"
            damaged_receipt = {**after["BUSINESS"], "serving_ownership": None}
            damaged_receipt.pop("membership_transfer_handoff_version", None)
            repair_args = {
                **preserved,
                "current_receipt": damaged_receipt,
                "current_documents": lambda: corrupted,
                "accepted_choices": checkpoint_choices(business_checkpoint, "BUSINESS"),
                "accepted_donor_scopes": source_observations.selected_donor_scopes(
                    "SPAC",
                    old_serving_ownership=before["SPAC"]["serving_ownership"],
                    accepted_identity_changes=after["SPAC"]["membership_only_rebind"]["identities"],
                    current_receipt=after["SPAC"],
                    current_documents=lambda: source_work.assignment_documents(root, after["SPAC"], {}),
                    accepted_checkpoint=donor_checkpoint,
                    accepted_choices=checkpoint_choices(donor_checkpoint, "SPAC"),
                ),
            }
            repaired = source_observations.preserve_selection(**repair_args)
            self.assertEqual(arguments["current_documents"], {**corrupted, **repaired})
            if not pinned:
                for coverage in (
                    [],
                    [{**item, "rows": item["rows"] - 1} for item in repair_args["accepted_donor_scopes"]],
                    [{**item, "complete_grid": False} for item in repair_args["accepted_donor_scopes"]],
                ):
                    with (
                        self.subTest(missing_or_incompatible_donor=coverage),
                        self.assertRaisesRegex(LineageError, "lack accepted donor coverage"),
                    ):
                        source_observations.preserve_selection(
                            **{**repair_args, "accepted_donor_scopes": coverage}
                        )
                with self.assertRaisesRegex(LineageError, "lack accepted donor coverage"):
                    source_observations.preserve_selection(
                        **{
                            **repair_args,
                            "current_receipt": {**damaged_receipt, "public_common_continuation_ownership": None},
                        }
                    )
                for field, value in (
                    ("missing_sessions", ["2024-01-03"]),
                    ("price_reasons", ["incompatible_sessions"]),
                ):
                    invalid_checkpoint = copy.deepcopy(donor_checkpoint)
                    for record in invalid_checkpoint["history_validation"]["prefix_records"]:
                        record[field] = value
                    self.assertEqual(
                        set(),
                        source_observations.complete_donor_ids(
                            "SPAC",
                            before["SPAC"]["serving_ownership"],
                            before["SPAC"]["source_sha256"],
                            invalid_checkpoint,
                        ),
                    )
            for field in ("owner", "position"):
                with self.subTest(recovery_rejects=field):
                    invalid = copy.deepcopy(corrupted)
                    for document in invalid.values():
                        for entry in document["adjustments"]:
                            for segment in entry["segments"]:
                                if segment["branch"] != "correlated":
                                    continue
                                if field == "owner":
                                    segment["security_identity"]["security_id"] = "unrelated"
                                else:
                                    segment["row_start"] += 1
                                    segment["row_end"] += 1
                    with self.assertRaises(LineageError):
                        source_observations.preserve_selection(**{**repair_args, "current_documents": invalid})
            self.assertEqual(
                transfer,
                membership_transfer.build(
                    **{
                        **arguments,
                        "old_identity_memberships": [],
                        "accepted_identity_changes": transfer["identities"],
                    }
                ),
            )
            changed = {
                **arguments,
                "preserves_pricing": False,
                "current_documents": lambda: self.fail("changed generation opened assignments"),
            }
            self.assertIsNone(membership_transfer.build(**changed))
            for field in ("label", "position"):
                with self.subTest(changed=field):
                    changed = copy.deepcopy(arguments)
                    for document in changed["current_documents"].values():
                        for entry in document["adjustments"]:
                            for segment in entry["segments"]:
                                if field == "label":
                                    segment["observation_tickers"] = ["UNRELATED"]
                                else:
                                    segment["row_start"] += 100
                                    segment["row_end"] += 100
                    self.assertIsNone(membership_transfer.build(**changed))

    def _damage_and_recover(self, store, root, names, before_index, before, after):
        bad = copy.deepcopy(after)
        changed = {name: {} for name in names}
        for name in ("SPAC", "BUSINESS"):
            for kind, original in source_work.assignment_documents(root, after[name], {}).items():
                document = copy.deepcopy(original)
                for entry in document["adjustments"]:
                    for segment in entry["segments"]:
                        if segment["branch"] == "correlated":
                            segment["observation_selection"] = "superseded" if name == "SPAC" else "selected"
                if document != original:
                    generation = document_digest(document)
                    changed[name][kind] = document
                    bad[name]["boundaries"][kind]["sha256"] = generation
                    bad[name]["assignment_handoffs"][kind] = produce(document, generation)
        phantom = copy.deepcopy(after["BUSINESS"]["membership_only_rebind"])
        phantom.update(
            source="ALIAS",
            previous_receipt_generation=before_index["symbols"]["ALIAS"]["sha256"],
            previous_pricing_generation=before["ALIAS"]["source_sha256"],
            pricing_generation=after["ALIAS"]["source_sha256"],
            previous_boundaries=before["ALIAS"]["boundaries"],
        )
        phantom["runs"] = [
            {**run, "source": "ALIAS", "row_start": 1, "row_end": 1}
            for run in phantom["runs"]
            if run["labels"] == ["ALIAS"]
        ]
        bad["ALIAS"]["membership_only_rebind"] = phantom
        bad["BUSINESS"]["membership_only_rebind"] = None
        for receipt in bad.values():
            receipt.pop("membership_transfer_handoff_version", None)
        index = storage.read_index(root)
        storage.publish(store, root, index, bad, changed, {}, previous=after)
        serving = serving_storage.controls(store)
        with (
            patch.object(
                source_observations,
                "select_observations",
                side_effect=AssertionError("recovery replayed observations"),
            ),
            patch.object(
                source_observations,
                "counterpart",
                side_effect=AssertionError("recovery read counterpart prices"),
            ),
            patch.object(standalone, "_rows", side_effect=AssertionError("recovery read numerical history")),
            patch.object(
                policy, "_bar_reasons", side_effect=AssertionError("recovery replayed numerical checks")
            ),
        ):
            repaired = membership_transfer_recovery._recover_group(store, root, index, set(names), serving)
        self.assertEqual(sorted(names), repaired)
        recovered = {name: storage.read_receipt(root, name, index) for name in names}
        self.assertIsNone(recovered["ALIAS"]["membership_only_rebind"])
        for name in names:
            self.assertEqual(after[name]["serving_ownership"], recovered[name]["serving_ownership"])
        for name in ("SPAC", "BUSINESS"):
            transfer = recovered[name]["membership_only_rebind"]
            self.assertEqual(2, transfer["version"])
            self.assertEqual(before_index["symbols"][name]["sha256"], transfer["previous_receipt_generation"])
            self.assertEqual(before[name]["source_sha256"], transfer["previous_pricing_generation"])
