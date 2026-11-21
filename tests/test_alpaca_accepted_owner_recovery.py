"""Exercise scoped restoration of accepted same-source rows without price replay."""

from copy import deepcopy
from dataclasses import replace
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_lineage_build import accepted_owner_recovery as owner
from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.retained_recovery import (
    RECOVERY_BOUNDARY,
    RESTORE_ACCEPTED_OWNER,
    accept_selection,
    recover,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.lineage import symbol_cusip_evidence
from libs.alpaca_serving_catalogue.producer import maintain


class AcceptedOwnerRecoveryTests(NativeCase):
    def lose_prefix(self):
        symbol = "FUTURE"
        bars = valid_bars("2025-12-22", "2026-01-08")
        self.add_symbol(symbol, bars)
        library = self.bootstrap()
        pid = library.resolve_ticker(symbol)[0].public_id
        initial = library.get_histories(pid)

        def lose(receipt, boundaries):
            body = boundaries[owner.COLD]
            for view in body["adjustments"]:
                original = view["segments"][0]
                lost = {
                    **original,
                    "branch": "uncorrelated",
                    "row_end": 2,
                    "row_count": 2,
                    "to": bars[1]["t"][:10],
                    "last_timestamp": bars[1]["t"],
                    "candidate_securities": [],
                    "reason_codes": [owner.LOSS],
                }
                lost.pop("security_identity")
                retained = {
                    **original,
                    "row_start": 3,
                    "row_count": original["row_count"] - 2,
                    "from": bars[2]["t"][:10],
                    "first_timestamp": bars[2]["t"],
                }
                view["segments"] = [lost, retained, *view["segments"][1:]]
            receipt["assignment_handoffs"].pop(owner.COLD)
            receipt["correlated_rows"] -= 2 * len(ADJUSTMENTS)
            receipt["unresolved_rows"] += 2 * len(ADJUSTMENTS)
            receipt["identity_intervals"] = [
                {**interval, "from": max(interval["from"], bars[2]["t"][:10])}
                for interval in receipt["identity_intervals"]
            ]
            evidence = receipt["evidence"]
            cusip = "111111111"
            lookup = next(item for item in evidence["asset_lookups"] if item["lookup_kind"] == "symbol")
            evidence["asset_lookups"].append({**lookup, "lookup_kind": "cusip", "lookup": cusip})
            evidence["corporate_actions"] = [
                {
                    "action_type": "name_change",
                    "id": "retained-name-change",
                    "process_date": bars[2]["t"][:10],
                    "old_symbol": "1FUTURE",
                    "new_symbol": symbol,
                    "old_cusip": cusip,
                    "new_cusip": cusip,
                }
            ]
            evidence["symbol_occupancy_evidence"] = [
                {
                    "symbol": item["symbol"],
                    "cusip": item["cusip"],
                    "asset_id": lookup["asset"]["id"],
                    "evidence": [item],
                }
                for item in symbol_cusip_evidence(evidence["corporate_actions"])
            ]

        self.publish_change(lose, symbol)
        self.assertTrue(maintain(self.store)["complete"])
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(symbol), ())
        index = storage.read_index(self.root)
        entry = index["symbols"][symbol]
        policy = OperationContext(self.store).accept_correlation_policy().semantic_sha256
        selection = accept_selection(
            {
                "schema_version": 1,
                "boundary": RECOVERY_BOUNDARY,
                "mode": RESTORE_ACCEPTED_OWNER,
                "store": str(self.store),
                "correlation_policy_sha256": policy,
                "groups": [
                    {
                        "members": [symbol],
                        "pricing_generations": {symbol: entry["source_sha256"]},
                        "receipt_generations": {symbol: entry["sha256"]},
                    }
                ],
            },
            self.store,
        )
        universe_state.initialize(self.store)
        return symbol, pid, selection, initial, index

    def test_recover_borrows_approved_history_and_preserves_independent_generations(self):
        symbol, pid, selection, expected, index = self.lose_prefix()
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("ownership replayed")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("retained pricing reloaded")),
            patch.object(standalone, "_rows", side_effect=AssertionError("approved prices reopened")),
        ):
            result = recover(self.store, None, selection=selection)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["published_prepared_groups"], 1)
        served = result["serving_catalogue"]
        self.assertEqual((served["reused_history_prefixes"], served["checked_bars"]), (1, 0))
        after = storage.read_index(self.root)
        self.assertEqual(after["symbols"]["TEST"], index["symbols"]["TEST"])
        receipt = storage.read_receipt(self.root, symbol, after)
        self.assertEqual(receipt["retained_owner_restoration"]["row_end"], 2)
        self.assertEqual(receipt["unresolved_rows"], 0)
        actual = DataLibrary(store=self.store).get_histories(pid)
        for view in ADJUSTMENTS:
            self.assertEqual(actual[view].bars, expected[view].bars)

    def test_retry_installs_the_same_completed_journal_without_producer_replay(self):
        _, _, selection, _, _ = self.lose_prefix()
        with (
            patch.object(storage, "_install", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            recover(self.store, None, selection=selection)
        with (
            patch.object(owner, "_restore_prefix", side_effect=AssertionError("completed owner recomputed")),
            patch.object(pipeline, "_current_sources", side_effect=AssertionError("completed source reread")),
            patch.object(standalone, "_rows", side_effect=AssertionError("approved prices reopened")),
        ):
            repeated = recover(self.store, None, selection=selection)
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(repeated["previously_completed_groups"], 1)
        self.assertEqual(repeated["serving_catalogue"]["checked_bars"], 0)

    def test_changed_pricing_ancestry_and_competing_grid_reject(self):
        symbol, pid, selection, _, _ = self.lose_prefix()
        offline = self.catalogue_groups(symbol)[symbol]
        group = selection.groups[0]
        checkpoint = offline["offline_ownership"]["checkpoints"][0]
        old = group.pricing[symbol]
        new = "a" * 64
        group = replace(group, pricing={symbol: new})
        offline["references"]["pricing"][symbol] = new
        for loss in offline["offline_ownership"]["outcome"]["lost_scopes"]:
            loss["pricing_generation"] = new
        record = checkpoint["history_validation"]["prefix_records"][0]
        changed = {"history_changes": [{"source_sha256": old, "changed_from": record["validated_through"]}]}
        with self.assertRaisesRegex(LineageError, "selected pricing ancestry"):
            owner._checkpoint(group, offline, changed)
        changed["history_changes"][0]["changed_from"] = None
        owner._checkpoint(group, offline, changed)
        rival = deepcopy(offline)
        rival["offline_ownership"]["checkpoints"][0]["history_validation"]["identities"][pid]["all"][
            "sources"
        ].append("RIVAL")
        with self.assertRaisesRegex(LineageError, "clean all-view source and label grid"):
            owner._checkpoint(group, rival, changed)

    def test_independent_retired_label_occupancy_rejects(self):
        symbol, pid, _, _, index = self.lose_prefix()
        previous = storage.read_receipt(self.root, symbol, index)
        expected_owner = [pid, [symbol]]
        size = previous["serving_ownership"]["views"]["raw"]["rows"]
        owner._require_current_owner(previous, pid, expected_owner, 2, size)
        old = next(
            item for item in previous["evidence"]["symbol_occupancy_evidence"] if item["symbol"] != symbol
        )
        old["evidence"].append({**old["evidence"][0], "action_type": "cash_dividend", "action_id": "old-tape"})
        with self.assertRaisesRegex(LineageError, "retired label has evidence"):
            owner._require_current_owner(previous, pid, expected_owner, 2, size)
