"""Recover a closed native rename's accepted successor positions and numerical checkpoint."""

import unittest
from collections import Counter
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from dataclasses import replace
from unittest.mock import patch

import test_alpaca_lineage_build as fixtures
from test_alpaca_end_of_day_api import valid_bars

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_end_of_day_security_correlation import name_change
from libs.alpaca_lineage_build import pipeline, pricing, retained_rename_seam, storage, universe_state
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.retained_recovery import (
    RECOVERY_BOUNDARY,
    RESTORE_ACCEPTED_OWNER,
    accept_selection,
    recover,
)
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_serving_catalogue import policy, producer
from libs.alpaca_serving_catalogue import storage as catalogue

OLD, MIDDLE, TERMINAL = "WILLET", "PETREL", "QUARRY"
NAMES = (OLD, MIDDLE, TERMINAL)
CUSIP = "830000001"


class RetainedRenameSeamTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.store = self.case.store

    def lose_group(self, *, earlier=None):
        before = valid_bars("2024-02-01", "2024-02-12")
        after = valid_bars("2024-02-12", "2024-02-26")
        leading = valid_bars("2024-01-24", "2024-02-01") if earlier else []
        self.names = (*NAMES, earlier) if earlier else NAMES
        if earlier:
            self.case.download(earlier, end="2025-03-01", bars=leading)
        for symbol, bars in ((OLD, before), (MIDDLE, after), (TERMINAL, leading + before + after)):
            self.case.download(symbol, end="2025-03-01", bars=bars)
        terminal = {
            **fixtures.asset(TERMINAL, "native-issue"),
            "status": "inactive",
            "exchange": "OTC",
            "tradable": False,
        }
        retired_lookup = {**fixtures.asset(OLD, "other-inactive-id"), "status": "inactive", "tradable": False}
        actions = [
            {
                "id": identifier,
                "action_type": "name_change",
                "old_symbol": old,
                "new_symbol": new,
                "old_cusip": CUSIP,
                "new_cusip": CUSIP,
                "process_date": day,
            }
            for old, new, identifier, day in (
                (OLD, MIDDLE, "arrival", "2024-02-12"),
                (MIDDLE, TERMINAL, "departure", "2024-02-26"),
            )
        ]
        if earlier:
            actions.append(
                {
                    **actions[0],
                    "id": "earlier-entry",
                    "old_symbol": earlier,
                    "new_symbol": OLD,
                    "process_date": "2024-02-01",
                }
            )
        self.client = fixtures.IdentityClient(
            {TERMINAL: terminal, CUSIP: terminal, OLD: retired_lookup}, actions
        )
        self.case.run_build(symbols=[MIDDLE], client=self.client)
        self.assertTrue(producer.maintain(self.store, bootstrap=True)["complete"])
        library = DataLibrary(store=self.store)
        self.pid = library.resolve_ticker(MIDDLE)[0].public_id
        self.approved = library.get_histories(self.pid)
        # Reproduce the earlier correlation policy that lost the first three
        # observations after the provider's actual process-date field changed.
        self.client.actions[0]["process_date"] = "2024-02-15"
        with patch.object(name_change, "_closed_successor_source_bridge_history", return_value=None):
            self.case.run_build(symbols=[MIDDLE], client=self.client, refresh=True)
        outcome = producer.maintain(self.store)
        self.assertEqual(outcome["checked_bars"], 0)
        self.assertEqual(outcome["offline_reviews"][0]["public_ids"], [self.pid])
        self.index = storage.read_index(self.case.root)
        self.selection = accept_selection(
            {
                "schema_version": 1,
                "boundary": RECOVERY_BOUNDARY,
                "mode": RESTORE_ACCEPTED_OWNER,
                "store": str(self.store),
                "correlation_policy_sha256": OperationContext(self.store)
                .accept_correlation_policy()
                .semantic_sha256,
                "groups": [
                    {
                        "members": sorted(self.names),
                        "pricing_generations": {
                            name: self.index["symbols"][name]["source_sha256"] for name in self.names
                        },
                        "receipt_generations": {
                            name: self.index["symbols"][name]["sha256"] for name in self.names
                        },
                    }
                ],
            },
            self.store,
        )
        universe_state.initialize(self.store)

    @contextmanager
    def forbid_history_work(self):
        with ExitStack() as stack:
            for module, function in (
                (pipeline, "load_pricing"),
                (pipeline, "download_pricing"),
                (pipeline, "correlate"),
                (pipeline, "acquire"),
                (pipeline, "acquire_unpriced"),
                (pricing, "load_published_boundary"),
                (pricing_storage, "read_validator"),
                (pricing_storage, "publish"),
                (standalone, "_rows"),
                (policy, "_bar_reasons"),
                (self.client, "get_asset_lookup"),
                (self.client, "get_corporate_actions"),
            ):
                stack.enter_context(patch.object(module, function, side_effect=AssertionError(function)))
            yield

    def test_complete_group_recovers_exact_sources_and_all_price_views(self):
        self.lose_group()
        reads = Counter()
        original = storage.read_assignment_projection

        def count(root, receipt, kind, handoff, *, selected_loader=None):
            def read(path, digest):
                key = receipt["symbol"], kind
                reads[key] += 1
                self.assertEqual(reads[key], 1, key)
                return (selected_loader or storage.selected_document)(path, digest)

            return original(root, receipt, kind, handoff, selected_loader=read)

        with self.forbid_history_work(), patch.object(storage, "read_assignment_projection", side_effect=count):
            result = recover(self.store, None, selection=self.selection)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["published_prepared_groups"], 1)
        self.assertEqual(result["serving_catalogue"]["checked_bars"], 0)
        self.assertEqual(result["serving_catalogue"]["reused_history_prefixes"], 1)
        self.assertEqual(result["serving_catalogue"]["offline_reviews"], [])
        self.assertEqual(reads[TERMINAL, "completed_years"], 1)
        self.assertEqual(reads[MIDDLE, "completed_years"], 1)
        library = DataLibrary(store=self.store)
        self.assertEqual(library.resolve_ticker(OLD)[0].public_id, self.pid)
        self.assertEqual(library.resolve_ticker(MIDDLE)[0].public_id, self.pid)
        histories = library.get_histories(self.pid)
        for adjustment in ADJUSTMENTS:
            self.assertEqual(histories[adjustment].bars, self.approved[adjustment].bars)
        receipt = self.case.receipt(MIDDLE)
        self.assertEqual(receipt["unresolved_rows"], 0)
        self.assertEqual(
            receipt["retained_owner_restoration"]["supporting_corporate_action_ids"], ["arrival", "departure"]
        )
        for adjustment in ADJUSTMENTS:
            first, after = self.case.assignments(MIDDLE, adjustment)
            self.assertEqual((first["from"], first["to"]), ("2024-02-12", "2024-02-14"))
            self.assertEqual((first["historical_symbol"], first["observation_selection"]), (MIDDLE, "selected"))
            interval = first["ticker_ownership_interval"]
            self.assertEqual(
                (interval["cusip"], interval["asset_id"], interval["end_date_exclusive"]),
                (CUSIP, "native-issue", "2024-02-15"),
            )
            self.assertEqual(after["from"], "2024-02-15")
        after = storage.read_index(self.case.root)
        for name in NAMES:
            self.assertEqual(
                after["symbols"][name]["source_sha256"], self.index["symbols"][name]["source_sha256"]
            )
            self.assertEqual(after["symbols"][name]["group_members"], sorted(NAMES))

    def test_interrupt_after_preparation_recovers_once_and_unchanged_retry_reads_no_inputs(self):
        self.lose_group()
        with (
            self.forbid_history_work(),
            patch.object(storage, "_install", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            recover(self.store, None, selection=self.selection)
        with (
            self.forbid_history_work(),
            patch.object(retained_rename_seam, "prepare", side_effect=AssertionError("replayed restoration")),
            patch.object(pipeline, "_current_sources", side_effect=AssertionError("reopened sources")),
        ):
            recovered = recover(self.store, None, selection=self.selection)
            repeated = recover(self.store, None, selection=self.selection)
        for outcome in (recovered, repeated):
            self.assertTrue(outcome["complete"], outcome)
            self.assertEqual(outcome["previously_completed_groups"], 1)
            self.assertEqual(outcome["serving_catalogue"]["checked_bars"], 0)

    def test_only_the_exact_unchanged_grid_and_native_rename_can_restore(self):
        self.lose_group()
        group = self.selection.groups[0]
        generation = catalogue.selector_state(self.store)["generation"]
        offline = catalogue.read_groups(self.store, [min(NAMES)], generation=generation)[min(NAMES)]
        previous = {name: storage.read_receipt(self.case.root, name, self.index) for name in NAMES}

        def competing_choice(receipts, _):
            receipts[TERMINAL]["serving_ownership"]["views"]["all"]["ownership"][0]["owner"] = [
                self.pid,
                [TERMINAL],
            ]

        def competing_event(receipts, _):
            receipts[OLD]["evidence"]["corporate_actions"].append(
                {
                    "id": "competing",
                    "action_type": "cash_merger",
                    "acquiree_symbol": MIDDLE,
                    "acquiree_cusip": "rival-cusip",
                    "effective_date": "2024-02-14",
                }
            )

        def effective_date(receipts, _):
            next(a for a in receipts[MIDDLE]["evidence"]["corporate_actions"] if a["id"] == "arrival")[
                "effective_date"
            ] = "2024-02-15"

        def authored_arrival(receipts, _):
            evidence = receipts[MIDDLE]["evidence"]
            evidence["provider_corporate_actions"] = [
                item for item in evidence["corporate_actions"] if item["id"] != "arrival"
            ]
            evidence["event_supplements"] = [{"source": "external", "action_id": "arrival"}]

        def nonadjacent(_, held):
            ranges = held["offline_ownership"]["checkpoints"][0]["history_validation"]["prefix_records"][0][
                "ticker_ranges"
            ]
            next(item for item in ranges if item["ticker"] == OLD)["last_session"] = "2024-02-08"

        def changed_generation(_, held):
            held["offline_ownership"]["checkpoints"][0]["references"]["pricing"][TERMINAL] = "3" * 64

        def changed_target(receipts, _):
            receipts[MIDDLE]["serving_ownership"]["views"]["raw"]["ownership"][1]["rows"] -= 1

        def incomplete_window(receipts, _):
            receipts[OLD]["pricing_request"]["end_exclusive"] = "2024-02-13T00:00:00Z"

        for mutation in (
            competing_choice,
            competing_event,
            effective_date,
            authored_arrival,
            nonadjacent,
            changed_generation,
            changed_target,
            incomplete_window,
        ):
            with self.subTest(change=mutation.__name__):
                receipts, held = deepcopy(previous), deepcopy(offline)
                mutation(receipts, held)
                with (
                    self.forbid_history_work(),
                    patch.object(
                        storage, "read_boundaries", side_effect=AssertionError("premature boundary read")
                    ),
                    patch.object(
                        storage,
                        "read_assignment_projection",
                        side_effect=AssertionError("premature projection read"),
                    ),
                    self.assertRaises(LineageError),
                ):
                    retained_rename_seam.prepare(
                        retained_rename_seam.select(group, held), receipts, self.case.root
                    )
        with self.forbid_history_work(), self.assertRaisesRegex(LineageError, "generation changed"):
            replace(group, pricing={**group.pricing, OLD: "0" * 64}).select(self.index)

    def test_earlier_unchanged_label_keeps_its_entire_numerical_approval(self):
        earlier = "SUMAC"
        self.lose_group(earlier=earlier)
        with self.forbid_history_work():
            result = recover(self.store, None, selection=self.selection)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["serving_catalogue"]["checked_bars"], 0)
        self.assertEqual(result["serving_catalogue"]["reused_history_prefixes"], 1)
        library = DataLibrary(store=self.store)
        self.assertEqual(library.resolve_ticker(earlier)[0].public_id, self.pid)
        for view, history in library.get_histories(self.pid).items():
            self.assertEqual(history.bars, self.approved[view].bars)

    def test_overlapping_unassigned_peer_with_a_real_candidate_prevents_restoration(self):
        self.lose_group()
        original = storage.read_assignment_projection
        reads = []

        def competing(root, receipt, kind, handoff, **kwargs):
            reads.append((receipt["symbol"], kind))
            document = original(root, receipt, kind, handoff, **kwargs)
            if receipt["symbol"] == TERMINAL and kind == "completed_years":
                segment = document["adjustments"][0]["segments"][0]
                segment["candidate_securities"] = [{"asset_id": "peer-rival"}]
            return document

        with (
            self.forbid_history_work(),
            patch.object(storage, "read_assignment_projection", side_effect=competing),
            patch.object(storage, "publish", side_effect=AssertionError("published ambiguous owner")),
            self.assertRaisesRegex(LineageError, "overlapping peer"),
        ):
            recover(self.store, None, selection=self.selection)
        self.assertEqual(reads, [(TERMINAL, "completed_years")])
