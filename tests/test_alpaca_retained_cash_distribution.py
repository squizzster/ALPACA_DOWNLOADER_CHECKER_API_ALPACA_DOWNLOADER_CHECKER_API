"""Restore a traded cash-distribution suffix across overlapping physical lineage partitions."""

from copy import deepcopy
from dataclasses import replace
from itertools import groupby
from unittest.mock import patch

from test_alpaca_due_bill_cash import case
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import asset

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_lineage_build import accepted_owner_recovery as owner
from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build import retained_cash_distribution as cash
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.retained_recovery import (
    RECOVERY_BOUNDARY,
    RESTORE_ACCEPTED_OWNER,
    accept_selection,
    recover,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS
from libs.alpaca_serving_catalogue.producer import maintain


class RetainedCashDistributionTests(NativeCase):
    def lose_suffix(self, *, conflict=False, contender=False, ex=None):
        name = "PAYER"
        rows = valid_bars("2026-01-22", "2026-02-06")
        ex = ex or "2026-01-29"
        sessions = {row["t"][:10]: row for row in rows}
        self.add_symbol(name, rows, metadata=asset(name, "old-id"), end="2026-02-06")
        pid = self.bootstrap().resolve_ticker(name)[0].public_id
        observed = [day for day in sessions if day >= ex]
        evidence, _ = case(name, "REPLACEMENT")
        replace_dates = {
            "2024-01-17": "2026-02-09",
            "2024-01-08": ex,
            "2024-01-03": "2026-01-22",
            "2024-01-05": "2026-01-28",
        }
        for action in evidence["corporate_actions"]:
            for key, value in action.items():
                if key.endswith("_date"):
                    action[key] = replace_dates[value]
        if conflict:
            evidence["corporate_actions"][-1]["rate"] = "900"
        evidence["query"].update(corporate_actions_start="2026-01-01", corporate_actions_end="2026-02-20")
        for occupant in evidence["symbol_occupancy_evidence"]:
            for record in occupant["evidence"]:
                record["process_date"] = "2026-02-09"
        original_interval = {
            "historical_symbol": name,
            "cusip": "old-cusip",
            "asset_id": "old-id",
            "start_date_inclusive": min(sessions),
            "end_date_exclusive": ex,
            "supporting_corporate_action_ids": ["cash", "transition"],
            "ownership_interval_rules": [
                {
                    "order": 1,
                    "name": "name_change_ticker_transition",
                    "supporting_corporate_action_ids": ["transition"],
                },
                {
                    "order": 3,
                    "name": "cash_merger_acquiree_terminal_boundary",
                    "supporting_corporate_action_ids": ["cash"],
                },
            ],
        }

        def lose(receipt, documents):
            for kind, doc in documents.items():
                if kind == "completed_years":
                    continue
                for view in doc["adjustments"]:
                    revised = []
                    for segment in view["segments"]:
                        dates = [day for day in sessions if segment["from"] <= day <= segment["to"]]
                        position = segment["row_start"]
                        for lost, part in groupby(dates, key=lambda day: day >= ex):
                            part = list(part)
                            body = {
                                **segment,
                                "from": part[0],
                                "to": part[-1],
                                "first_timestamp": sessions[part[0]]["t"],
                                "last_timestamp": sessions[part[-1]]["t"],
                                "row_start": position,
                                "row_end": position + len(part) - 1,
                                "row_count": len(part),
                            }
                            if lost:
                                body = {
                                    k: body[k]
                                    for k in (
                                        "from",
                                        "to",
                                        "first_timestamp",
                                        "last_timestamp",
                                        "row_start",
                                        "row_end",
                                        "row_count",
                                    )
                                }
                                candidates = (
                                    [{"asset_id": "another-issue"}]
                                    if (
                                        contender
                                        and kind == "current_year"
                                        and view["adjustment"] == "dividend"
                                        and part[0] >= "2026-02-01"
                                    )
                                    else []
                                )
                                body.update(
                                    branch="uncorrelated",
                                    candidate_securities=candidates,
                                    reason_codes=[cash.LOSS],
                                )
                            else:
                                body["ticker_ownership_interval"] = original_interval
                            revised.append(body)
                            position += len(part)
                    view["segments"] = revised
                receipt["assignment_handoffs"].pop(kind)
            receipt["evidence"] = {
                **receipt["evidence"],
                **evidence,
                "corporate_action_count": len(evidence["corporate_actions"]),
            }
            before = max(day for day in sessions if day < ex)
            receipt["observation_routes"] = {pid: [sessions[before]["t"], original_interval]}
            receipt["identity_intervals"] = [{"from": min(sessions), "to": before, "security_id": pid}]
            receipt["unresolved_rows"] = len(observed) * len(ADJUSTMENTS)
            receipt["correlated_rows"] -= receipt["unresolved_rows"]

        self.publish_change(lose, name)
        self.assertTrue(maintain(self.store)["complete"])
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(name), ())
        index = storage.read_index(self.root)
        entry = index["symbols"][name]
        selection = accept_selection(
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
                        "members": [name],
                        "pricing_generations": {name: entry["source_sha256"]},
                        "receipt_generations": {name: entry["sha256"]},
                    }
                ],
            },
            self.store,
        )
        universe_state.initialize(self.store)
        selected = {
            "symbol": name,
            "asof": "2026-02-06",
            "end_exclusive": "2026-02-06T00:00:00Z",
            "source_sha256": entry["source_sha256"],
            "tail_all": [[str(row[field]) for field in FIELDS] for row in rows[-10:]],
        }
        return (
            name,
            pid,
            observed,
            selection,
            {"complete": True, "entries": {name: selected}, "pending": {name: set()}},
        )

    def test_same_activity_and_numerical_checkpoint_restore_both_month_copies(self):
        name, pid, days, selection, summary = self.lose_suffix()
        with (
            patch.object(cash.check_summary, "selected_inputs", return_value=summary) as selected,
            patch.object(
                cash.pricing_storage, "read_validator", side_effect=AssertionError("accepted summary reread")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing replay")),
            patch.object(
                cash.pricing_storage, "read_boundary", side_effect=AssertionError("retained payload opened")
            ),
            patch.object(standalone, "_rows", side_effect=AssertionError("historical prices reopened")),
        ):
            result = recover(self.store, None, selection=selection)
        self.assertTrue(result["complete"], result)
        self.assertEqual(selected.call_count, 1)
        self.assertEqual(result["serving_catalogue"]["checked_bars"], 0)
        self.assertEqual(result["serving_catalogue"]["reused_history_prefixes"], 1)
        self.assertEqual([pid], [item.public_id for item in DataLibrary(store=self.store).resolve_ticker(name)])
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, name, index)
        self.assertEqual(receipt["unresolved_rows"], 0)
        self.assertEqual(receipt["retained_owner_restoration"]["observed_through"], days[-1])
        for kind, doc in storage.read_boundaries(
            self.root, receipt, kinds={"current_year", "current_month"}
        ).items():
            expected = [day for day in days if kind == "current_year" or day[:7] == "2026-02"]
            for view in doc["adjustments"]:
                restored = [s for s in view["segments"] if s.get("correlation_method") == cash.METHOD]
                self.assertEqual(sum(s["row_count"] for s in restored), len(expected))
                self.assertEqual({s["canonical_security"]["alpaca_asset_id"] for s in restored}, {"old-id"})
                self.assertEqual(
                    set(restored[0]["ticker_ownership_interval"]["supporting_corporate_action_ids"]),
                    {"cash", "special", "transition"},
                )

    def test_stale_summary_uses_only_the_selected_validator_and_journal_retry_needs_neither(self):
        name, pid, _, selection, summary = self.lose_suffix()
        summary["entries"][name]["source_sha256"] = "0" * 64
        with (
            patch.object(cash.check_summary, "selected_inputs", return_value=summary),
            patch.object(
                cash.pricing_storage, "read_validator", wraps=cash.pricing_storage.read_validator
            ) as read,
            patch.object(storage, "_install", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            recover(self.store, None, selection=selection)
        self.assertEqual(read.call_count, 1)
        with (
            patch.object(cash, "prepare", side_effect=AssertionError("completed ownership repeated")),
            patch.object(
                cash.check_summary, "selected_inputs", side_effect=AssertionError("completed pricing repeated")
            ),
            patch.object(
                cash.pricing_storage,
                "read_validator",
                side_effect=AssertionError("completed validator repeated"),
            ),
            patch.object(standalone, "_rows", side_effect=AssertionError("approved prices reopened")),
        ):
            result = recover(self.store, None, selection=selection)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["serving_catalogue"]["checked_bars"], 0)
        self.assertEqual([pid], [item.public_id for item in DataLibrary(store=self.store).resolve_ticker(name)])

    def test_missing_activity_keeps_the_accepted_owner_recovery_offline(self):
        name, _, _, selection, summary = self.lose_suffix()
        for change in ("zero", "missing"):
            with self.subTest(problem=change):
                modified = deepcopy(summary)
                tail = modified["entries"][name]["tail_all"]
                if change == "zero":
                    tail[-3][6] = "0"
                else:
                    del tail[:-3]
                with (
                    patch.object(cash.check_summary, "selected_inputs", return_value=modified),
                    patch.object(
                        cash.pricing_storage,
                        "read_validator",
                        side_effect=AssertionError("accepted summary repeated"),
                    ),
                    patch.object(
                        cash, "assignment_documents", side_effect=AssertionError("unselected assignments opened")
                    ),
                    self.assertRaisesRegex(LineageError, "activity tail|reported trading"),
                ):
                    recover(self.store, None, selection=selection)
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(name), ())

    def test_contrary_native_terms_do_not_select_pricing_or_publication(self):
        name, _, _, selection, _ = self.lose_suffix(conflict=True)
        with (
            patch.object(cash, "_activity_tail", side_effect=AssertionError("unselected pricing")),
            self.assertRaisesRegex(LineageError, "native cash distribution"),
        ):
            recover(self.store, None, selection=selection)
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(name), ())

    def test_candidate_in_only_the_overlapping_year_copy_preserves_the_offline_disposition(self):
        name, _, _, selection, summary = self.lose_suffix(contender=True)
        with (
            patch.object(cash.check_summary, "selected_inputs", return_value=summary),
            patch.object(storage, "publish", side_effect=AssertionError("conflict published")),
            self.assertRaisesRegex(LineageError, "candidate-free traded suffix"),
        ):
            recover(self.store, None, selection=selection)
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(name), ())

    def test_single_distribution_day_print_does_not_establish_trading_beyond_that_day(self):
        name, _, _, selection, summary = self.lose_suffix(ex="2026-02-05")
        with (
            patch.object(cash.check_summary, "selected_inputs", return_value=summary),
            patch.object(
                cash, "assignment_documents", side_effect=AssertionError("unselected assignments opened")
            ),
            self.assertRaisesRegex(LineageError, "needs reported trading"),
        ):
            recover(self.store, None, selection=selection)
        self.assertEqual(DataLibrary(store=self.store).resolve_ticker(name), ())

    def test_price_change_inside_the_selected_checkpoint_has_no_reusable_approval(self):
        name, _, _, selection, _ = self.lose_suffix()
        offline = self.catalogue_groups(name)[name]
        group = selection.groups[0]
        previous = group.pricing[name]
        later = "f" * 64
        changed = replace(group, pricing={name: later})
        offline["references"]["pricing"] = changed.pricing
        source = {"history_changes": [{"source_sha256": previous, "changed_from": "2026-02-02"}]}
        with self.assertRaisesRegex(LineageError, "ancestry"):
            owner._accepted_source_checkpoint(changed, offline, source)
        source["history_changes"][0]["changed_from"] = None
        owner._accepted_source_checkpoint(changed, offline, source)
