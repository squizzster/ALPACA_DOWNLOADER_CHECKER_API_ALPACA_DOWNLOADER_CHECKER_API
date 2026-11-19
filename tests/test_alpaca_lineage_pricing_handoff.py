import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.contract import LineageError, versions
from libs.alpaca_lineage_build.dependency_suffixes import accept_empty_suffixes
from libs.alpaca_lineage_build.pipeline import _catch_up_published_dependencies, _prefetch_metadata
from libs.alpaca_lineage_build.selection import DependencyGraph


class CompletedPricingHandoffTests(unittest.TestCase):
    def test_offline_dependency_receipt_applies_only_to_the_original_work_and_exact_provider_scope(self):
        group = ["CURRENT", "PEER"]
        receipt = {
            "version": 1,
            "obligation_id": "work",
            "owner": "CURRENT",
            "group": group,
            "source_sha256": "b" * 64,
            "start": "2026-10-20T00:00:00+00:00",
            "end_exclusive": "2026-10-22T00:00:00+00:00",
            "asof": "2026-09-10",
        }
        for field in (None, "source_sha256", "start", "end_exclusive", "asof", "obligation_id"):
            with self.subTest(changed=field), tempfile.TemporaryDirectory() as directory:
                selected = dict(receipt)
                if field:
                    selected[field] = "superseded"
                index = {
                    "symbols": {
                        symbol: {
                            "dependencies": [s for s in group if s != symbol],
                            "asset_ids": [],
                            "source_sha256": ("a" if symbol == "CURRENT" else "b") * 64,
                            "projection": {
                                "pricing_request": {
                                    "history_start": "2016-01-01",
                                    "asof": "2026-09-10",
                                    "end_exclusive": (
                                        "2026-10-22T00:00:00Z" if symbol == "CURRENT" else "2026-10-20T00:00:00Z"
                                    ),
                                }
                            },
                        }
                        for symbol in group
                    }
                }
                policy = SimpleNamespace(
                    allowed=lambda _: True,
                    work={"CURRENT": {"obligation_id": "work", "dependency_suffixes": {"PEER": selected}}},
                )
                empty = set()
                with (
                    patch("libs.alpaca_lineage_build.pipeline.acquire_selected", side_effect=AssertionError),
                    patch("libs.alpaca_lineage_build.storage.pricing_locks", side_effect=AssertionError),
                ):
                    _catch_up_published_dependencies(
                        Path(directory),
                        ["CURRENT"],
                        index,
                        DependencyGraph(index["symbols"]),
                        None,
                        work_policy=policy,
                        pricing_handoff={},
                        empty_suffixes=empty,
                    )
                    if field is None:
                        # The same operation keeps its selection after workflow
                        # acknowledgement releases the durable copy.
                        policy.work.clear()
                        resumed = _catch_up_published_dependencies(
                            Path(directory),
                            ["CURRENT"],
                            index,
                            DependencyGraph(index["symbols"]),
                            SimpleNamespace(execute_plan=lambda *_: None),
                            work_policy=policy,
                            pricing_handoff={},
                            empty_suffixes=empty,
                        )
                        self.assertEqual((0, 1), (resumed["requested"], resumed["skipped"]))
                self.assertEqual(set() if field else {("PEER", "b" * 64, "2026-10-22")}, empty)

    def test_finishing_owner_transfers_only_to_still_unfinished_group_work(self):
        for held in (False, True):
            with self.subTest(held=held), tempfile.TemporaryDirectory() as directory:
                store = Path(directory)
                universe_state.initialize(store)
                group = {"SEED", "PEER"}
                work = universe_state.put_work_group(
                    store, {"SEED": {"obligation_id": "seed", "status": "pending"}}
                )
                policy = SimpleNamespace(store=store, work=work)
                result = {
                    "symbol": "PEER",
                    "source_sha256": "a" * 64,
                    "append_work_selection": {
                        "start": "2026-10-09T00:00:00+00:00",
                        "end_exclusive": "2026-10-14T00:00:00+00:00",
                        "asof": "2026-10-14",
                    },
                }
                accept_empty_suffixes(policy, {"PEER": group}, [result])
                original = work["SEED"]["dependency_suffixes"]["PEER"]
                work["PEER"] = universe_state.put_work(
                    store, "PEER", {"obligation_id": "next", "status": "pending"}
                )

                def acknowledge():
                    with universe_state.outcome_batch(store, work=work if held else None) as batch:
                        universe_state.record_outcome(store, "SEED", batch=batch, status="complete", outcome={})

                with patch.object(universe_state, "_advance", side_effect=RuntimeError("before commit")):
                    with self.assertRaisesRegex(RuntimeError, "before commit"):
                        acknowledge()
                self.assertNotIn("dependency_suffixes", work["PEER"])
                self.assertEqual(work, universe_state.read_state(store)["work"])
                acknowledge()
                state = universe_state.read_state(store)["work"]
                self.assertNotIn("dependency_suffixes", state["SEED"])
                self.assertEqual(
                    {**original, "obligation_id": "next", "owner": "PEER"},
                    state["PEER"]["dependency_suffixes"]["PEER"],
                )
                if held:
                    self.assertEqual(state["PEER"], work["PEER"])
                with self.assertRaisesRegex(LineageError, "superseded work"):
                    universe_state.accept_dependency_suffixes(store, {"SEED": {"PEER": original}})
                universe_state.record_outcome(store, "PEER", status="complete", outcome={})
                self.assertNotIn("dependency_suffixes", universe_state.read_state(store)["work"]["PEER"])

    def test_incompatible_durable_suffix_receipt_selects_owning_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            for receipt in ({"version": 5}, {"version": 1}, None):
                with self.subTest(receipt=receipt):
                    universe_state.put_work(
                        store,
                        "PEER",
                        {
                            "obligation_id": "pending",
                            "status": "pending",
                            "dependency_suffixes": {"PEER": receipt},
                        },
                    )
                    with self.assertRaisesRegex(LineageError, "owning recovery"):
                        universe_state.read_state(store)

    def test_metadata_plan_consumes_the_handoff_without_reopening_sources_notices_or_locks(self):
        previous = {
            "source_sha256": "a" * 64,
            "anchor_month": "2026-12",
            "versions": versions(),
            "evidence": {"queried_symbols": ["CURRENT"], "query": {"corporate_actions_start": "2016-01-01"}},
        }
        index = {
            "symbols": {
                "CURRENT": {
                    "source_sha256": "a" * 64,
                    "dependencies": [],
                    "asset_ids": ["current-id"],
                    "group_members": ["CURRENT"],
                    "projection": {
                        "pricing_request": {
                            "history_start": "2016-01-01T00:00:00Z",
                            "end_exclusive": "2026-12-31T00:00:00Z",
                            "asof": "2026-12-31",
                        }
                    },
                }
            }
        }
        handoff = {
            "CURRENT": {
                "source_sha256": "b" * 64,
                "mode": "append",
                "changed": True,
                "request": {
                    "start": "2026-12-31T00:00:00Z",
                    "end_exclusive": "2027-01-05T00:00:00Z",
                    "asof": "2027-01-05",
                },
            }
        }
        actions, symbols = [], []
        client = SimpleNamespace(
            supports_plans=True,
            prefetch_corporate_actions=lambda requests: actions.extend(requests),
            prefetch_symbols=lambda requests: symbols.extend(requests),
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch(
                "libs.alpaca_lineage_build.pipeline._current_sources",
                side_effect=AssertionError("completed sources were rediscovered"),
            ),
            patch(
                "libs.alpaca_lineage_build.storage.pricing_locks",
                side_effect=AssertionError("metadata planning touched ticker locks"),
            ),
            patch(
                "libs.alpaca_lineage_build.pipeline.read_update",
                side_effect=AssertionError("completed changed range was rediscovered"),
            ),
        ):
            root = Path(directory)
            _prefetch_metadata(
                root,
                ["CURRENT"],
                index,
                DependencyGraph(index["symbols"]),
                client,
                False,
                SimpleNamespace(read=lambda symbol: previous),
                end_date="2027-01-05",
                pricing_handoff=handoff,
            )
        self.assertEqual(actions, [("CURRENT", "2026-12-01", "2027-01-05")])
        self.assertEqual(symbols, ["CURRENT"])

    def test_metadata_plan_does_not_exact_lookup_a_retained_label_without_new_rows(self):
        previous = {
            "source_sha256": "a" * 64,
            "anchor_month": "2026-12",
            "versions": versions(),
            "evidence": {
                "queried_symbols": ["RETIRED"],
                "query": {"corporate_actions_start": "2016-01-01"},
                "asset_lookups": [
                    {
                        "lookup": "RETIRED",
                        "lookup_kind": "symbol",
                        "outcome": "not_found",
                        "http_status": 404,
                    }
                ],
            },
        }
        index = {
            "symbols": {
                "RETIRED": {
                    "source_sha256": "a" * 64,
                    "dependencies": [],
                    "asset_ids": [],
                    "group_members": ["RETIRED"],
                    "projection": {
                        "pricing_request": {
                            "history_start": "2016-01-01T00:00:00Z",
                            "end_exclusive": "2026-12-31T00:00:00Z",
                            "asof": "2026-12-31",
                        }
                    },
                }
            }
        }
        handoff = {
            "RETIRED": {
                "source_sha256": "b" * 64,
                "mode": "append",
                "changed": True,
                "added_rows": dict.fromkeys(
                    (
                        "raw",
                        "split",
                        "dividend",
                        "split,dividend",
                        "spin-off",
                        "split,spin-off",
                        "dividend,spin-off",
                        "all",
                    ),
                    0,
                ),
                "request": {
                    "start": "2026-12-31T00:00:00Z",
                    "end_exclusive": "2027-01-05T00:00:00Z",
                    "asof": "2027-01-05",
                },
            }
        }
        actions, symbols = [], []
        client = SimpleNamespace(
            supports_plans=True,
            prefetch_corporate_actions=lambda requests: actions.extend(requests),
            prefetch_symbols=lambda requests: symbols.extend(requests),
        )
        with tempfile.TemporaryDirectory() as directory:
            _prefetch_metadata(
                Path(directory),
                ["RETIRED"],
                index,
                DependencyGraph(index["symbols"]),
                client,
                False,
                SimpleNamespace(read=lambda symbol: previous),
                end_date="2027-01-05",
                pricing_handoff=handoff,
            )
        self.assertEqual(actions, [("RETIRED", "2026-12-01", "2027-01-05")])
        self.assertEqual(symbols, [])

    def test_empty_dependency_suffix_selects_no_persistent_work(self):
        members = ("CURRENT", "RETIRED")
        inactive = {
            "id": "retired-id",
            "symbol": "RETIRED",
            "class": "us_equity",
            "exchange": "NYSE",
            "name": "Retired",
            "status": "inactive",
            "tradable": False,
        }
        index = {
            "symbols": {
                symbol: {
                    "source_sha256": ("a" if symbol == "CURRENT" else "b") * 64,
                    "dependencies": [other for other in members if other != symbol],
                    "asset_ids": [symbol.lower() + "-id"],
                    "group_members": list(members),
                    "projection": {
                        "pricing_request": {
                            "history_start": "2016-01-01T00:00:00Z",
                            "end_exclusive": "2026-10-20T00:00:00Z",
                            "asof": "2026-10-20",
                        },
                        "processed_through": "2026-10-20T00:00:00Z",
                        "source_rows": 0,
                        "unresolved_rows": 0,
                        "symbol_lookups": [
                            {
                                "lookup": symbol,
                                "lookup_kind": "symbol",
                                "outcome": "found",
                                "http_status": 200,
                                "asset": inactive
                                if symbol == "RETIRED"
                                else {
                                    **inactive,
                                    "id": "current-id",
                                    "symbol": "CURRENT",
                                    "name": "Current",
                                    "status": "active",
                                    "tradable": True,
                                },
                            }
                        ],
                    },
                }
                for symbol in members
            }
        }
        handoff = {
            "CURRENT": {
                "symbol": "CURRENT",
                "source_sha256": "c" * 64,
                "mode": "append",
                "status": "updated",
                "changed": True,
                "request": {
                    "start": "2026-10-20T00:00:00Z",
                    "end_exclusive": "2026-10-21T00:00:00Z",
                    "asof": "2026-10-21",
                },
            }
        }
        acquired = {
            "results": [
                {
                    "symbol": "RETIRED",
                    "status": "not_selected",
                    "source_sha256": "b" * 64,
                    "append_work_selection": {"end_exclusive": "2026-10-21T00:00:00+00:00"},
                }
            ],
            "requests": {"append_work_candidates": 1, "append_work_skipped": 1},
            "failed": 0,
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("libs.alpaca_lineage_build.pipeline.acquire_selected", return_value=acquired) as acquire,
        ):
            empty_suffixes = set()
            result = _catch_up_published_dependencies(
                Path(directory),
                ["CURRENT"],
                index,
                DependencyGraph(index["symbols"]),
                type("PricingOwner", (), {"execute_plan": lambda self, plan: None})(),
                end_date="2026-10-21",
                pricing_handoff=handoff,
                empty_suffixes=empty_suffixes,
            )
        self.assertEqual(result["requested"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertTrue(acquire.call_args.kwargs["select_append_work"])
        self.assertEqual(empty_suffixes, {("RETIRED", "b" * 64, "2026-10-21")})

    def test_dependency_plan_consumes_published_ranges_without_opening_ticker_storage(self):
        for previous_end, published_end in (
            ("2026-10-20", "2026-10-21"),
            ("2026-10-20", "2026-11-10"),
            ("2026-12-31", "2027-01-05"),
        ):
            with self.subTest(previous_end=previous_end, published_end=published_end):
                members = ("CURRENT", "RELATED")
                index = {
                    "symbols": {
                        symbol: {
                            "source_sha256": ("a" if symbol == "CURRENT" else "b") * 64,
                            "dependencies": [other for other in members if other != symbol],
                            "asset_ids": [symbol + "-id"],
                            "group_members": list(members),
                            "projection": {
                                "pricing_request": {
                                    "history_start": "2016-01-01T00:00:00Z",
                                    "end_exclusive": previous_end + "T00:00:00Z",
                                    "asof": previous_end,
                                },
                                "processed_through": previous_end + "T00:00:00Z",
                            },
                        }
                        for symbol in members
                    }
                }
                handoff = {
                    "CURRENT": {
                        "symbol": "CURRENT",
                        "source_sha256": "c" * 64,
                        "mode": "append",
                        "status": "updated",
                        "changed": True,
                        "request": {
                            "start": previous_end + "T00:00:00Z",
                            "end_exclusive": published_end + "T00:00:00Z",
                            "asof": published_end,
                        },
                    }
                }
                acquired = {"results": [], "requests": {}, "failed": 0}
                with (
                    tempfile.TemporaryDirectory() as directory,
                    patch(
                        "libs.alpaca_lineage_build.pipeline._current_sources",
                        side_effect=AssertionError("completed pricing ranges were rediscovered"),
                    ),
                    patch(
                        "libs.alpaca_lineage_build.storage.pricing_locks",
                        side_effect=AssertionError("planning touched ticker locks"),
                    ),
                    patch(
                        "libs.alpaca_lineage_build.pipeline.acquire_selected", return_value=acquired
                    ) as acquire,
                ):
                    result = _catch_up_published_dependencies(
                        Path(directory),
                        ["CURRENT"],
                        index,
                        DependencyGraph(index["symbols"]),
                        type("PricingOwner", (), {"execute_plan": lambda self, plan: None})(),
                        end_date=published_end,
                        pricing_handoff=handoff,
                    )
                self.assertEqual(result["failed"], 0)
                self.assertEqual(result["requested"], 1)
                self.assertEqual(acquire.call_args.args[1:3], (["RELATED"], published_end))
                self.assertTrue(acquire.call_args.kwargs["select_append_work"])
