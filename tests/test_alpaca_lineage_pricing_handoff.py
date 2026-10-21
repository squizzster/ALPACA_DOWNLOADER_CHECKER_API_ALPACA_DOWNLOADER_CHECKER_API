import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.pipeline import _catch_up_published_dependencies, _prefetch_metadata
from libs.alpaca_lineage_build.selection import DependencyGraph


class CompletedPricingHandoffTests(unittest.TestCase):
    def test_metadata_plan_consumes_the_handoff_without_reopening_sources_notices_or_locks(self):
        previous = {
            "anchor_month": "2026-12",
            "versions": versions(),
            "evidence": {"queried_symbols": ["CURRENT"]},
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
