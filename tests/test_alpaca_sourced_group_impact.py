"""Complete provider groups account for every changed row and preserve unrelated owners."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import correlation, public_common_continuation
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_lineage_build.reader import read_lineage
from libs.alpaca_pricing_download.download import download_pricing


class SourcedCompleteGroupImpactTests(unittest.TestCase):
    def test_all_nine_sources_account_for_every_owner_change(self):
        names = ("AMRK", "B", "GOLD", "ABL", "ABLLW", "ABX", "ERES", "ERESU", "ERESW")
        rename = {
            "id": "synthetic-rename",
            "action_type": "name_change",
            "old_symbol": "GOLD",
            "new_symbol": "B",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "process_date": "2024-01-05",
        }
        goldcom = {
            "id": "synthetic-goldcom",
            "action_type": "name_change",
            "old_symbol": "AMRK",
            "new_symbol": "GOLD",
            "old_cusip": "444444444",
            "new_cusip": "444444444",
            "process_date": "2024-01-05",
        }
        later_common_label = {
            "id": "synthetic-common-label",
            "action_type": "name_change",
            "old_symbol": "ABL",
            "new_symbol": "ABX",
            "old_cusip": "666666666",
            "new_cusip": "666666666",
            "process_date": "2024-01-08",
        }
        merger = {
            "id": "synthetic-consideration",
            "action_type": "stock_merger",
            "acquiree_symbol": "GOLD",
            "acquiree_cusip": "333333333",
            "acquirer_symbol": "ABX",
            "acquirer_cusip": "111111111",
            "effective_date": "2024-01-02",
            "process_date": "2024-01-02",
            "acquiree_rate": "1",
            "acquirer_rate": "6.128",
        }
        assets = {name: asset(name, name.lower() + "-id") for name in names}
        assets["AMRK"] = asset("GOLD", "gold-id")
        assets["ABX"]["exchange"] = "NYSE"
        assets["ABL"] = None
        assets.update(
            {
                "111111111": asset("GOLD", "barrick-old-id"),
                "222222222": asset("B", "barnes-id"),
                "444444444": assets["GOLD"],
                "555555555": assets["ERES"],
                "666666666": assets["ABX"],
            }
        )
        assets["B"] = asset("B", "barnes-id")
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            ),
        ):
            store = Path(temporary) / "store"
            rows = {}
            for name in names:
                first, end = (
                    ("2024-01-02", "2024-01-05") if name in {"AMRK", "ERES"} else ("2024-01-05", "2024-01-09")
                )
                if name in {"B", "ABX"}:
                    first = "2024-01-02"
                if name == "ABL":
                    end = "2024-01-08"
                rows[name] = informative_bars(first, end)
                download_pricing(
                    store, name, "2024-01-09", from_date="2024-01-01", client=FakeClient(rows[name])
                )
            before_result = pipeline.run_lineage(
                store,
                names,
                identity_client=IdentityClient(assets, [rename, goldcom, merger, later_common_label]),
            )
            self.assertEqual(0, before_result["failed"], before_result)

            def assignments():
                result = {}
                for name in names:
                    document = read_lineage(store, name)
                    for adjustment, segments in document["adjustments"].items():
                        for segment in segments:
                            for index in range(segment["row_start"], segment["row_end"] + 1):
                                result[f"{name}/{adjustment}/{index}"] = {
                                    "asset_id": (segment.get("canonical_security") or {}).get("alpaca_asset_id"),
                                    "public_id": (segment.get("security_identity") or {}).get("security_id"),
                                    "label": segment.get("historical_symbol"),
                                    "branch": segment["branch"],
                                }
                return result

            before = assignments()
            assets["B"] = assets["222222222"] = asset("B", "barrick-current-id")
            allocate = correlation.correlate_download_rows
            evaluated_sources = set()

            def selected_allocation(rows, source, *args, **kwargs):
                self.assertNotEqual("ERES", source["requested_symbol"], "accepted predecessor owner replay")
                evaluated_sources.add(source["requested_symbol"])
                return allocate(rows, source, *args, **kwargs)

            source_history = public_common_continuation.source_history

            def selected_source_history(symbol, *args, **kwargs):
                self.assertNotEqual("ERES", symbol, "accepted predecessor source proof replay")
                return source_history(symbol, *args, **kwargs)

            load = pipeline.load_pricing

            def selected_load(store, symbol, **kwargs):
                self.assertNotEqual("ERES", symbol, "accepted predecessor full history load")
                return load(store, symbol, **kwargs)

            with (
                patch.object(
                    pipeline,
                    "download_pricing",
                    side_effect=AssertionError("synthetic comparison requested prices"),
                ),
                patch.object(pipeline, "load_pricing", side_effect=selected_load),
                patch.object(correlation, "correlate_download_rows", side_effect=selected_allocation),
                patch.object(public_common_continuation, "source_history", side_effect=selected_source_history),
            ):
                after_result = pipeline.run_lineage(
                    store,
                    ["B"],
                    identity_client=IdentityClient(assets, [rename, goldcom, merger, later_common_label]),
                    refresh=True,
                )
            self.assertEqual(0, after_result["failed"], after_result)
            self.assertIn("B", evaluated_sources)
            after = assignments()
            self.assertEqual(before.keys(), after.keys())
            predecessor = [key for key in before if key.startswith("ERES/")]
            self.assertEqual(24, len(predecessor))
            for key in predecessor:
                self.assertEqual("eres-id", after[key]["asset_id"], key)
                self.assertEqual("ERES", after[key]["label"], key)
                self.assertEqual("correlated", after[key]["branch"], key)
            changes = [
                {"row": key, "before": before[key], "after": after[key]}
                for key in before
                if before[key] != after[key]
            ]
            for name in ("AMRK", "GOLD", "ABLLW", "ERESU", "ERESW"):
                self.assertEqual(
                    {key: value for key, value in before.items() if key.startswith(name + "/")},
                    {key: value for key, value in after.items() if key.startswith(name + "/")},
                    name,
                )
            barnes_before = [key for key, item in before.items() if item["asset_id"] == "barnes-id"]
            self.assertTrue(barnes_before)
            self.assertFalse([key for key, item in after.items() if item["asset_id"] == "barnes-id"])
            self.assertTrue(
                all(
                    after[key]["asset_id"] in {None, "barrick-old-id", "barrick-current-id"}
                    for key in barnes_before
                )
            )
            receipts = {
                name: storage.read_receipt(
                    store / "end_of_day/lineage", name, storage.read_index(store / "end_of_day/lineage")
                )
                for name in names
            }
            (store / ".barrick-synthetic-impact-debug.json").write_text(
                json.dumps({"before": before, "after": after, "receipts": receipts}, indent=2)
            )
            # Provider data has no ERES -> ABL public-common transition. Its
            # owner stays separate rather than acquiring a research-only union.
            self.assertNotEqual(
                receipts["ABL"]["identity_memberships"], receipts["ERES"]["identity_memberships"]
            )
            self.assertEqual(receipts["ABL"]["identity_memberships"], receipts["ABX"]["identity_memberships"])
            for receipt in receipts.values():
                self.assertFalse(
                    (receipt.get("public_common_continuation_membership") or {}).get("certificates")
                )
                self.assertIsNone(receipt.get("public_common_continuation_ownership"))
            evidence = {
                "kind": "synthetic_complete_group_impact",
                "sources": list(names),
                "rows_before": len(before),
                "rows_after": len(after),
                "changes": changes,
                "barnes_loss_rows": barnes_before,
                "retained_numerical_parity_asserted": False,
                "online_external_target_asserted": False,
            }
            (store / ".barrick-synthetic-nine-label-impact.json").write_text(
                json.dumps(evidence, indent=2) + "\n"
            )
