"""Serving work is coalesced only while the selected group bindings remain applicable."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import dated_price_repair as repair


class RepairBatchTests(TestCase):
    def test_reselects_only_when_prior_batch_changed_the_current_group(self):
        names = {"first": "A", "second": "B", "third": "A"}
        versions = {"A": 0, "B": 0}
        reads, selections, publications = [], [], []

        def read(store, pid):
            group = names[pid]
            reads.append((pid, versions[group]))
            return SimpleNamespace(_groups={group: {"locator": group}}, version=versions[group])

        def select(store, pid, cut, client, data):
            group = names[pid]
            self.assertEqual(data.version, versions[group])
            selections.append(pid)
            return group, {"status": "selected"}

        def maintain(store, symbols):
            publications.append(symbols)
            for group in symbols:
                versions[group] += 1
            return {"failed": 0, "complete": True}

        with (
            patch.object(repair.DataLibrary, "_for_online_identity", side_effect=read),
            patch.object(repair, "_select", side_effect=select),
            patch.object(repair, "maintain", side_effect=maintain),
        ):
            result = repair.repair_many(".", list(names), ["2021-10-01"] * 3, SimpleNamespace(request_count=0))
        self.assertEqual(selections, list(names))
        self.assertEqual(reads, [("first", 0), ("second", 0), ("third", 0), ("third", 1)])
        self.assertEqual(publications, [["A", "B"], ["A"]])
        self.assertEqual([o["status"] for o in result["outcomes"]], ["published"] * 3)
        self.assertEqual(len(result["prior_serving_batches"]), 1)

    def test_invalid_later_date_precedes_any_source_or_store_access(self):
        with (
            patch.object(
                repair.DataLibrary, "_for_online_identity", side_effect=AssertionError("storage accessed")
            ),
            self.assertRaises(ValueError),
        ):
            repair.repair_many(".", ["a", "b"], ["2021-10-01", "invalid"], None)
