"""Serving publication waits remain bounded; owner journals retain failed work."""

import sqlite3
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_pricing_download import storage as pricing
from libs.alpaca_serving_catalogue import export_inputs, producer, storage
from libs.common.sqlite_connections import connect_sqlite


class ServingTransactionTests(NativeCase):
    def test_serving_writers_receive_the_120_second_timeout(self):
        data = self.bootstrap()
        generation = next(iter(data._groups.values()))["references"]["pricing"]["TEST"]
        calls = []

        def opened(path, **kwargs):
            calls.append(kwargs)
            return connect_sqlite(path, **kwargs)

        with patch.object(export_inputs, "connect_sqlite", side_effect=opened):
            export_inputs.invalidate(self.store, "pricing", {"TEST": generation})
            export_inputs.complete_unchanged_pricing(self.store, "TEST", generation)
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new metadata"]))
        with patch.object(storage, "connect_sqlite", side_effect=opened):
            result = producer.maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        writes = [options for options in calls if not options.get("readonly")]
        self.assertTrue(writes)
        self.assertTrue(all(options.get("timeout") == 120 for options in writes), writes)
        self.assertTrue(all("timeout" not in options for options in calls if options.get("readonly")))

    def test_timed_out_invalidation_leaves_the_completed_lineage_journal_recoverable(self):
        self.bootstrap()

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_tickers"] = ["NEW", "TEST"]

        with (
            patch.object(
                export_inputs, "connect_sqlite", side_effect=sqlite3.OperationalError("database is locked")
            ),
            self.assertRaises(sqlite3.OperationalError),
        ):
            self.publish_change(relabel)
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            patch.object(lineage, "read_boundaries", side_effect=AssertionError("accepted assignments reread")),
            patch.object(pricing, "read_boundary", side_effect=AssertionError("accepted prices reread")),
            patch("libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("rules repeated")),
        ):
            with lineage.writer_snapshot(self.store) as view:
                self.assertTrue(lineage.recover(self.store, view.root))
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertTrue(producer.maintain(self.store)["complete"])
