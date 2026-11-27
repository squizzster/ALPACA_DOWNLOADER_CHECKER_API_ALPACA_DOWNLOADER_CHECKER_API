"""Historical listing scope, legacy capabilities, and atomic metadata-only publication."""

import gzip
import json
from contextlib import closing
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary
from libs.alpaca_serving_catalogue import metadata_evidence, metadata_projection, storage
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.sqlite_connections import connect_sqlite


def assessment(symbol):
    return {
        "symbol": symbol,
        "first_session": "2026-10-02",
        "last_session": "2026-10-05",
        "status": "changes_observed",
        "observations": [
            {"session": "2026-10-02", "tape": "B", "trade_at": "2026-10-02T17:00:00Z"},
            {"session": "2026-10-05", "tape": "C", "trade_at": "2026-10-05T17:00:00Z", "exchange": "NASDAQ"},
        ],
        "changes": [
            {
                "from_session": "2026-10-02",
                "to_session": "2026-10-05",
                "from_tape": "B",
                "to_tape": "C",
                "to_exchange": "NASDAQ",
                "effective_session": "2026-10-05",
            }
        ],
        "limits": ["multiple_listings_on_tape_b"],
    }


def consecutive_assessments(symbol):
    case = assessment(symbol)
    return [
        {
            **case,
            "first_session": point["session"],
            "last_session": point["session"],
            "observations": [point],
            "changes": [],
            "status": "sampled",
            "limits": case["limits"] if point["tape"] == "B" else [],
        }
        for point in case["observations"]
    ]


class HistoricalListingMetadataTests(NativeCase):
    def test_same_ticker_entry_change_requires_both_applicable_endpoints_on_rebuild(self):
        symbol = "SAME"
        rows = consecutive_assessments(symbol)
        change = assessment(symbol)["changes"][0]
        event = {**change, "from_symbol": symbol, "to_symbol": symbol, "diagnostics": "private"}
        envelope = {
            "version": 1,
            "assessments": {"online": rows, "offline": rows},
            "range_changes": {"online": [event], "offline": [event]},
        }
        endpoints = [
            {"ticker": symbol, "first_session": row["first_session"], "last_session": row["last_session"]}
            for row in rows
        ]
        records = {"online": {"ticker_ranges": endpoints}}
        grouped = {symbol: {"evidence": {}, "historical_listings": envelope}}

        def current():
            projected = metadata_evidence.project_group_evidence(records, grouped)
            self.assertNotIn("offline", projected)
            self.assertNotIn("cross_ticker_listing_changes", projected["online"])
            return projected["online"]["historical_listings"]

        self.assertEqual(current(), [rows[0], {**rows[1], "entry_change": change}])
        for endpoint in endpoints:
            records["rival"] = {"ticker_ranges": [endpoint]}
            self.assertEqual(current(), rows)
        del records["rival"]
        endpoints.append({**endpoints[0], "ticker": "OVERLAPPING"})
        self.assertEqual(current(), rows)
        endpoints.pop()
        endpoints.pop(0)
        self.assertEqual(current(), [rows[1]])
        envelope.pop("range_changes")
        self.assertEqual(current(), [rows[1]])

    def test_cross_ticker_change_follows_both_endpoint_assignments_only_for_its_published_owner(self):
        event = {**assessment("OLD")["changes"][0], "from_symbol": "OLD", "to_symbol": "NEW"}
        endpoints = [
            {"ticker": label, "first_session": day, "last_session": day}
            for label, day in (("OLD", event["from_session"]), ("NEW", event["to_session"]))
        ]
        ranges = [
            {
                "symbol": row["ticker"],
                "first_session": row["first_session"],
                "last_session": row["last_session"],
                "changes": [],
                "observations": [point],
                "status": "sampled",
                "limits": ["multiple_listings_on_tape_b"] if point["tape"] == "B" else [],
            }
            for row, point in zip(endpoints, assessment("OLD")["observations"])
        ]
        envelope = {
            "version": 1,
            "assessments": {"online": ranges, "offline": ranges},
            "label_changes": {"online": [{**event, "diagnostics": "private"}], "offline": [event]},
        }
        grouped = {"OLD": {"evidence": {}, "historical_listings": envelope}}
        records = {"online": {"ticker_ranges": endpoints}}

        def current():
            result = metadata_evidence.project_group_evidence(records, grouped)
            self.assertNotIn("offline", result)
            return result["online"].get("cross_ticker_listing_changes")

        self.assertEqual(current(), [event])
        endpoints.append({**endpoints[1], "ticker": "RIVAL"})
        self.assertIsNone(current())
        endpoints.pop()
        endpoints.pop(0)
        self.assertIsNone(current())

    def test_rebuilt_projection_keeps_completed_unknowns_and_only_applicable_public_ids_and_spans(self):
        case = assessment("TEST")
        unknown = {
            **case,
            "status": "unresolved",
            "observations": [],
            "changes": [],
            "limits": ["no_recognized_trade_tape"],
        }
        record = {
            "ticker_ranges": [
                {"ticker": "TEST", "first_session": day, "last_session": day}
                for day in ("2026-10-02", "2026-10-05")
            ]
        }
        envelope = {"version": 1, "assessments": {"online": [unknown], "offline": [case]}}
        grouped = {"OLD": {"evidence": {}, "historical_listings": envelope}}
        results = metadata_evidence.project_group_evidence({"online": record}, grouped)
        self.assertEqual(results, {"online": {"historical_listings": [unknown]}})
        record["ticker_ranges"][-1]["first_session"] = "2026-10-06"
        record["ticker_ranges"][-1]["last_session"] = "2026-10-06"
        self.assertEqual(metadata_evidence.project_group_evidence({"online": record}, grouped), {"online": {}})

    @staticmethod
    def handoff(data, key):
        group = data._groups[key]
        pid = data.resolve_ticker(key)[0].public_id
        result = assessment(key)
        result["diagnostics"] = {"trade_sessions_sampled": 2}
        result["observations"][0]["execution_venue"] = "private"
        return {
            "version": 1,
            "group_key": key,
            "decision_generation": group["decision_generation"],
            "pricing": group["references"]["pricing"],
            "lineage_before": group["references"]["lineage"],
            "lineage_after": dict.fromkeys(group["references"]["lineage"], "a" * 64),
            "unchanged_metadata_projection_version": 2,
            "historical_listings": {"version": 1, "assessments": {pid: [result], "offline": [assessment(key)]}},
        }

    def test_batch_metadata_only_commit_is_atomic_idempotent_and_preserves_each_base_capability(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("UPDATE metadata_evidence SET version=1 WHERE group_key='TEST'")
            db.execute("UPDATE metadata_evidence SET version=2 WHERE group_key='SECOND'")
        batches = {key: self.handoff(data, key) for key in ("TEST", "SECOND")}
        pid = data.resolve_ticker("SECOND")[0].public_id
        later = consecutive_assessments("SECOND")
        change = assessment("SECOND")["changes"][0]
        envelope = batches["SECOND"]["historical_listings"]
        envelope["assessments"][pid] = later
        envelope["range_changes"] = {pid: [{**change, "from_symbol": "SECOND", "to_symbol": "SECOND"}]}
        state = storage.selector_state(self.store)
        invalid = {**batches, "SECOND": {**batches["SECOND"], "unchanged_metadata_projection_version": 0}}
        with (
            patch.object(storage, "read_groups", side_effect=AssertionError("reopened decisions")),
            patch.object(
                metadata_projection, "selected_metadata_receipts", side_effect=AssertionError("read receipts")
            ),
            patch.object(
                metadata_evidence, "project_group_evidence", side_effect=AssertionError("reprojected group")
            ),
            patch.object(storage, "_write_state", wraps=storage._write_state) as publish,
        ):
            with self.assertRaisesRegex(CatalogueError, "incompatible historical"):
                storage.retarget_historical_decisions(self.store, invalid, state)
            self.assertEqual(storage.selector_state(self.store), state)
            self.assertEqual(
                metadata_projection.selected(self.store, data._groups, bodies=False),
                dict.fromkeys(batches, True),
            )
            committed = storage.retarget_historical_decisions(self.store, batches, state)
            # Recovery's owning selector already skipped every committed group.
            self.assertEqual(storage.retarget_historical_decisions(self.store, {}, committed), committed)
            self.assertEqual(publish.call_count, 1)
            current = DataLibrary(store=self.store)
            for key, supports_snapshots in (("TEST", False), ("SECOND", True)):
                with self.subTest(key=key):
                    pid = current.resolve_ticker(key)[0].public_id
                    result = current.write_metadata_export(self.directory / key, public_ids=[pid])
                    self.assertIn("historical_listings", result["extensions"])
                    self.assertEqual("listing_exchange_changes" in result["extensions"], supports_snapshots)
                    with gzip.open(result["records_file"], "rt") as rows:
                        (record,) = map(json.loads, rows)
                    expected = (
                        [assessment(key)] if key == "TEST" else [later[0], {**later[1], "entry_change": change}]
                    )
                    self.assertEqual(record["historical_listings"], expected)
                    self.assertNotIn("cross_ticker_listing_changes", record)
                    self.assertEqual(record["public_id"], pid)
                    self.assertIn("asset_snapshots", record)
            self.assertNotIn("offline", metadata_projection.require(self.store, batches.keys()))
            with self.assertRaisesRegex(CatalogueError, "catalogue changed"):
                storage.retarget_historical_decisions(self.store, batches, state)

    def test_missing_base_requires_preparation_for_only_its_group(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("DELETE FROM metadata_evidence WHERE group_key='TEST'")
        state = storage.selector_state(self.store)
        batch = {"TEST": self.handoff(data, "TEST")}
        with patch.object(
            metadata_projection, "selected_metadata_receipts", side_effect=AssertionError("read receipts")
        ):
            storage.retarget_historical_decisions(self.store, batch, state)
            self.assertEqual(metadata_projection.prepare(self.store, plan=True)["pending_groups"], 1)
