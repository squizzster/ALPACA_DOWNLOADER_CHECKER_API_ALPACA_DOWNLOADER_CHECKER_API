"""Native standalone producer -> serving catalogue -> public application contract."""

import csv
import io
import json
import tempfile
import unittest
from contextlib import closing
from copy import deepcopy
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from cli_support import parser_for
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from alpaca_end_of_day import (
    API_CHANGE_LOG_PATH,
    API_VERSION,
    CatalogChangedError,
    DataLibrary,
    DataLibraryError,
    PublicIdUnavailableError,
    StandaloneReader,
)
from alpaca_end_of_day.catalogue_cli import run_end_of_day_reader_command
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.contract import NEW_YORK
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import storage
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.sqlite_connections import connect_sqlite
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage


def valid_bars(start="2026-10-01", end="2026-10-14"):
    result = []
    for day in expected_market_session_coverage(start, end).session_dates:
        instant = datetime.combine(date.fromisoformat(day), time(), NEW_YORK).astimezone(timezone.utc)
        result.append(
            {**bar(day), "t": instant.isoformat().replace("+00:00", "Z"), "v": Decimal("9007199254740993.5")}
        )
    return result


class NativeCase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.store = self.directory / "store"
        self.root = self.store / "end_of_day/lineage"
        self.bars = valid_bars()
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.add_symbol("TEST", self.bars)

    def add_symbol(self, symbol, bars=None, *, metadata=None, end="2026-10-14"):
        download_pricing(self.store, symbol, end, client=FakeClient(self.bars if bars is None else bars))
        result = run_lineage(
            self.store, [symbol], identity_client=IdentityClient({symbol: metadata or asset(symbol)})
        )
        self.assertEqual(result["failed"], 0, result)

    def bootstrap(self):
        result = maintain(self.store, bootstrap=True)
        self.assertTrue(result["complete"], result)
        return DataLibrary(store=self.store)

    def publish_change(self, mutate, symbol="TEST"):
        index = lineage.read_index(self.root)
        receipt = lineage.read_receipt(self.root, symbol, index)
        boundaries = lineage.read_boundaries(self.root, receipt)
        mutate(receipt, boundaries)
        for kind, boundary in boundaries.items():
            receipt["boundaries"][kind]["sha256"] = lineage.document_digest(boundary)
        with lineage.pricing_locks(self.store, receipt["input_sources"]):
            lineage.publish(self.store, self.root, index, {symbol: receipt}, {symbol: boundaries}, {})

    def catalogue_groups(self, *keys):
        return storage.read_groups(self.store, keys, generation=storage.selector_state(self.store)["generation"])


class AlpacaEndOfDayPublicApiTests(NativeCase):
    def test_version_and_explicit_migration(self):
        self.assertEqual(API_VERSION, len(json.loads(API_CHANGE_LOG_PATH.read_text())))
        for invoke in (lambda: DataLibrary(self.directory), lambda: DataLibrary(path=self.directory)):
            with self.assertRaisesRegex(TypeError, "store="):
                invoke()
        before = snapshot(self.store)
        with self.assertRaisesRegex(DataLibraryError, "bootstrap"):
            DataLibrary(store=self.store)
        self.assertEqual(snapshot(self.store), before)

    def test_native_exact_history_matches_standalone_and_all_views_share_selection(self):
        data = self.bootstrap()
        (security,) = data.list_securities()
        self.assertEqual(security.names, ("TEST company",))
        self.assertEqual(security.stored_session_count, len(self.bars))
        with patch.object(data._reader, "_snapshot", wraps=data._reader._snapshot) as select:
            views = data.get_histories(security.public_id)
        self.assertEqual(select.call_count, 1)
        expected = StandaloneReader(self.store).get_histories("TEST")
        for name, history in views.items():
            self.assertEqual(history.generation, data.catalog_generation())
            self.assertEqual(
                [str(b.close) for b in history.bars], [b["close"] for b in expected["views"][name]["bars"]]
            )
            self.assertEqual(history.bars[0].volume, Decimal("9007199254740993.5"))
            self.assertEqual(history.to_dict()["bars"][0]["volume"], "9007199254740993.5")
            self.assertEqual(history.bars[0].trade_count, 7)
        filtered = data.get_history(security.public_id.upper(), start="2026-10-02", end=date(2026, 10, 2))
        self.assertEqual(len(filtered.bars), 1)
        self.assertEqual(data.get_history(security.public_id, start="2027-01-01").bars, ())
        with self.assertRaises(ValueError):
            data.get_history(security.public_id, start="2026-10-03", end="2026-10-02")

    def test_open_search_listing_and_unknown_id_use_only_catalogue(self):
        self.bootstrap()
        before = snapshot(self.store)
        original = Path.open
        touched = []

        def opened(path, *args, **kwargs):
            if path.is_relative_to(self.store):
                touched.append(path)
                self.assertTrue(path.is_relative_to(self.store / "end_of_day/catalogue"), path)
            return original(path, *args, **kwargs)

        with (
            patch.object(Path, "open", opened),
            patch.object(Path, "iterdir", side_effect=AssertionError("crawl")),
        ):
            data = DataLibrary(store=self.store)
            self.assertEqual(len(data.search_securities("test company")), 1)
            self.assertEqual(len(data.resolve_ticker("test", on="2026-10-05")), 1)
            self.assertEqual(data.resolve_ticker("TEST", on="2020-01-01"), ())
            self.assertEqual(data.online_count(), 1)
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_history("0" * 64)
        self.assertEqual(snapshot(self.store), before)

    def test_unknown_and_ineligible_ids_are_indistinguishable(self):
        self.add_symbol("BAD", [{**b, "v": -1} for b in self.bars])
        unavailable = StandaloneReader(self.store).resolve_ticker("BAD")["matches"][0]["public_id"]
        data = self.bootstrap()
        self.assertEqual(data.search_securities("BAD"), ())
        self.assertEqual(data.resolve_ticker("BAD"), ())
        self.assertEqual(data.online_count(), 1)
        for public_id in (unavailable, "0" * 64):
            for method in (data.get_security, data.get_history, data.get_histories):
                with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                    method(public_id)

    def test_observations_are_not_historical_csv_values(self):
        data = self.bootstrap()
        (security,) = data.list_securities()
        self.assertTrue(security.observations)
        self.assertTrue(all(o.observed_at is None and o.effective_from is None for o in security.observations))
        output = io.StringIO()
        with patch.object(data._reader, "_snapshot", wraps=data._reader._snapshot) as select:
            receipt = data.write_flat_online_csv(output)
        self.assertEqual(select.call_count, 1)
        rows = list(csv.DictReader(io.StringIO(output.getvalue())))
        self.assertEqual(receipt.row_count, len(self.bars))
        self.assertTrue(
            all(row["ticker"] == "TEST" and row["name"] == row["exchange"] == row["cusip"] == "" for row in rows)
        )
        self.assertEqual(rows[0]["raw_volume"], "9007199254740993.5")
        for internal in ("TEST-id", "source_sha256", "online", str(self.store)):
            self.assertNotIn(internal, json.dumps(security.to_dict()))

    def test_missing_descriptive_metadata_and_inactive_security_can_serve(self):
        self.add_symbol(
            "OLD", metadata={**asset("OLD"), "name": "", "exchange": "", "status": "inactive", "tradable": False}
        )
        data = self.bootstrap()
        matches = data.search_securities("OLD")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].names, ())
        self.assertEqual(matches[0].exchanges, ())

    def test_catalogue_observation_time_does_not_date_exact_fallback(self):
        def change(receipt, _):
            receipt["evidence"]["catalogue_observed_at_utc"] = "2026-10-15T12:00:00Z"
            receipt["evidence"]["asset_lookups"].append(
                {
                    "lookup_kind": "cusip",
                    "lookup": "123456789",
                    "outcome": "found",
                    "asset": asset("TEST"),
                }
            )
            receipt["evidence"]["asset_lookups"][0]["source"] = "asset_catalogue"

        self.publish_change(change)
        (security,) = self.bootstrap().list_securities()
        self.assertIn("123456789", security.cusips)
        self.assertTrue(any(o.observed_at is not None for o in security.observations))
        self.assertTrue(any(o.field == "cusip" and o.observed_at is None for o in security.observations))

    def test_selected_changes_reject_old_instance_unrelated_changes_do_not(self):
        data = self.bootstrap()
        (security,) = data.list_securities()
        self.add_symbol("OTHER")
        self.assertTrue(maintain(self.store)["complete"])
        self.assertNotEqual(DataLibrary(store=self.store).catalog_generation(), data.catalog_generation())
        self.assertEqual(len(data.get_history(security.public_id).bars), len(self.bars))
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["changed"]))
        with self.assertRaises(CatalogChangedError):
            data.get_history(security.public_id)
        self.assertTrue(maintain(self.store)["complete"])
        with self.assertRaises(CatalogChangedError):
            data.get_history(security.public_id)
        self.assertTrue(DataLibrary(store=self.store).get_history(security.public_id).bars)

    def test_changed_pricing_and_pending_publication_reject_without_recovery(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        download_pricing(self.store, "TEST", "2026-10-15", client=FakeClient(valid_bars(end="2026-10-15")))
        before = snapshot(self.store)
        with (
            patch("alpaca_end_of_day.standalone.read_boundaries", side_effect=AssertionError("assignment read")),
            patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("pricing read")),
        ):
            with self.assertRaises(CatalogChangedError):
                data.get_history(public_id)
        self.assertEqual(snapshot(self.store), before)
        lineage.atomic_json(self.root / ".pending.json", {})
        with self.assertRaises(CatalogChangedError):
            data.get_history(public_id)

    def test_changed_serving_decision_without_upstream_change_rejects(self):
        data = self.bootstrap()
        group = deepcopy(self.catalogue_groups("TEST")["TEST"])
        group["decision_generation"] = "new-decision"
        storage.commit(self.store, {"TEST"}, [group])
        with (
            patch("alpaca_end_of_day.standalone.read_boundaries", side_effect=AssertionError("assignment read")),
            patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("pricing read")),
        ):
            with self.assertRaises(CatalogChangedError):
                data.get_history(data.list_securities()[0].public_id)

    def test_csv_failure_preserves_destination_and_stream(self):
        data = self.bootstrap()
        destination = self.directory / "result.csv"
        destination.write_text("original")
        with patch.object(data, "_read_views", side_effect=CatalogChangedError("changed")):
            with self.assertRaises(CatalogChangedError):
                data.write_flat_online_csv(destination)
            stream = io.StringIO()
            with self.assertRaises(CatalogChangedError):
                data.write_flat_online_csv(stream)
            self.assertEqual(stream.getvalue(), "")
        self.assertEqual(destination.read_text(), "original")
        with self.assertRaises(ValueError):
            data.write_flat_online_csv(self.store / "unsafe.csv")

    def test_cli_uses_store_for_read_search_and_export(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        args = parser_for("catalogue", "read").parse_args(["--store", str(self.store), "--public-id", public_id])
        output = io.StringIO()
        run_end_of_day_reader_command(args, store=args.store, output_stream=output)
        self.assertEqual(json.loads(output.getvalue())["security"]["public_id"], public_id)
        args = parser_for("catalogue", "export").parse_args(["--store", str(self.store), "--output", "-"])
        output = io.StringIO()
        run_end_of_day_reader_command(args, store=args.store, output_stream=output)
        self.assertIn("9007199254740993.5", output.getvalue())

    def test_first_and_cached_reads_do_not_authenticate_selected_payloads(self):
        self.bootstrap()
        from libs.alpaca_end_of_day_security_correlation import security_identity

        original_stat = Path.stat
        pricing = self.store / "end_of_day/pricing/TEST"
        assignments = self.store / "end_of_day/lineage/TEST"

        def no_selected_stat(path, *args, **kwargs):
            if path.is_relative_to(pricing) or path.is_relative_to(assignments):
                self.assertEqual(path.name, ".pending.json", f"published input inspected: {path}")
            return original_stat(path, *args, **kwargs)

        for cache in (0, 2 * 1024 * 1024):
            data = DataLibrary(store=self.store, cache_bytes=cache)
            with (
                patch.object(Path, "stat", no_selected_stat),
                patch.object(Path, "iterdir", side_effect=AssertionError("crawl")),
                patch.object(
                    security_identity,
                    "security_id_for_alpaca_asset_ids",
                    side_effect=AssertionError("identity recomputation"),
                ),
                patch(
                    "libs.alpaca_serving_catalogue.producer.evaluate",
                    side_effect=AssertionError("eligibility recomputation"),
                ),
            ):
                first = data.get_history(data.list_securities()[0].public_id)
                second = data.get_history(data.list_securities()[0].public_id)
                self.assertEqual(first, second)

    def test_cache_reuses_selected_payload_by_digest_without_reopening_it(self):
        self.bootstrap()
        data = DataLibrary(store=self.store, cache_bytes=2 * 1024 * 1024)
        public_id = data.list_securities()[0].public_id
        expected = data.get_history(public_id)
        original = Path.read_bytes

        def no_payload(path):
            if path.suffix == ".gz":
                raise AssertionError("cached payload reopened")
            return original(path)

        with patch.object(Path, "read_bytes", no_payload):
            self.assertEqual(data.get_history(public_id), expected)
        self.assertLessEqual(data._reader.cache_info()["used_bytes"], 2 * 1024 * 1024)

    def test_generations_checked_inside_lock_before_assignment_or_price_payloads(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["changed"]))
        with (
            patch("alpaca_end_of_day.standalone.read_boundaries", side_effect=AssertionError("assignment read")),
            patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("pricing read")),
        ):
            with self.assertRaises(CatalogChangedError):
                data.get_history(public_id)

    def test_read_io_is_independent_of_unrelated_group_population(self):
        self.bootstrap()

        def paths_read(data):
            opened = []
            original = Path.read_bytes

            def read(path):
                if path.is_relative_to(self.store):
                    opened.append(path.relative_to(self.store))
                return original(path)

            with patch.object(Path, "read_bytes", read):
                data.get_history(data.search_securities("TEST")[0].public_id)
            return opened

        before = paths_read(DataLibrary(store=self.store))
        for name in ("AAA", "BBB", "CCC"):
            self.add_symbol(name)
        self.assertTrue(maintain(self.store)["complete"])
        after = paths_read(DataLibrary(store=self.store))
        self.assertEqual(before, after)

    def test_v22_lock_protocol_and_decisions_are_reused(self):
        with closing(connect_sqlite(self.root / "index.sqlite3")) as connection, connection:
            connection.execute("PRAGMA user_version=22")
        self.assertTrue(StandaloneReader(self.store).get_history("TEST")["bars"])
        with patch(
            "libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("identity replay")
        ):
            self.assertEqual(self.bootstrap().online_count(), 1)


if __name__ == "__main__":
    unittest.main()
