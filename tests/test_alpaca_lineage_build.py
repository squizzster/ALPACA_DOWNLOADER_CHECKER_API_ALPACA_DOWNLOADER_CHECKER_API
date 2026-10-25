import copy
import io
import json
import multiprocessing
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from cli_support import lineage_main as main
from cli_support import lineage_parser as build_parser
from test_alpaca_pricing_download import FakeClient, bar, snapshot
from test_alpaca_security_lineage import FakeIdentityClient

from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import validate_version, versions
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.reader import read_lineage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.storage import initialize_store, ticker_lock
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage


def asset(symbol, identity=None):
    return {
        "id": identity or symbol + "-id",
        "symbol": symbol,
        "class": "us_equity",
        "exchange": "NASDAQ",
        "name": symbol + " company",
        "status": "active",
        "tradable": True,
    }


class IdentityClient:
    def __init__(self, assets=None, actions=()):
        self.assets = assets if assets is not None else {"GOOG": asset("GOOG")}
        self.actions = list(actions)
        self.calls = []
        self.request_count = 0

    def get_corporate_actions(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        self.request_count += 1
        return [
            copy.deepcopy(item)
            for item in self.actions
            if symbol in (value for key, value in item.items() if key.endswith("symbol"))
            and start <= item["process_date"] <= end
        ]

    def get_asset_lookup(self, identifier):
        self.request_count += 1
        value = self.assets.get(identifier)
        return {
            "outcome": "found" if value else "not_found",
            "http_status": 200 if value else 404,
            "asset": copy.deepcopy(value),
        }


def calendar_bars(start, end):
    return [bar(day) for day in expected_market_session_coverage(start, end).session_dates]


def informative_bars(start, end):
    return [
        {
            **bar(day, str(10 + int(day[-2:]))),
            "t": datetime.fromisoformat(day)
            .replace(tzinfo=NEW_YORK)
            .astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        for day in expected_market_session_coverage(start, end).session_dates
    ]


def lineage_worker(store, ready, entered, release):
    class BlockingIdentity(IdentityClient):
        def get_corporate_actions(self, *args):
            entered.set()
            if not release.wait(15):
                raise RuntimeError("test release timed out")
            return super().get_corporate_actions(*args)

    ready.set()
    result = run_lineage(store, ["GOOG"], identity_client=BlockingIdentity())
    if result["failed"]:
        raise RuntimeError(result)


class LineageBuildTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.bars = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-01-02", "2026-09-30", "2026-10-01")]

    def download(self, symbol="GOOG", end="2026-10-14", bars=None, **kwargs):
        return download_pricing(
            self.store, symbol, end, client=FakeClient(self.bars if bars is None else bars), **kwargs
        )

    def run_build(self, symbols=None, client=None, **kwargs):
        result = run_lineage(self.store, symbols, identity_client=client or IdentityClient(), **kwargs)
        self.assertEqual(result["failed"], 0, result)
        return result

    def receipt(self, symbol="GOOG"):
        return storage.read_receipt(self.root, symbol, storage.read_index(self.root))

    def install_legacy_rule_contract(
        self,
        symbol="GOOG",
        *,
        lineage_version=32,
        correlation_version=58,
    ):
        """Install one complete synthetic group as a genuine policy-less handoff."""
        index = storage.read_index(self.root)
        group = set(index["symbols"][symbol]["group_members"])
        receipts = {
            name: copy.deepcopy(storage.read_receipt(self.root, name, index))
            for name in sorted(group & index["symbols"].keys())
        }
        legacy_versions = {
            **versions(),
            "lineage": lineage_version,
            "correlation": correlation_version,
        }
        legacy_versions.pop("correlation_policy_sha256")
        updates = {}
        for name, receipt in receipts.items():
            receipt.update(schema_version=lineage_version, versions=copy.deepcopy(legacy_versions))
            receipt.pop("correlation_policy")
            entry = copy.deepcopy(index["symbols"][name])
            entry.update(sha256=storage.document_digest(receipt), versions=copy.deepcopy(legacy_versions))
            entry.pop("correlation_policy")
            updates[name] = entry
        journal = {
            "schema_version": lineage_version,
            "boundary": "alpaca_lineage_transaction",
            "documents": {f"{name}/lineage.manifest.json": receipt for name, receipt in receipts.items()},
            "retire": [],
            "index_updates": updates,
            "sources": {name: receipt["source_sha256"] for name, receipt in receipts.items()},
            "unavailable_pricing": sorted(group - receipts.keys()),
            "notices": {},
        }
        with (
            storage.pricing_locks(self.store, group),
            storage.publication_lock(self.root, exclusive=True),
        ):
            storage._install(self.root, journal, compare=False)
        return receipts

    def notice(self, symbol="GOOG"):
        return self.store / f"end_of_day/manifests/{symbol}/update.json"

    def assignments(self, symbol="GOOG", adjustment="raw"):
        receipt = self.receipt(symbol)
        boundaries = storage.read_boundaries(self.root, receipt)
        segments = []
        for kind in ("completed_years", "current_year", "current_month"):
            selected = next(item for item in boundaries[kind]["adjustments"] if item["adjustment"] == adjustment)
            segments.extend(
                segment
                for segment in selected["segments"]
                if kind != "current_year" or segment["from"][:7] < receipt["anchor_month"]
            )
        return segments

    def test_initial_publication_is_three_boundaries_without_price_copies(self):
        self.download()
        before = snapshot(self.store / "end_of_day/pricing")
        result = self.run_build()
        self.assertEqual(result["workers"], 1)
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing"))
        self.assertFalse(self.notice().exists())
        self.assertEqual(
            set(snapshot(self.root)),
            {
                ".worker.lock",
                "index.sqlite3",
                "GOOG/lineage.manifest.json",
                "GOOG/completed_years.manifest.json",
                "GOOG/2026/year.manifest.json",
                "GOOG/2026/Oct/month.manifest.json",
            },
        )
        receipt = self.receipt()
        self.assertEqual(receipt["source_rows"], 8 * len(self.bars))
        self.assertTrue(receipt["row_partition_complete"])
        self.assertEqual(sum(segment["row_count"] for segment in self.assignments()), len(self.bars))
        self.assertNotIn('"open"', json.dumps(receipt))

    def test_daily_append_retains_cold_and_year_and_resumes_metadata_month(self):
        self.bars = [bar("2016-01-04")] + calendar_bars("2025-12-31", "2026-10-14")
        self.download()
        self.run_build()
        before = snapshot(self.root)
        self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
        client = IdentityClient()
        self.run_build(client=client)
        after = snapshot(self.root)
        for filename in ("GOOG/completed_years.manifest.json", "GOOG/2026/year.manifest.json"):
            self.assertTrue(before[filename] == after[filename], filename)
        self.assertEqual(client.calls, [("GOOG", "2026-10-01", "2026-10-15")])
        self.assertEqual(sum(item["row_count"] for item in self.assignments()), len(self.bars) + 1)

    def test_unchanged_selected_source_has_no_requests_or_writes(self):
        self.download()
        self.run_build()
        before = snapshot(self.root)
        client = IdentityClient()
        result = self.run_build(["GOOG"], client=client)
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(client.request_count, 0)
        self.assertEqual(before, snapshot(self.root))

    def test_rule_decisions_are_shared_within_one_build_only(self):
        from libs.alpaca_end_of_day_security_correlation import row_correlation
        from libs.alpaca_lineage_build import policy_engine

        self.download()
        build_report = policy_engine.build_security_correlation_report
        with (
            patch.object(row_correlation, "_decide_row", wraps=row_correlation._decide_row) as decide,
            patch.object(
                row_correlation,
                "build_ticker_ownership_intervals",
                wraps=row_correlation.build_ticker_ownership_intervals,
            ) as intervals,
            patch.object(
                row_correlation,
                "build_uuid_lineage_edges",
                wraps=row_correlation.build_uuid_lineage_edges,
            ) as edges,
            patch.object(policy_engine, "build_security_correlation_report", wraps=build_report) as build,
        ):
            self.run_build()
            self.assertEqual(len(self.bars), decide.call_count)
            self.assertEqual(2, build.call_count)
            self.assertEqual(1, intervals.call_count)
            self.assertEqual(1, edges.call_count)
            decide.reset_mock()
            build.reset_mock()
            intervals.reset_mock()
            edges.reset_mock()
            self.run_build()
            self.assertEqual(0, decide.call_count)
            self.assertEqual(0, build.call_count)
            self.assertEqual(0, intervals.call_count)
            self.assertEqual(0, edges.call_count)
            self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
            self.run_build()
            self.assertEqual(len(self.bars) + 1, decide.call_count)
            self.assertEqual(2, build.call_count)
            self.assertEqual(1, intervals.call_count)
            self.assertEqual(1, edges.call_count)

    def test_repeated_arrival_does_not_create_false_cross_source_identity_partition(self):
        # A later duplicate transition must not manufacture an unresolved gap
        # and then let the next cascade pass split the accepted UUID membership.
        before = informative_bars("2022-12-08", "2023-12-13")
        after = informative_bars("2023-12-13", "2023-12-22")
        self.download("OLD", end="2024-01-01", bars=before)
        self.download("NEW", end="2024-01-01", bars=before + after)
        old, new = asset("OLD"), asset("NEW")
        client = IdentityClient(
            {"OLD": old, "NEW": new, "111111111": old, "222222222": new},
            [
                {
                    "action_type": "name_change",
                    "id": day,
                    "process_date": day,
                    "old_symbol": "OLD",
                    "new_symbol": "NEW",
                    "old_cusip": "111111111",
                    "new_cusip": "222222222",
                }
                for day in ("2023-12-13", "2023-12-21")
            ],
        )
        pricing_before = snapshot(self.store / "end_of_day/pricing")
        result = self.run_build(["NEW"], client=client)
        self.assertEqual(["NEW", "OLD"], result["results"][0]["processed"])
        self.assertEqual(pricing_before, snapshot(self.store / "end_of_day/pricing"))
        expected_id = security_id_for_alpaca_asset_ids([old["id"], new["id"]])
        for symbol in ("OLD", "NEW"):
            self.assertEqual(0, self.receipt(symbol)["unresolved_rows"])
            for adjustment in ADJUSTMENTS:
                segments = self.assignments(symbol, adjustment)
                self.assertEqual(
                    len(before) + (len(after) if symbol == "NEW" else 0), sum(s["row_count"] for s in segments)
                )
                for segment in segments:
                    historical = "OLD" if segment["to"] < "2023-12-13" else "NEW"
                    self.assertEqual(historical, segment["historical_symbol"])
                    self.assertEqual(historical + "-id", segment["canonical_security"]["alpaca_asset_id"])
                    self.assertEqual(expected_id, segment["security_identity"]["security_id"])
            self.assertFalse(self.notice(symbol).exists())
        published = snapshot(self.root)
        self.run_build(["NEW"], client=client)
        self.assertEqual(published, snapshot(self.root))

    def test_normal_lineage_trusts_pricing_without_calling_authenticators(self):
        self.download()
        with (
            patch("libs.alpaca_pricing_download.storage.read_boundary", side_effect=AssertionError("audit")),
            patch("libs.alpaca_pricing_download.storage.boundary_document", side_effect=AssertionError("audit")),
            patch("libs.alpaca_pricing_download.storage.digest", side_effect=AssertionError("rehash")),
        ):
            self.run_build()

    def test_simple_continuation_never_opens_cold_prices_or_recalculates_old_rows(self):
        self.bars = calendar_bars("2025-12-01", "2026-10-14")
        self.download()
        self.run_build()
        before = snapshot(self.root)
        self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
        original = Path.read_bytes

        def read_bytes(path):
            if path.suffix == ".gz" and path.parent.name != "Oct":
                self.fail("approved cold pricing was reopened")
            return original(path)

        from libs.alpaca_lineage_build import policy_engine

        correlate = policy_engine.build_security_correlation_report

        def check_rows(*args, **kwargs):
            reader = kwargs["authenticated_download_reader"]
            for descriptor in args[0]["local"]["downloads"]:
                rows = reader(None, None, descriptor).rows
                self.assertTrue(all(row["t"].startswith("2026-10") for row in rows))
            return correlate(*args, **kwargs)

        with (
            patch.object(Path, "read_bytes", read_bytes),
            patch.object(policy_engine, "build_security_correlation_report", side_effect=check_rows),
        ):
            result = self.run_build()
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        after = snapshot(self.root)
        for name in ("GOOG/completed_years.manifest.json", "GOOG/2026/year.manifest.json"):
            self.assertTrue(before[name] == after[name], name)
        self.assertEqual(sum(item["row_count"] for item in self.assignments()), len(self.bars) + 1)

    def test_month_and_year_rollover_retire_only_superseded_lineage_boundaries(self):
        self.download()
        self.run_build()
        self.download(end="2026-11-03", bars=self.bars + [bar("2026-11-02")])
        self.run_build()
        self.assertFalse((self.root / "GOOG/2026/Oct").exists())
        self.assertTrue((self.root / "GOOG/2026/Nov/month.manifest.json").exists())
        self.download(end="2027-01-05", bars=self.bars + [bar("2026-11-02"), bar("2027-01-04")])
        self.run_build()
        self.assertFalse((self.root / "GOOG/2026").exists())
        self.assertTrue((self.root / "GOOG/2027/Jan/month.manifest.json").exists())
        self.assertEqual(sum(item["row_count"] for item in self.assignments()), len(self.bars) + 2)

    def test_force_refresh_deletions_remove_old_assignments(self):
        self.download()
        self.run_build()
        self.download(bars=[bar("2026-10-01")], refresh_all=True)
        self.run_build()
        self.assertEqual(self.receipt()["source_rows"], 8)
        self.assertEqual([item["from"] for item in self.assignments()], ["2026-10-01"])
        self.download(bars=[], refresh_all=True)
        self.run_build()
        self.assertEqual(self.assignments(), [])
        self.assertFalse(self.notice().exists())

    def test_metadata_only_refresh_can_correct_old_boundaries(self):
        self.download()
        self.run_build()
        before = snapshot(self.root)
        client = IdentityClient({"GOOG": asset("GOOG", "corrected-id")})
        self.run_build(["GOOG"], client=client, refresh=True)
        self.assertEqual(client.calls, [("GOOG", "2016-01-01", "2026-10-14")])
        self.assertNotEqual(
            before["GOOG/completed_years.manifest.json"][0],
            snapshot(self.root)["GOOG/completed_years.manifest.json"][0],
        )
        self.assertFalse(self.notice().exists())

    def test_failure_does_not_consume_notice_and_other_tickers_continue(self):
        self.download()
        self.download("OTHER")
        client = IdentityClient({"GOOG": asset("GOOG"), "OTHER": asset("OTHER")})
        original = client.get_corporate_actions

        def acquire(symbol, start, end):
            if symbol == "GOOG":
                raise RuntimeError("identity unavailable")
            return original(symbol, start, end)

        client.get_corporate_actions = acquire
        result = run_lineage(self.store, ["GOOG", "OTHER"], identity_client=client)
        self.assertEqual(result["failed"], 1)
        self.assertTrue(self.notice().exists())
        self.assertFalse(self.notice("OTHER").exists())

    def test_missing_pricing_is_delegated_outside_locks_then_notice_is_consumed(self):
        self.download()
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-09-01",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = IdentityClient(
            {"GOOG": asset("GOOG"), "OLD": asset("GOOG"), "111111111": asset("GOOG")}, [action]
        )
        requested = []

        def acquire(store, symbol, end, **kwargs):
            # A nonblocking separate descriptor must acquire the source lock here.
            import fcntl

            with (self.store / "end_of_day/pricing/GOOG/.download.lock").open("rb") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            requested.append(symbol)
            result = download_pricing(store, symbol, end, **kwargs)
            self.assertTrue(self.notice(symbol).exists())
            return result

        with patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=acquire):
            result = self.run_build(["GOOG"], client=client, pricing_client=FakeClient(self.bars))
        self.assertEqual(requested, ["OLD"])
        self.assertEqual(result["results"][0]["processed"], ["GOOG", "OLD"])
        self.assertFalse(self.notice("OLD").exists())
        self.assertFalse(self.notice().exists())

    def test_known_lagging_dependency_checks_retained_history_before_advancing(self):
        initial = self.bars
        self.download(end="2026-10-15", bars=initial)
        self.download("OLD", end="2026-10-15", bars=initial)
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-09-01",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        identities = IdentityClient(
            {"GOOG": asset("GOOG"), "OLD": asset("GOOG"), "111111111": asset("GOOG")}, [action]
        )
        self.run_build(["GOOG"], client=identities)
        added = [bar("2026-10-15"), bar("2026-10-16")]
        self.download(end="2026-10-17", bars=initial + added)
        calls = []
        pricing = BarsClient("test", "test")

        def page(params):
            calls.append(dict(params))
            start, end = timestamp(params["start"]), timestamp(params["end"])
            return {
                "bars": {
                    symbol: [row for row in initial + added if start <= timestamp(row["t"]) <= end]
                    for symbol in params["symbols"].split(",")
                }
            }

        with (
            patch.object(pricing, "_get", side_effect=page),
            patch(
                "libs.alpaca_lineage_build.pipeline.download_pricing",
                side_effect=AssertionError("known dependency used the single-ticker fallback"),
            ),
        ):
            result = self.run_build(["GOOG"], client=identities, pricing_client=pricing)
        self.assertEqual(result["dependency_pricing"]["requested"], 1)
        self.assertEqual(result["dependency_pricing"]["failed"], 0)
        self.assertGreater(len(calls), 8)
        self.assertTrue(any(call["start"][:10] < "2026-10-15" for call in calls))
        self.assertTrue(all(call["symbols"] == "OLD" for call in calls))
        source = storage.load_source(self.store / "end_of_day/pricing/OLD", "OLD")
        self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")
        self.assertEqual(result["results"][0]["processed"], ["GOOG", "OLD"])

    def test_missing_pricing_without_downloader_retains_notice(self):
        self.download()
        client = IdentityClient({"GOOG": asset("NEW")})
        result = run_lineage(self.store, ["GOOG"], identity_client=client)
        self.assertEqual(result["results"][0]["status"], "waiting_for_pricing")
        self.assertEqual(list(result["results"][0]["required"]), ["NEW"])
        self.assertTrue(self.notice().exists())

    def test_missing_seed_uses_downloader_defaults_outside_locks_then_consumes_notice(self):
        initialize_store(self.store)
        pricing_root = self.store / "end_of_day/pricing/META"
        pricing_root.mkdir(parents=True)
        with ticker_lock(pricing_root, exclusive=True, create=True):
            pass  # A prior failed acquisition may have left only its stable lock.
        requested = []

        def acquire(store, symbol, end, **kwargs):
            import fcntl

            with (pricing_root / ".download.lock").open("rb") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            requested.append((symbol, end, kwargs["from_date"]))
            result = download_pricing(store, symbol, end, **kwargs)
            self.assertTrue(self.notice(symbol).exists())
            return result

        with patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=acquire):
            result = self.run_build(
                ["META"], client=IdentityClient({"META": asset("META")}), pricing_client=FakeClient(self.bars)
            )
        self.assertEqual(requested, [("META", None, None)])
        self.assertEqual(result["results"][0]["pricing_acquisition_waves"], 1)
        self.assertEqual(result["results"][0]["acknowledged"], ["META"])
        self.assertEqual(self.receipt("META")["processed_through"], "2030-01-01T00:00:00Z")
        self.assertFalse(self.notice("META").exists())

    def test_missing_seed_without_downloader_waits_without_identity_calls_or_notices(self):
        initialize_store(self.store)
        client = IdentityClient()
        with patch("libs.alpaca_lineage_build.pipeline.download_pricing") as downloader:
            result = run_lineage(self.store, ["META"], identity_client=client)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][0]["status"], "waiting_for_pricing")
        self.assertEqual(result["results"][0]["required"], {"META": {"from": None, "end_exclusive": None}})
        self.assertEqual(client.request_count, 0)
        downloader.assert_not_called()
        self.assertFalse(self.notice("META").exists())
        self.assertFalse((self.store / "end_of_day/pricing/META").exists())

    def test_missing_seed_failure_does_not_stop_other_tickers(self):
        self.download()
        with patch(
            "libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=RuntimeError("unavailable")
        ):
            result = run_lineage(
                self.store, ["META", "GOOG"], identity_client=IdentityClient(), pricing_client=FakeClient([])
            )
        self.assertEqual(result["failed"], 1)
        self.assertEqual([item["status"] for item in result["results"]], ["failed", "processed"])
        self.assertFalse(self.notice().exists())
        self.assertIsNone(self.receipt("META"))

    def test_missing_seed_acquisition_is_bounded(self):
        initialize_store(self.store)
        client = IdentityClient()
        with patch("libs.alpaca_lineage_build.pipeline.download_pricing", return_value={}) as downloader:
            result = run_lineage(self.store, ["META"], identity_client=client, pricing_client=FakeClient([]))
        self.assertEqual(downloader.call_count, 3)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(client.request_count, 0)

    def test_empty_seed_is_published_once_and_not_requested_again(self):
        initialize_store(self.store)
        client = IdentityClient({"META": asset("META")})
        pricing = FakeClient([])
        result = self.run_build(["META"], client=client, pricing_client=pricing)
        self.assertEqual(result["results"][0]["pricing_acquisition_waves"], 1)
        self.assertEqual(self.receipt("META")["source_rows"], 0)
        before = len(pricing.calls)
        result = self.run_build(["META"], client=client, pricing_client=pricing)
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(len(pricing.calls), before)

    def test_cli_acquires_meta_then_related_prices_before_lineage_publication(self):
        from cli_support import initialize_command_registration

        initialize_command_registration(self)
        prices = {
            "META": informative_bars("2021-07-01", "2026-10-14"),
            "FB": informative_bars("2021-07-01", "2022-06-09") + informative_bars("2025-06-26", "2026-10-14"),
            "METV": informative_bars("2021-07-01", "2026-10-14"),
        }
        clients = {symbol: FakeClient(rows) for symbol, rows in prices.items()}
        requested = []

        class PricingClient:
            def fetch(self, symbol, *args, **kwargs):
                return clients[symbol].fetch(symbol, *args, **kwargs)

        def acquire(store, symbol, end, **kwargs):
            requested.append((symbol, end, kwargs["from_date"]))
            return download_pricing(store, symbol, end, **kwargs)

        output = io.StringIO()
        with (
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2026, 10, 14, 12, tzinfo=timezone.utc),
            ),
            patch.dict("os.environ", {"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"}),
            patch("libs.alpaca_cli.BarsClient", return_value=PricingClient()),
            patch("libs.alpaca_cli.AlpacaIdentityClient", return_value=FakeIdentityClient()),
            patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=acquire),
            redirect_stdout(output),
        ):
            self.assertEqual(main(["--store", str(self.store), "--symbol", "META"]), 0, output.getvalue())
        self.assertEqual(
            requested,
            [
                ("META", "2026-10-14", "2016-01-01"),
                ("FB", "2026-10-14", "2016-01-01"),
                ("METV", "2026-10-14", "2016-01-01"),
            ],
        )
        result = json.loads(output.getvalue())["results"][0]
        self.assertEqual(result["pricing_acquisition_waves"], 2)
        self.assertEqual(result["processed"], ["FB", "META", "METV"])
        self.assertEqual(result["acknowledged"], ["FB", "META", "METV"])
        for symbol in prices:
            self.assertFalse(self.notice(symbol).exists())
        meta = [item for item in self.assignments("META") if item["branch"] == "correlated"]
        self.assertEqual({item["historical_symbol"] for item in meta}, {"FB", "META"})
        self.assertEqual(
            {item["security_identity"]["security_id"] for item in meta},
            {security_id_for_alpaca_asset_ids(["meta-id"])},
        )

    def test_legacy_policy_publication_recovers_and_upgrades_without_reacquiring_prices(self):
        self.download()
        self.run_build()
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "GOOG", index)
        legacy_versions = {**versions(), "lineage": 33, "correlation": 59}
        legacy_versions.pop("correlation_policy_sha256")
        receipt.update(schema_version=33, versions=legacy_versions)
        receipt.pop("correlation_policy")
        entry = {
            **index["symbols"]["GOOG"],
            "sha256": storage.document_digest(receipt),
            "versions": legacy_versions,
        }
        entry.pop("correlation_policy")
        storage.atomic_json(
            self.root / ".pending.json",
            {
                "schema_version": 33,
                "boundary": "alpaca_lineage_transaction",
                "documents": {"GOOG/lineage.manifest.json": receipt},
                "retire": [],
                "index_updates": {"GOOG": entry},
                "sources": {"GOOG": receipt["source_sha256"]},
                "unavailable_pricing": [],
                "notices": {},
            },
        )
        before = snapshot(self.store / "end_of_day/pricing")
        with patch(
            "libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=AssertionError("download")
        ):
            result = self.run_build(["GOOG"])
        self.assertTrue(result["recovered"])
        self.assertEqual(self.receipt()["schema_version"], storage.VERSION)
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing"))
        self.assertFalse(self.notice().exists())

    def test_new_related_notice_reprocesses_previously_acknowledged_source(self):
        self.download()
        self.download("OLD")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2022-06-09",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = IdentityClient(
            {"GOOG": asset("GOOG"), "OLD": asset("GOOG"), "111111111": asset("GOOG")}, [action]
        )
        self.run_build(["GOOG"], client=client)
        self.assertFalse(self.notice().exists())
        self.download("OLD", refresh_all=True)
        result = self.run_build(client=client)
        self.assertEqual(result["results"][0]["processed"], ["GOOG", "OLD"])
        self.assertEqual(result["results"][0]["acknowledged"], ["OLD"])

    def test_publish_failure_recovers_before_ack_without_reacquiring_metadata(self):
        self.download()
        with patch.object(storage, "_install", side_effect=OSError("disk failed")):
            result = run_lineage(self.store, identity_client=IdentityClient())
        self.assertEqual(result["failed"], 1)
        self.assertTrue(self.notice().exists())
        client = IdentityClient()
        result = self.run_build(client=client)
        self.assertTrue(result["recovered"])
        self.assertEqual(client.request_count, 0)
        self.assertFalse(self.notice().exists())
        self.assertFalse((self.root / ".pending.json").exists())

    def test_ack_failure_is_idempotently_recovered(self):
        self.download()
        with patch.object(storage, "acknowledge_update", side_effect=OSError("interrupted ack")):
            result = run_lineage(self.store, identity_client=IdentityClient())
        self.assertEqual(result["failed"], 1)
        before = snapshot(self.root / "GOOG")
        self.run_build()
        self.assertEqual(before, snapshot(self.root / "GOOG"))
        self.assertFalse(self.notice().exists())

    def test_recovery_does_not_ack_newer_pricing_with_identical_notice_dates(self):
        self.download()
        with patch.object(storage, "_acknowledge", side_effect=OSError("crash")):
            run_lineage(self.store, identity_client=IdentityClient())
        self.download(bars=[{**item, "c": 42} for item in self.bars], refresh_all=True)
        with storage.writer_lock(self.store) as root:
            storage.recover(self.store, root)
        self.assertTrue(self.notice().exists())
        self.run_build()
        self.assertFalse(self.notice().exists())

    def test_fb_meta_rename_and_reused_fb_remain_distinct(self):
        prices = {
            "META": informative_bars("2021-07-01", "2026-10-14"),
            "FB": informative_bars("2021-07-01", "2022-06-09") + informative_bars("2025-06-26", "2026-10-14"),
            "METV": informative_bars("2021-07-01", "2026-10-14"),
        }
        for symbol, bars in prices.items():
            self.download(symbol, bars=bars)
        client = FakeIdentityClient()
        self.run_build(["META"], client=client)
        meta = [item for item in self.assignments("META") if item["branch"] == "correlated"]
        fb = [item for item in self.assignments("FB") if item["branch"] == "correlated"]
        self.assertEqual({item["historical_symbol"] for item in meta}, {"FB", "META"})
        self.assertEqual(
            {item["security_identity"]["security_id"] for item in meta},
            {security_id_for_alpaca_asset_ids(["meta-id"])},
        )
        self.assertEqual(
            {item["security_identity"]["security_id"] for item in fb},
            {security_id_for_alpaca_asset_ids(["fb-etf-id"]), security_id_for_alpaca_asset_ids(["meta-id"])},
        )
        for item in fb:
            expected = "meta-id" if item["to"] < "2022-06-09" else "fb-etf-id"
            self.assertEqual([expected], item["security_identity"]["member_alpaca_asset_ids"])

    def test_only_one_lineage_worker_and_ticker_lock_blocks_provider_calls(self):
        self.download()
        ctx = multiprocessing.get_context("fork")
        ready, entered, release = ctx.Event(), ctx.Event(), ctx.Event()
        worker = ctx.Process(target=lineage_worker, args=(str(self.store), ready, entered, release))
        with ticker_lock(self.store / "end_of_day/pricing/GOOG", exclusive=True):
            worker.start()
            self.assertTrue(ready.wait(5))
            self.assertFalse(entered.wait(0.2))
        self.assertTrue(entered.wait(5))
        ready2, entered2, release2 = ctx.Event(), ctx.Event(), ctx.Event()
        second = ctx.Process(target=lineage_worker, args=(str(self.store), ready2, entered2, release2))
        second.start()
        try:
            self.assertTrue(ready2.wait(5))
            self.assertFalse(entered2.wait(0.2))
            release.set()
            release2.set()
            worker.join(15)
            second.join(15)
            self.assertEqual(worker.exitcode, 0)
            self.assertEqual(second.exitcode, 0)
        finally:
            for process in (worker, second):
                if process.is_alive():
                    process.terminate()
                process.join(5)
                process.close()

    def test_cli_selection_and_one_worker_constraint(self):
        from cli_support import initialize_command_registration

        from libs.alpaca_cli import main as canonical_main

        initialize_command_registration(self)
        validate_version()
        parser = build_parser()
        self.assertFalse(hasattr(parser.parse_args(["--store", "example"]), "workers"))
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["--store", "example", "--workers", "2"])
        output = io.StringIO()
        with (
            patch.dict("os.environ", {"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"}),
            patch(
                "libs.alpaca_lineage_build.pipeline.run_lineage", return_value={"failed": 0, "results": []}
            ) as run,
            redirect_stdout(output),
        ):
            self.assertEqual(
                canonical_main(
                    [
                        "lineage",
                        "refresh",
                        "--store",
                        str(self.store),
                        "--symbol",
                        "GOOG",
                        "--no-pricing-acquisition",
                    ]
                ),
                0,
            )
        self.assertEqual(run.call_args.args, (self.store, ["GOOG"]))
        self.assertIsNone(run.call_args.kwargs["pricing_client"])
        self.assertTrue(run.call_args.kwargs["refresh"])

    def test_full_refresh_does_not_read_other_runtime_store(self):
        self.download()
        from contextlib import ExitStack

        def guard(method):
            def selected_path(path, *args, **kwargs):
                self.assertNotIn(("data", "end_of_day"), zip(path.parts, path.parts[1:]), "legacy path")
                return method(path, *args, **kwargs)

            return selected_path

        with ExitStack() as stack:
            for name in ("open", "stat", "iterdir", "glob"):
                stack.enter_context(patch.object(Path, name, guard(getattr(Path, name))))
            self.run_build(["GOOG"], refresh=True)

    def test_lineage_handoff_trusts_assignments_and_rejects_changed_input_generation(self):
        self.download()
        self.run_build()
        from libs.alpaca_lineage_build import pipeline

        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("repeated identity rules")),
            patch(
                "libs.alpaca_pricing_download.handoff.load_published_views",
                side_effect=AssertionError("pricing read"),
            ),
        ):
            result = read_lineage(self.store, "GOOG")
        self.assertEqual(sum(item["row_count"] for item in result["adjustments"]["raw"]), len(self.bars))
        self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
        with self.assertRaisesRegex(RuntimeError, "awaits changed pricing"):
            read_lineage(self.store, "GOOG")

    def test_partial_group_publication_blocks_handoff_and_recovery_finishes_it(self):
        self.download()
        original = storage.atomic_json

        def interrupted(path, document, **kwargs):
            if path.name == "lineage.manifest.json":
                raise OSError("interrupted after boundary files")
            return original(path, document, **kwargs)

        with patch.object(storage, "atomic_json", side_effect=interrupted):
            result = run_lineage(self.store, identity_client=IdentityClient())
        self.assertEqual(result["failed"], 1)
        with self.assertRaisesRegex(RuntimeError, "publication incomplete"):
            read_lineage(self.store, "GOOG")
        self.run_build()
        self.assertTrue(read_lineage(self.store, "GOOG")["receipt"]["row_partition_complete"])

    def test_missing_dependency_acquisition_is_bounded_and_retains_pending_work(self):
        self.download()
        client = IdentityClient({"GOOG": asset("NEW")})
        with patch("libs.alpaca_lineage_build.pipeline.download_pricing", return_value={}) as downloader:
            result = run_lineage(self.store, ["GOOG"], identity_client=client, pricing_client=FakeClient([]))
        self.assertEqual(downloader.call_count, 3)
        self.assertEqual(result["failed"], 1)
        self.assertTrue(self.notice().exists())

    def test_empty_published_dependency_is_not_requested_again(self):
        self.download()
        client = IdentityClient({"GOOG": asset("NEW"), "NEW": asset("NEW")})
        result = self.run_build(["GOOG"], client=client, pricing_client=FakeClient([]))
        self.assertEqual(result["results"][0]["pricing_acquisition_waves"], 1)
        self.assertEqual(self.receipt("NEW")["source_rows"], 0)
        self.assertFalse(self.notice().exists())

    def test_conflicting_action_identity_fails_without_assigning_or_acknowledging(self):
        self.download()
        event = {
            "action_type": "name_change",
            "id": "conflicting",
            "process_date": "2026-09-01",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = IdentityClient(actions=[event, {**event, "process_date": "2026-09-02"}])
        result = run_lineage(self.store, identity_client=client)
        self.assertEqual(result["failed"], 1)
        self.assertIn("conflicting corporate-action", result["results"][0]["error"])
        self.assertTrue(self.notice().exists())

    def test_delegation_reloads_the_latest_notice_after_reacquiring_locks(self):
        self.download()
        client = IdentityClient({"GOOG": asset("NEW"), "NEW": asset("NEW")})
        count = 0

        def acquire(store, symbol, end, **kwargs):
            nonlocal count
            count += 1
            if count == 1:
                self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
            return download_pricing(store, symbol, end, **kwargs)

        with patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=acquire):
            self.run_build(["GOOG"], client=client, pricing_client=FakeClient(self.bars + [bar("2026-10-14")]))
        self.assertEqual(self.receipt()["processed_through"], "2026-10-15T00:00:00Z")
        self.assertEqual(self.receipt("NEW")["processed_through"], "2026-10-15T00:00:00Z")
        self.assertFalse(self.notice().exists())
        self.assertFalse(self.notice("NEW").exists())
