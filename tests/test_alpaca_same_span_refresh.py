import copy
import io
import json
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_pricing_download import snapshot as files_snapshot

from libs.alpaca_cli import main
from libs.alpaca_lineage_build import check_summary as projection
from libs.alpaca_lineage_build import refresh
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import check_summary as summary
from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, DownloadError, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.refresh import prepare_refresh, replace_prepared


class SameSpanRefreshTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dates = patch(
            "libs.alpaca_pricing_download.download.utc_now",
            return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
        )
        dates.start()
        cls.addClassCleanup(dates.stop)
        cls.fixture = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.base = Path(cls.fixture.name) / "base"
        cls.rows = [bar(day) for day in ("2026-10-01", "2026-10-02", "2026-10-13")]
        cls.assets = {name: asset(name) for name in ("AAA", "BBB", "CCC")}
        for name, start, end in (
            ("AAA", "2026-10-01", "2026-10-15"),
            ("BBB", "2026-10-03", "2026-10-15"),
            ("CCC", "2026-10-01", "2026-10-14"),
        ):
            download_pricing(cls.base, name, end, from_date=start, client=FakeClient(cls.rows))
        result = run_lineage(cls.base, identity_client=IdentityClient(cls.assets))
        if result["failed"]:
            raise AssertionError(result)
        projection.rebuild(cls.base)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        shutil.copytree(self.base, self.store)
        self.calls = []
        self.live = {
            "AAA": [bar("2026-10-01", "456"), {**bar("2026-10-13"), "v": 42}, bar("2026-10-15")],
            "BBB": [{**bar("2026-10-13"), "v": 42}, bar("2026-10-15")],
            "CCC": copy.deepcopy(self.rows),
        }
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.calls.append(dict(params))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                symbol: [row for row in self.live[symbol] if start <= timestamp(row["t"]) <= end]
                for symbol in params["symbols"].split(",")
            }
        }

    def root(self, symbol):
        return self.store / "end_of_day/pricing" / symbol

    def run_refresh(self):
        return refresh.refresh_existing(
            self.store, pricing_client=self.client, identity_client=IdentityClient(self.assets)
        )

    def test_real_pipeline_replaces_only_changes_same_spans_and_can_append_later(self):
        previous = {name: storage.load_source(self.root(name), name) for name in self.assets}
        unchanged = files_snapshot(self.root("CCC"))
        result = self.run_refresh()
        self.assertTrue(result["complete"], result)
        self.assertEqual((result["check"]["stats"]["matched"], result["check"]["stats"]["differed"]), (1, 2))
        self.assertEqual(result["check"]["stats"]["other_fields_only"], 2)
        self.assertEqual({item["symbol"] for item in result["pricing"]["results"]}, {"AAA", "BBB"})
        self.assertEqual(result["pricing"]["requests"]["page_requests"], 16)
        self.assertEqual(files_snapshot(self.root("CCC")), unchanged)
        for name in ("AAA", "BBB"):
            source = storage.load_source(self.root(name), name)
            self.assertEqual(source["history_start"], previous[name]["history_start"])
            for key in ("end_exclusive", "asof"):
                self.assertEqual(source["latest_request"][key], previous[name]["latest_request"][key])
            self.assertFalse((self.store / f"end_of_day/manifests/{name}/update.json").exists())
        rows = read_pricing_views(self.store, "AAA")["all"]
        self.assertEqual([row[0][:10] for row in rows], ["2026-10-01", "2026-10-13"])
        self.assertEqual(rows[0][1], "456")
        self.assertEqual(
            {row["symbol"]: row["last_all"][5] for row in summary.read_snapshot(self.store)["entries"]},
            {"AAA": "42", "BBB": "42", "CCC": "9007199254740993"},
        )
        self.assertEqual(self.run_refresh()["pricing"]["results"], [])
        appended = download_pricing(self.store, "AAA", "2026-10-16", client=FakeClient(self.live["AAA"]))
        self.assertEqual(appended["mode"], "update")
        self.assertEqual(read_pricing_views(self.store, "AAA")["all"][-1][0][:10], "2026-10-15")

    def test_all_matches_uses_summary_only_and_writes_nothing(self):
        self.live = {name: copy.deepcopy(self.rows) for name in self.assets}
        before = files_snapshot(self.store)
        with (
            patch.object(storage, "load_source", side_effect=AssertionError("ticker receipt read")),
            patch.object(Path, "iterdir", side_effect=AssertionError("directory scan")),
            patch.object(refresh, "run_lineage", side_effect=AssertionError("unnecessary lineage")),
        ):
            result = self.run_refresh()
        self.assertTrue(result["complete"])
        self.assertEqual(result["check"]["stats"]["matched"], 3)
        self.assertEqual(files_snapshot(self.store), before)

    def test_omitted_batch_member_cannot_erase_a_nonempty_replacement(self):
        unchanged = files_snapshot(self.root("CCC"))

        def omitted(params):
            response = self.page(params)
            if (
                "," in params["symbols"]
                and params["start"][:10] <= "2026-10-03"
                and params["end"][:19] == "2026-10-14T23:59:59"
            ):
                response["bars"].pop("BBB", None)
            return response

        with patch.object(self.client, "_get", side_effect=omitted):
            result = self.run_refresh()
        self.assertTrue(result["complete"], result)
        self.assertTrue(all(item["has_data"] for item in result["pricing"]["results"]))
        for rows in read_pricing_views(self.store, "BBB").values():
            self.assertEqual([row[0][:10] for row in rows], ["2026-10-13"])
            self.assertEqual(rows[0][5], "42")
        exact_histories = [
            call
            for call in self.calls
            if "," not in call["symbols"]
            and call["start"][:10] <= "2026-10-03"
            and call["end"][:19] == "2026-10-14T23:59:59"
        ]
        self.assertEqual({call["symbols"] for call in exact_histories}, {"BBB"})
        self.assertEqual(len(exact_histories), len(ADJUSTMENTS))
        self.assertEqual(files_snapshot(self.root("CCC")), unchanged)

    def test_incomplete_live_check_publishes_nothing(self):
        self.live["BBB"] = []
        before = files_snapshot(self.store)
        with self.assertRaisesRegex(DownloadError, "live check incomplete"):
            self.run_refresh()
        self.assertEqual(files_snapshot(self.store), before)

    def test_changed_summary_during_acquisition_prevents_first_replacement(self):
        entries = {item["symbol"]: item for item in summary.read_snapshot(self.store)["entries"]}
        before = files_snapshot(self.store / "end_of_day")
        changed = False

        def concurrent(params):
            nonlocal changed
            if params["start"][:10] == "2026-10-01" and not changed:
                changed = True
                summary.invalidate(self.store, ["AAA"])
                summary.replace_entries(self.store, {"AAA": entries["AAA"]}, pricing_only=True)
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=concurrent):
            with self.assertRaisesRegex(DownloadError, "baseline changed"):
                self.run_refresh()
        self.assertTrue(changed)
        self.assertEqual(files_snapshot(self.store / "end_of_day"), before)

    def test_newer_source_cannot_be_overwritten_by_prepared_replacement(self):
        prepared = prepare_refresh(self.store, ["AAA"])
        download_pricing(self.store, "AAA", "2026-10-16", client=FakeClient(self.rows))
        before = files_snapshot(self.root("AAA"))
        result = replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(files_snapshot(self.root("AAA")), before)

    def test_requested_span_change_rejected_under_publication_lock(self):
        prepared = prepare_refresh(self.store, ["AAA"])[0]
        before = files_snapshot(self.root("AAA"))
        for start, end in (("2026-10-01", "2026-10-16"), ("2016-01-01", "2026-10-15")):
            client = FakeClient(self.rows)
            with self.assertRaisesRegex(DownloadError, "preserve the stored"):
                download_pricing(
                    self.store,
                    "AAA",
                    end,
                    from_date=start,
                    client=client,
                    refresh_all=True,
                    expected_source_sha256=prepared["source_sha256"],
                )
            self.assertEqual(client.calls, [])
        self.assertEqual(files_snapshot(self.root("AAA")), before)

    def test_lineage_failure_is_reported_and_existing_notice_survives(self):
        with patch.object(
            refresh, "run_lineage", return_value={"failed": 1, "results": [{"status": "waiting_for_pricing"}]}
        ) as build:
            result = self.run_refresh()
        self.assertFalse(result["complete"])
        self.assertIs(build.call_args.kwargs["pricing_client"], self.client)
        self.assertEqual(set(build.call_args.args[1]), {"AAA", "BBB"})
        self.assertTrue((self.store / "end_of_day/manifests/AAA/update.json").exists())

    def test_cli_dispatches_explicit_mode_without_saved_handoff(self):
        from cli_support import initialize_command_registration

        initialize_command_registration(self)
        output = io.StringIO()
        with (
            patch.dict("os.environ", {"ALPACA_API_KEY_ID": "test", "ALPACA_API_SECRET_KEY": "test"}),
            patch.object(
                refresh, "refresh_existing", return_value={"failed": 0, "complete": True, "deferred_symbols": []}
            ) as run,
            redirect_stdout(output),
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(main(["pricing", "refresh", "--store", str(self.store)]), 0)
        self.assertEqual(run.call_args.args, (self.store,))
        self.assertEqual(json.loads(output.getvalue()), {"failed": 0, "complete": True, "deferred_symbols": []})


if __name__ == "__main__":
    unittest.main()
