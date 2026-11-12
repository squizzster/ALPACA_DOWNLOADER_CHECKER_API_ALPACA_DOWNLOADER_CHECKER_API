import copy
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlite_crash_fixture import leave_hot_journal
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, single_session_request
from test_alpaca_pricing_download import snapshot as files_snapshot

from libs.alpaca_lineage_build import check_summary as projection
from libs.alpaca_lineage_build import contract as lineage_contract
from libs.alpaca_lineage_build import pipeline, refresh, update
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.reader import read_lineage
from libs.alpaca_lineage_build.transform import ASSIGNMENT_FIELDS
from libs.alpaca_pricing_download import check_summary, storage
from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.update_checks import CHECK_FILE, queue_check, read_checks


class UpdateEverythingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        clock = patch(
            "libs.alpaca_pricing_download.download.utc_now",
            return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
        )
        clock.start()
        cls.addClassCleanup(clock.stop)
        temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(temporary.cleanup)
        cls.base = Path(temporary.name) / "base"
        cls.assets = {symbol: asset(symbol) for symbol in ("AAA", "BBB", "ZZZ")}
        cls.assets["ZZZ"]["status"] = "inactive"
        cls.old = [bar("2016-01-04"), bar("2026-10-14")]
        for symbol in cls.assets:
            download_pricing(cls.base, symbol, "2026-10-15", client=FakeClient(cls.old))
        result = run_lineage(cls.base, identity_client=IdentityClient(cls.assets))
        if result["failed"]:
            raise AssertionError(result)
        projection.rebuild(cls.base)
        if len(check_summary.read_snapshot(cls.base)["entries"]) != 2:
            raise AssertionError("fixture must exclude the inactive source")

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        shutil.copytree(self.base, self.store)
        self.live = {
            symbol: copy.deepcopy(self.old) + [bar("2026-10-15"), bar("2026-10-16")] for symbol in self.assets
        }
        self.calls = []
        self.client = BarsClient("test", "test")
        self.identity = IdentityClient(self.assets)
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

    def run_update(self, end="2026-10-17", **kwargs):
        return update.update_everything(
            self.store, end, pricing_client=self.client, identity_client=self.identity, **kwargs
        )

    def assert_finished(self):
        ready = check_summary.read_snapshot(self.store)
        self.assertEqual({entry["symbol"] for entry in ready["entries"]}, {"AAA", "BBB"})
        for entry in ready["entries"]:
            self.assertEqual(entry["asof"], "2026-10-17")
            self.assertEqual(entry["last_all"][0][:10], "2026-10-16")
            self.assertFalse((self.store / f"end_of_day/manifests/{entry['symbol']}/update.json").exists())

    def test_complete_update_and_repeat_trust_boundaries_and_skip_inactive_sources(self):
        inactive = files_snapshot(self.store / "end_of_day/pricing/ZZZ")
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["stages"]["refresh"]["check"]["stats"]["matched"], 2)
        self.assertEqual(result["stages"]["append"]["requests"]["page_requests"], 2)
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(all(call["symbols"] == "AAA,BBB" for call in self.calls))
        self.assertTrue(all(call["start"][:10] == "2026-10-15" for call in self.calls[2:]))
        self.assertEqual(files_snapshot(self.store / "end_of_day/pricing/ZZZ"), inactive)
        self.assert_finished()
        self.calls.clear()
        before = files_snapshot(self.store)
        identity_calls = self.identity.request_count
        with (
            patch.object(Path, "iterdir", side_effect=AssertionError("directory scan")),
            patch.object(storage, "load_source", side_effect=AssertionError("unnecessary source read")),
        ):
            repeated = self.run_update()
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(len(self.calls), 2, "integrated update must always recheck live")
        self.assertEqual({p["start"][:10] for p in self.calls}, {"2016-01-04", "2026-10-14"})
        self.assertEqual(self.identity.request_count, identity_calls)
        self.assertEqual(files_snapshot(self.store), before)

    def test_recovery_progress_needs_no_lineage_boundaries_for_unchanged_sources(self):
        for symbol in self.assets:
            queue_check(self.store, symbol)
        events = []
        with patch.object(lineage_storage, "read_boundaries", side_effect=AssertionError("progress read")):
            result = update.finish_pending(
                self.store,
                pricing_client=self.client,
                identity_client=self.identity,
                progress=lambda stage, counts: events.append((stage, counts)),
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertIn(("recovery", {"processed": len(self.assets)}), events)
        self.assertTrue(
            any(stage == "lineage_group" and values.get("reused_decision") for stage, values in events)
        )
        self.assertEqual({row["status"] for row in result["lineage"]["results"]}, {"unchanged"})

    def test_changed_history_is_replaced_in_its_old_span_before_append(self):
        self.live["AAA"][0] = bar("2016-01-04", "456")
        self.live["AAA"][1]["v"] = 42
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        refresh = result["stages"]["refresh"]
        self.assertEqual(refresh["check"]["stats"]["differed"], 1)
        replaced = refresh["pricing"]["results"]
        self.assertEqual([row["symbol"] for row in replaced], ["AAA"])
        self.assertEqual(replaced[0]["request"]["end_exclusive"], "2026-10-15T00:00:00Z")
        self.assertEqual(len(self.calls), 7)  # Accepted all observation replaces its first confirmation.
        rows = read_pricing_views(self.store, "AAA")["all"]
        self.assertEqual(rows[0][1], "456")
        self.assertEqual(rows[1][5], "42")
        self.assertEqual([row[0][:10] for row in rows], ["2016-01-04", "2026-10-14", "2026-10-15", "2026-10-16"])
        self.assert_finished()

    def test_provider_late_bar_uses_exact_serving_handoff_and_normal_replacement(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = Path(temporary.name) / "late-bar"
        initial = {
            "AAA": [bar("2026-01-05"), bar("2026-01-07")],
            "BBB": [bar("2026-01-05"), bar("2026-01-06"), bar("2026-01-07")],
        }
        assets = {symbol: asset(symbol) for symbol in initial}
        for symbol, rows in initial.items():
            download_pricing(store, symbol, "2026-01-08", client=FakeClient(rows))
        lineage = run_lineage(store, identity_client=IdentityClient(assets))
        self.assertEqual(lineage["failed"], 0, lineage)
        projection.rebuild(store)

        from libs.alpaca_serving_catalogue import storage as serving_storage
        from libs.alpaca_serving_catalogue.producer import maintain

        serving = maintain(store, bootstrap=True)
        self.assertTrue(serving["complete"], serving)
        controls = serving_storage.controls(store)
        with patch.object(
            serving_storage, "read_groups", side_effect=AssertionError("selector expanded into group bodies")
        ):
            selected = serving_storage.select_late_bar_repairs(store, controls)
        self.assertEqual(set(selected["sources"]), {"AAA"})
        self.assertEqual(selected["sources"]["AAA"]["sessions"], ["2026-01-06"])
        key = controls["members"]["AAA"]
        legacy = {
            **controls,
            "groups": {
                **controls["groups"],
                key: {k: v for k, v in controls["groups"][key].items() if k != "late_bar_repairs"},
            },
        }
        with (
            patch.object(serving_storage, "read_groups", side_effect=AssertionError("legacy selector crawled")),
            self.assertRaisesRegex(serving_storage.CatalogueError, "catalogue rebuild"),
        ):
            serving_storage.select_late_bar_repairs(store, legacy)
        before_group = serving_storage.read_groups(store, [key], generation=controls["state"]["generation"])[key]
        public_id = before_group["decisions"][0]["public_id"]
        self.assertFalse(before_group["decisions"][0]["eligible"])

        from libs.alpaca_pricing_download.refresh import probe_late_bar_repairs

        unchanged = files_snapshot(store / "end_of_day/pricing/AAA")
        absent_calls = []
        absent = BarsClient("test", "test")

        def absent_page(params):
            absent_calls.append(dict(params))
            start, end = timestamp(params["start"]), timestamp(params["end"])
            return {
                "bars": {
                    symbol: [row for row in initial[symbol] if start <= timestamp(row["t"]) <= end]
                    for symbol in params["symbols"].split(",")
                }
            }

        with patch.object(absent, "_get", side_effect=absent_page):
            prepared, negative = probe_late_bar_repairs(store, selected, client=absent)
        self.assertEqual(prepared, [])
        self.assertEqual(negative["returned_sessions"], 0)
        self.assertEqual(len(absent_calls), 1)
        self.assertEqual(files_snapshot(store / "end_of_day/pricing/AAA"), unchanged)
        stale = copy.deepcopy(selected)
        stale["sources"]["AAA"]["source_sha256"] = "0" * 64
        with patch.object(absent, "execute_plan", side_effect=AssertionError("stale probe executed")):
            prepared, rejected = probe_late_bar_repairs(store, stale, client=absent)
        self.assertEqual(prepared, [])
        self.assertEqual(rejected["stale_sources"], ["AAA"])

        healthy = files_snapshot(store / "end_of_day/pricing/BBB")
        live = {**initial, "AAA": [bar("2026-01-05"), bar("2026-01-06"), bar("2026-01-07")]}
        calls = []
        client = BarsClient("test", "test")

        def page(params):
            calls.append(dict(params))
            start, end = timestamp(params["start"]), timestamp(params["end"])
            return {
                "bars": {
                    symbol: [row for row in live[symbol] if start <= timestamp(row["t"]) <= end]
                    for symbol in params["symbols"].split(",")
                }
            }

        with patch.object(client, "_get", side_effect=page):
            repaired = refresh.refresh_existing(
                store,
                pricing_client=client,
                identity_client=IdentityClient(assets),
                end_date="2026-01-08",
                late_bar_repairs=selected,
            )
        self.assertTrue(repaired["complete"], repaired)
        self.assertEqual(repaired["late_bar_check"]["requested_sessions"], 1)
        self.assertEqual(repaired["late_bar_check"]["returned_sessions"], 1)
        self.assertEqual([item["symbol"] for item in repaired["pricing"]["results"]], ["AAA"])
        self.assertEqual(files_snapshot(store / "end_of_day/pricing/BBB"), healthy)
        exact = [
            call
            for call in calls
            if call["adjustment"] == "raw"
            and timestamp(call["start"]).astimezone(NEW_YORK).date().isoformat() == "2026-01-06"
        ]
        self.assertEqual(len(exact), 1)
        self.assertEqual(exact[0]["symbols"], "AAA")
        # Session insertion approved only the selected missing session. Both
        # retained histories still need their daily first/tail comparison.
        self.assertEqual(repaired["check"]["stats"]["tickers"], 2)
        self.assertEqual(repaired["check"]["stats"]["matched"], 2)

        serving = maintain(store)
        self.assertTrue(serving["complete"], serving)
        controls = serving_storage.controls(store)
        group = serving_storage.read_groups(
            store,
            [controls["members"]["AAA"]],
            generation=controls["state"]["generation"],
        )[controls["members"]["AAA"]]
        decision = next(item for item in group["decisions"] if item["public_id"] == public_id)
        self.assertTrue(decision["eligible"], decision)
        self.assertEqual(
            [row[0][:10] for row in read_pricing_views(store, "AAA")["raw"]],
            ["2026-01-05", "2026-01-06", "2026-01-07"],
        )

    def test_new_action_completes_lineage_and_pricing_keeps_advancing(self):
        action = {
            "action_type": "cash_dividend",
            "id": "new-dividend",
            "symbol": "AAA",
            "cusip": "111111111",
            "process_date": "2026-10-16",
            "ex_date": "2026-10-16",
        }
        self.identity = IdentityClient({**self.assets, "111111111": self.assets["AAA"]}, [action])
        retained = read_pricing_views(self.store, "AAA")
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as evaluate:
            result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["failed"], 0)
        evaluated = set().union(*(call.args[1].keys() for call in evaluate.call_args_list))
        self.assertIn("AAA", evaluated, "new facts must reach the affected group's evaluator")
        self.assertLessEqual(evaluated, {"AAA", "BBB"})
        self.assert_finished()
        for mode, rows in retained.items():
            self.assertEqual(read_pricing_views(self.store, "AAA")[mode][: len(rows)], rows)
        self.assertEqual(
            [a["id"] for a in read_lineage(self.store, "AAA")["receipt"]["evidence"]["corporate_actions"]],
            ["new-dividend"],
        )
        for symbol in self.live:
            self.live[symbol].append(bar("2026-10-19"))
        result = self.run_update("2026-10-20")
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["failed"], 0)
        for symbol in ("AAA", "BBB"):
            self.assertEqual(read_pricing_views(self.store, symbol)["all"][-1][0][:10], "2026-10-19")
            self.assertEqual(read_lineage(self.store, symbol)["receipt"]["processed_through"][:10], "2026-10-20")
            self.assertFalse((self.store / f"end_of_day/manifests/{symbol}/update.json").exists())

        def decisions():
            return {
                symbol: {
                    mode: [
                        {key: value for key, value in segment.items() if key in ASSIGNMENT_FIELDS}
                        for segment in segments
                        for _ in range(segment["row_count"])
                    ]
                    for mode, segments in read_lineage(self.store, symbol)["adjustments"].items()
                }
                for symbol in ("AAA", "BBB")
            }

        accepted = decisions()
        reference = run_lineage(self.store, ["AAA", "BBB"], identity_client=self.identity, refresh=True)
        self.assertEqual(reference["failed"], 0, reference)
        # A full refresh may use later source coverage to select a more
        # descriptive rule for the same owner. An ordinary append must keep
        # the already accepted provenance rather than replaying those rows.
        provenance = {"correlation_rule", "correlation_method", "identity_confidence", "warnings"}

        def ownership(value):
            return {
                symbol: {
                    mode: [{key: item for key, item in row.items() if key not in provenance} for row in rows]
                    for mode, rows in modes.items()
                }
                for symbol, modes in value.items()
            }

        self.assertEqual(ownership(decisions()), ownership(accepted))

    def test_complete_empty_hints_still_catch_up_changed_rules_without_metadata_requests(self):
        checks = read_checks(self.store)
        self.assertTrue(checks["complete"])
        self.assertEqual(checks["checks"], {})
        root = self.store / "end_of_day/lineage"
        index = lineage_storage.read_index(root)
        legacy_versions = {**lineage_contract.versions(), "lineage": 33, "correlation": 59}
        legacy_versions.pop("correlation_policy_sha256")
        receipts, entries, documents, sources = {}, {}, {}, {}
        for symbol in sorted(self.assets):
            receipt = lineage_storage.read_receipt(root, symbol, index)
            receipt.update(schema_version=33, versions=legacy_versions)
            receipt.pop("correlation_policy")
            receipts[symbol] = receipt
            documents[f"{symbol}/lineage.manifest.json"] = receipt
            entries[symbol] = {
                **index["symbols"][symbol],
                "sha256": lineage_storage.document_digest(receipt),
                "versions": legacy_versions,
            }
            entries[symbol].pop("correlation_policy")
            sources[symbol] = receipt["source_sha256"]
        journal = {
            "schema_version": 33,
            "boundary": "alpaca_lineage_transaction",
            "documents": documents,
            "retire": [],
            "index_updates": entries,
            "sources": sources,
            "unavailable_pricing": [],
            "notices": {},
        }
        with (
            lineage_storage.pricing_locks(self.store, self.assets),
            lineage_storage.publication_lock(root, exclusive=True),
        ):
            lineage_storage._install(root, journal, compare=False)
        pricing = files_snapshot(self.store / "end_of_day/pricing")
        with (
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata reacquisition")),
            patch.object(pipeline, "local_symbols", side_effect=AssertionError("receipt discovery")),
        ):
            result = self.run_update("2026-10-15")
            self.assertTrue(result["complete"], result)
            processed = {
                symbol
                for row in result["stages"]["recovery"]["lineage"]["results"]
                for symbol in row["processed"]
            }
            self.assertEqual(processed, set(self.assets))
            index = lineage_storage.read_index(self.store / "end_of_day/lineage")
            self.assertTrue(
                all(lineage_contract.reusable_versions(entry["versions"]) for entry in index["symbols"].values())
            )
            repeated = self.run_update("2026-10-15")
            self.assertTrue(repeated["complete"], repeated)
            self.assertEqual(repeated["stages"]["recovery"]["lineage"]["results"], [])
        self.assertEqual(len(self.calls), 4, "each invocation needs its two fresh endpoint comparisons")
        self.assertEqual(self.identity.request_count, 0)
        self.assertEqual(files_snapshot(self.store / "end_of_day/pricing"), pricing)

    def test_missing_live_boundary_stops_before_any_publication_or_append(self):
        self.live["AAA"] = []
        before = files_snapshot(self.store)
        result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "refresh")
        self.assertIn("live check incomplete", result["error"])
        self.assertEqual(files_snapshot(self.store), before)

    def test_malformed_append_page_preserves_all_publications(self):
        before = files_snapshot(self.store)

        def malformed(params):
            response = self.page(params)
            if params["start"][:10] == "2026-10-15" and params["adjustment"] == "all":
                response["bars"]["AAA"].insert(0, bar("2026-10-14"))
            return response

        with patch.object(self.client, "_get", side_effect=malformed):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["stages"]["append"]["failed"], 2)
        self.assertEqual(files_snapshot(self.store), before)

    def test_changed_summary_during_append_prevents_stale_publication(self):
        before = files_snapshot(self.store / "end_of_day")
        original = check_summary.read_snapshot(self.store)["entries"][0]
        changed = False

        def concurrent(params):
            nonlocal changed
            if params["start"][:10] == "2026-10-15" and not changed:
                changed = True
                check_summary.invalidate(self.store, ["AAA"])
                check_summary.replace_entries(self.store, {"AAA": original})
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=concurrent):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "append")
        self.assertIn("baseline changed", result["error"])
        self.assertEqual(files_snapshot(self.store / "end_of_day"), before)

    def test_handoff_changed_between_stages_is_rejected_before_append_requests(self):
        refresh = update.refresh_existing
        entry = check_summary.read_snapshot(self.store)["entries"][0]
        before = files_snapshot(self.store / "end_of_day")

        def concurrent(*args, **kwargs):
            result = refresh(*args, **kwargs)
            check_summary.invalidate(self.store, ["AAA"])
            check_summary.replace_entries(self.store, {"AAA": entry})
            return result

        with patch.object(update, "refresh_existing", side_effect=concurrent):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "append")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(files_snapshot(self.store / "end_of_day"), before)

    def test_failed_history_replacement_preserves_its_source_and_allows_approved_peer_append(self):
        self.live["AAA"][0]["v"] = 42
        before = files_snapshot(self.store / "end_of_day/pricing/AAA")

        def fail_full_history(params):
            if params["start"][:4] == "2016" and not single_session_request(params):
                raise OSError("full history request failed")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=fail_full_history):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["stages"]["refresh"]["pricing"]["failed"], 1)
        self.assertEqual([r["symbol"] for r in result["stages"]["append"]["results"]], ["BBB"])
        self.assertEqual(result["stages"]["lineage"]["failed"], 0)
        self.assertEqual(files_snapshot(self.store / "end_of_day/pricing/AAA"), before)
        self.assertEqual(read_pricing_views(self.store, "BBB")["all"][-1][0][:10], "2026-10-16")
        suffix = [p for p in self.calls if p["start"][:10] == "2026-10-15"]
        self.assertEqual(len(suffix), 2)
        self.assertTrue(all(p["symbols"] == "BBB" for p in suffix))

    def test_same_command_recovers_interrupted_pricing_then_rechecks_live(self):
        original = storage.os.replace

        def interrupt(source, target):
            path = Path(target)
            if "AAA" in path.parts and path.suffix == ".gz":
                raise OSError("interrupted pricing install")
            return original(source, target)

        with patch.object(storage.os, "replace", side_effect=interrupt):
            failed = self.run_update()
        self.assertFalse(failed["complete"])
        self.assertTrue((self.store / "end_of_day/pricing/AAA/.pending.json").exists())
        self.calls.clear()
        recovered = self.run_update()
        self.assertTrue(recovered["complete"], recovered)
        self.assertEqual(recovered["stages"]["recovery"]["pricing_recovered"], ["AAA"])
        self.assertEqual(len(self.calls), 2, "recovery must not reacquire completed new sessions")
        self.assert_finished()

    def test_same_command_recovers_interrupted_databases_before_work_selection(self):
        leave_hot_journal(check_summary.summary_path(self.store), "UPDATE state SET complete=0")
        leave_hot_journal(self.store / CHECK_FILE, "DELETE FROM checks")
        leave_hot_journal(self.store / "end_of_day/lineage/index.sqlite3", "PRAGMA user_version=0")
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(len(self.calls), 4)
        self.assert_finished()

    def test_deleted_work_list_restores_pending_lineage_before_the_live_check(self):
        download_pricing(self.store, "AAA", "2026-10-17", client=FakeClient(self.live["AAA"]))
        notice = self.store / "end_of_day/manifests/AAA/update.json"
        self.assertTrue(notice.exists())
        (self.store / CHECK_FILE).unlink()

        def after_recovery(params):
            self.assertFalse(notice.exists(), "known coverage must be restored before history checking")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=after_recovery):
            result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertTrue(read_checks(self.store)["complete"])
        recovery = result["stages"]["recovery"]["lineage"]["results"]
        self.assertTrue(any(row["symbol"] == "AAA" and row["status"] == "processed" for row in recovery))
        self.assert_finished()

    def test_same_command_recovers_interrupted_lineage_before_using_summary(self):
        with patch.object(lineage_storage, "_install", side_effect=OSError("interrupted lineage install")):
            failed = self.run_update()
        self.assertFalse(failed["complete"])
        self.assertTrue((self.store / "end_of_day/lineage/.pending.json").exists())
        self.calls.clear()
        recovered = self.run_update()
        self.assertTrue(recovered["complete"], recovered)
        self.assertTrue(recovered["stages"]["recovery"]["lineage"]["recovered"])
        self.assertEqual(len(self.calls), 2)
        self.assert_finished()

    def test_midnight_during_live_check_does_not_extend_the_frozen_target(self):
        now = datetime(2026, 10, 17, 3, 59, tzinfo=timezone.utc)

        def crossing(params):
            nonlocal now
            now = datetime(2026, 10, 17, 4, 1, tzinfo=timezone.utc)
            return self.page(params)

        with (
            patch("libs.alpaca_pricing_download.download.utc_now", side_effect=lambda: now),
            patch.object(self.client, "_get", side_effect=crossing),
        ):
            result = self.run_update(None)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["end_exclusive"], "2026-10-16")
        self.assertTrue(all(timestamp(call["end"]) < timestamp("2026-10-16T00:00:00Z") for call in self.calls))
        self.assertTrue(
            all(row["asof"] == "2026-10-16" for row in check_summary.read_snapshot(self.store)["entries"])
        )

    def test_missing_summary_is_not_silently_rebuilt_or_scanned(self):
        check_summary.summary_path(self.store).unlink()
        before = files_snapshot(self.store)
        with patch.object(Path, "iterdir", side_effect=AssertionError("scan")):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertIn("summary is missing", result["error"])
        self.assertEqual(self.calls, [])
        self.assertEqual(files_snapshot(self.store), before)

    def test_canonical_update_dispatches_frozen_target_to_universe_owner(self):
        from libs.alpaca_cli import main

        with (
            patch("libs.alpaca_cli.commit_registry"),
            patch("libs.alpaca_cli.clients", return_value=(self.client, self.identity)),
            patch(
                "libs.alpaca_lineage_build.universe_update.run_update", return_value={"complete": True}
            ) as run,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(main(["update", "--store", str(self.store), "--through", "2026-10-16"]), 0)
        self.assertEqual(run.call_args.args, (self.store, "2026-10-17"))

    def test_conflicting_cli_overrides_fail_before_network_or_publication(self):
        from libs.alpaca_cli import main

        before = files_snapshot(self.store)
        for flags in (
            ["--append-ready"],
            ["--from", "2016-01-01"],
            ["--force-download"],
            ["--all"],
            ["--symbol", "AAA"],
        ):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                main(["update", "--store", str(self.store), *flags])
        self.assertEqual(self.calls, [])
        self.assertEqual(files_snapshot(self.store), before)
