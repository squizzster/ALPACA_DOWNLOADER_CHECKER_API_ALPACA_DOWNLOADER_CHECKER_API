"""Completion scope survives independent requests, workflow ingress and repair."""

import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import check_summary, storage, universe_state
from libs.alpaca_lineage_build import refresh as lineage_refresh
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import correction, refresh
from libs.alpaca_pricing_download.context import accept_context
from libs.alpaca_pricing_download.contract import DownloadError, timestamp, transform
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.alpaca_serving_catalogue import producer
from libs.alpaca_serving_catalogue import storage as serving_storage


class PhysicalCompletionTests(unittest.TestCase):
    def test_metadata_split_preserves_completed_positive_and_negative_subsets(self):
        for positive in (True, False):
            with self.subTest(positive=positive):
                client = AlpacaIdentityClient("test", "test")
                evidence = EvidenceClient(client)
                calls = []

                def page(url):
                    params = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
                    symbols = params["symbols"]
                    calls.append(symbols)
                    if "," in symbols:
                        raise IdentityHttpError(414, "too large")
                    if symbols == "AAA":
                        return {
                            "corporate_actions": {
                                "cash_dividends": [{"id": "dividend", "symbol": "AAA", "ex_date": "2026-01-06"}]
                                if positive
                                else []
                            }
                        }
                    self.assertIn(("AAA", "2026-01-01", "2026-01-13"), evidence.actions)
                    raise RuntimeError("independent metadata failure")

                with patch.object(client, "request_json", side_effect=page):
                    evidence.prefetch_corporate_actions(
                        [(s, "2026-01-01", "2026-01-13") for s in ("AAA", "BBB")]
                    )
                    self.assertEqual(calls, ["AAA,BBB", "AAA", "BBB"])
                    self.assertEqual(
                        len(evidence.get_corporate_actions("AAA", "2026-01-01", "2026-01-13")), int(positive)
                    )
                    with self.assertRaisesRegex(RuntimeError, "independent"):
                        evidence.get_corporate_actions("BBB", "2026-01-01", "2026-01-13")
                    evidence.prefetch_corporate_actions(
                        [(s, "2026-01-01", "2026-01-13") for s in ("AAA", "BBB")]
                    )
                    self.assertEqual(calls, ["AAA,BBB", "AAA", "BBB"])

    def test_completed_split_subset_is_checkpointed_before_peer_failure_and_reused(self):
        for positive in (True, False):
            with self.subTest(positive=positive), tempfile.TemporaryDirectory() as temporary:
                store = Path(temporary) / "store"
                old = [bar("2026-01-05"), bar("2026-01-12")]
                for symbol in ("AAA", "BBB"):
                    download_pricing(store, symbol, "2026-01-13", client=FakeClient(old))
                requests = refresh.prepare_refresh(store, ["AAA", "BBB"])
                selection = {
                    "catalogue_generation": "catalogue",
                    "inspected_groups": 2,
                    "selected_groups": 2,
                    "selected_identities": 2,
                    "sources": {
                        r["symbol"]: {"source_sha256": r["source_sha256"], "sessions": ["2026-01-06"]}
                        for r in requests
                    },
                }
                context = accept_context(store)
                calls, checkpoints = [], []

                def page(params):
                    symbol = params["symbols"]
                    calls.append(symbol)
                    if "," in symbol:
                        raise HTTPError("fixture", 414, "long", {}, None)
                    if symbol == "AAA":
                        return {"bars": {"AAA": [bar("2026-01-06")] if positive else []}}
                    self.assertEqual(len(context.late_bar_observations), 1)
                    self.assertEqual(len(checkpoints), int(positive))
                    raise DownloadError("BBB failed independently")

                def checkpoint(items):
                    checkpoints.extend(copy.deepcopy(items))
                    return items

                client = BarsClient("test", "test")
                with patch.object(client, "_get", side_effect=page):
                    result, report = refresh.probe_late_bar_repairs(
                        store, selection, client=client, context=context, on_positive=checkpoint
                    )
                    self.assertEqual(calls, ["AAA,BBB", "AAA", "BBB"])
                    self.assertEqual([r["symbol"] for r in result], ["AAA"] if positive else [])
                    self.assertEqual([r["symbols"] for r in report["failures"]], [["BBB"]])
                    calls.clear()
                    refresh.probe_late_bar_repairs(
                        store, selection, client=client, context=context, on_positive=checkpoint
                    )
                self.assertEqual(calls, ["BBB"])
                self.assertEqual(len(checkpoints), int(positive))

    def test_partial_subset_pagination_has_no_checkpoint(self):
        from libs.alpaca_data_api_solver import build_plan

        need = {
            "kind": "bars",
            "why": "provider_late_bar_check",
            "symbols": ["AAA"],
            "adjustments": ["raw"],
            "asof": "2026-01-13",
            "ranges": [{"start": "2026-01-06T05:00:00Z", "end_exclusive": "2026-01-07T05:00:00Z"}],
        }
        client = BarsClient("test", "test")
        checkpoints, failures = [], []

        def page(params):
            if "page_token" in params:
                raise DownloadError("incomplete second page")
            return {"bars": {"AAA": [bar("2026-01-06")]}, "next_page_token": "next"}

        with patch.object(client, "_get", side_effect=page):
            result = client.execute_plan(
                build_plan({"steps": [{"needs": [need]}]}),
                on_completed=lambda *args: checkpoints.append(args),
                on_failure=lambda *args: failures.append(args),
            )
        self.assertEqual(result, {})
        self.assertEqual(checkpoints, [])
        self.assertEqual(len(failures), 1)


class WorkflowIngressTests(unittest.TestCase):
    def test_no_work_update_accepts_unselected_completed_evidence_once(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            row = ["2016-01-04T05:00:00Z", "1", "2", "1", "2", "10", "2", "1"]
            universe_state.put_work(
                case.store,
                "DONE",
                {
                    "status": "complete",
                    "obligation_id": "done",
                    "pricing_correction": {"schema_version": 3, "observations": {"all": [row]}},
                },
            )
            accepted = []
            original = correction.validate_durable_observations

            def validate(contract):
                if contract is not None:
                    accepted.append(contract)
                return original(contract)

            with patch.object(correction, "validate_durable_observations", side_effect=validate):
                result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["plan"]["work"], {})
            self.assertEqual(len(accepted), 1)

    def test_status_count_consumes_no_observation_body_and_keeps_pending(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            universe_state.initialize(store)
            universe_state.put_work_group(
                store,
                {
                    "AAA": {"status": "complete"},
                    "BBB": {"status": "retry"},
                    "CCC": {"status": "blocked"},
                },
            )
            with patch.object(
                correction, "validate_durable_observations", side_effect=AssertionError("revalidation")
            ):
                universe_state.require_initialized(store)
                self.assertEqual(universe_state.count_pending(store), 2)


class LateRepairApprovalTests(unittest.TestCase):
    def test_peer_probe_failure_cannot_promote_an_unchecked_session_insertion(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary) / "store"
            old = [bar("2026-01-05"), bar("2026-01-07")]
            end = "2026-01-08"
            assets = {symbol: asset(symbol) for symbol in ("AAA", "BBB")}
            for symbol in assets:
                download_pricing(store, symbol, end, client=FakeClient(old))
            self.assertEqual(run_lineage(store, identity_client=IdentityClient(assets))["failed"], 0)
            check_summary.rebuild(store)
            self.assertTrue(producer.maintain(store, bootstrap=True)["complete"])
            selection = serving_storage.select_late_bar_repairs(store, serving_storage.controls(store))
            universe_state.initialize(store)
            policy = WorkPolicy(store, storage.read_index(store / "end_of_day/lineage"), {}, [], end)
            client = BarsClient("test", "test")

            def page(params):
                if "," in params["symbols"]:
                    raise HTTPError("fixture", 414, "split", {}, None)
                if params["symbols"] == "BBB":
                    raise DownloadError("BBB peer failed")
                return {"bars": {"AAA": [bar("2026-01-06")]}}

            with patch.object(client, "_get", side_effect=page):
                result = lineage_refresh.refresh_existing(
                    store,
                    pricing_client=client,
                    identity_client=IdentityClient(assets),
                    end_date=end,
                    late_bar_repairs=selection,
                    work_policy=policy,
                )
            self.assertFalse(result["complete"], result)
            self.assertNotIn("AAA", policy.history_approved)
            self.assertEqual(
                [r[0][:10] for r in read_pricing_views(store, "AAA")["raw"]],
                ["2026-01-05", "2026-01-06", "2026-01-07"],
            )

    def test_completed_full_late_correction_keeps_its_actual_producer_approval(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            current = [bar("2016-01-04", "500"), case.old[-1]]
            policy = WorkPolicy(
                case.store, storage.read_index(case.store / "end_of_day/lineage"), {}, [], "2026-10-17"
            )
            prepared = refresh.prepare_refresh(case.store, ["AAA"])
            prepared[0]["correction_cause"] = "provider_late_observations"
            prepared[0]["correction_observations"] = {
                "raw": [
                    list(r)
                    for r in transform(
                        current[:1], timestamp(prepared[0]["start"]), timestamp(prepared[0]["end_exclusive"])
                    )
                ]
            }
            policy.require_correction(prepared)
            # This models the supported older full-span late-bar producer;
            # the descriptive cause does not narrow its completed authority.
            download_pricing(
                case.store,
                "AAA",
                "2026-10-17",
                client=FakeClient(current),
                refresh_all=True,
                history_correction_id=prepared[0]["required_correction_id"],
                correction_cause="provider_late_observations",
                correction_observations=prepared[0]["correction_observations"],
            )
            value = run_lineage(case.store, ["AAA"], identity_client=IdentityClient({"AAA": asset("AAA")}))
            self.assertEqual(value["failed"], 0, value)
            with (
                patch.object(case.pricing, "_get", side_effect=AssertionError("completed history rechecked")),
                patch.object(
                    lineage_refresh, "run_lineage", side_effect=AssertionError("completed lineage reopened")
                ),
            ):
                result = lineage_refresh.refresh_existing(
                    case.store,
                    pricing_client=case.pricing,
                    identity_client=case.identity,
                    end_date="2026-10-17",
                    work_policy=policy,
                )
            self.assertTrue(result["complete"], result)

    def test_session_insertion_carries_an_applicable_completed_comparison(self):
        from libs.alpaca_lineage_build.context import OperationContext

        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary) / "store"
            old = [bar("2026-01-05"), bar("2026-01-07")]
            end = "2026-01-08"
            download_pricing(store, "AAA", end, client=FakeClient(old))
            assets = {"AAA": asset("AAA")}
            self.assertEqual(run_lineage(store, identity_client=IdentityClient(assets))["failed"], 0)
            check_summary.rebuild(store)
            self.assertTrue(producer.maintain(store, bootstrap=True)["complete"])
            selection = serving_storage.select_late_bar_repairs(store, serving_storage.controls(store))
            client, context, calls = BarsClient("test", "test"), OperationContext(store), []
            live = list(old)

            def page(params):
                calls.append(dict(params))
                lower, upper = timestamp(params["start"]), timestamp(params["end"])
                return {"bars": {"AAA": [r for r in live if lower <= timestamp(r["t"]) <= upper]}}

            with patch.object(client, "_get", side_effect=page):
                first = lineage_refresh.refresh_existing(
                    store,
                    pricing_client=client,
                    identity_client=IdentityClient(assets),
                    end_date=end,
                    context=context,
                )
                self.assertTrue(first["complete"], first)
                self.assertEqual(first["check"]["stats"]["matched"], 1)
                calls.clear()
                live[:] = [old[0], bar("2026-01-06"), old[-1]]
                second = lineage_refresh.refresh_existing(
                    store,
                    pricing_client=client,
                    identity_client=IdentityClient(assets),
                    end_date=end,
                    late_bar_repairs=selection,
                    context=context,
                )
            self.assertTrue(second["complete"], second)
            self.assertEqual(second["check"]["stats"].get("tickers", 0), 0)
            self.assertEqual(len(calls), 2)
            self.assertEqual({p["adjustment"] for p in calls}, {"raw", "all"})
            self.assertTrue(all(timestamp(p["start"]).date().isoformat() == "2026-01-06" for p in calls))

    def check_repair(self, *, pending_full):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary) / "store"
            old = [bar("2026-01-05"), bar("2026-01-07")]
            end = "2026-01-08"
            download_pricing(store, "AAA", end, client=FakeClient(old))
            assets = {"AAA": asset("AAA")}
            self.assertEqual(run_lineage(store, identity_client=IdentityClient(assets))["failed"], 0)
            check_summary.rebuild(store)
            self.assertTrue(producer.maintain(store, bootstrap=True)["complete"])
            selection = serving_storage.select_late_bar_repairs(store, serving_storage.controls(store))
            live = [bar("2026-01-05", "500"), bar("2026-01-06"), old[-1]]
            universe_state.initialize(store)
            policy = WorkPolicy(store, storage.read_index(store / "end_of_day/lineage"), {}, [], end)
            if pending_full:
                requests = refresh.prepare_refresh(store, ["AAA"])
                requests[0]["correction_cause"] = "selected_all_rows_changed"
                requests[0]["correction_observations"] = {
                    "all": [
                        list(r)
                        for r in transform(
                            [live[0], live[-1]], timestamp("2016-01-01T00:00:00Z"), timestamp(end + "T00:00:00Z")
                        )
                    ]
                }
                policy.require_correction(requests)
            client, calls = BarsClient("test", "test"), []

            def page(params):
                calls.append(dict(params))
                start, stop = timestamp(params["start"]), timestamp(params["end"])
                return {"bars": {"AAA": [r for r in live if start <= timestamp(r["t"]) <= stop]}}

            with patch.object(client, "_get", side_effect=page):
                result = lineage_refresh.refresh_existing(
                    store,
                    pricing_client=client,
                    identity_client=IdentityClient(assets),
                    end_date=end,
                    late_bar_repairs=selection,
                    work_policy=policy,
                )
            self.assertTrue(result["complete"], result)
            self.assertEqual(read_pricing_views(store, "AAA")["all"][0][1], "500")
            self.assertTrue(all(p["asof"] == end for p in calls))
            if pending_full:
                self.assertNotEqual(
                    policy.work["AAA"]["pricing_correction"]["cause"], "provider_late_observations"
                )
            else:
                self.assertGreaterEqual(result["check"]["stats"]["tickers"], 1)

    def test_session_insertion_cannot_approve_unchecked_history(self):
        self.check_repair(pending_full=False)

    def test_pending_full_correction_survives_new_missing_session_witness(self):
        self.check_repair(pending_full=True)
