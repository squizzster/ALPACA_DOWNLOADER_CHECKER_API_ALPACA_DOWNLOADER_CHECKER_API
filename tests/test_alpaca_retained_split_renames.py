"""Selected v107 adoption refines old open issue intervals without replaying prices."""

import tempfile
from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import ownership_interval_rules
from libs.alpaca_lineage_build import dated_labels, label_updates, pipeline, retained_split_renames, storage
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient, provider_evidence
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download import storage as prices
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import storage as serving
from libs.alpaca_serving_catalogue.producer import maintain


class RetainedSplitRenameTests(TestCase):
    def setUp(self):
        self.store = Path(self.enterContext(tempfile.TemporaryDirectory())) / "store"
        self.root = self.store / "end_of_day/lineage"
        first = asset("EARLY", "first-symbol-uuid")
        middle = asset("MIDDLE", "middle-uuid")
        last = asset("LAST", "last-uuid")
        for record in (first, middle, last):
            record.update(name="A common security")
        for record in (first, middle):
            record.update(status="inactive", tradable=False)
        a, b = "111111111", "222222222"
        self.client = IdentityClient(
            {"EARLY": first, "MIDDLE": middle, "LAST": last, a: middle, b: last},
            [
                {
                    "id": "entry",
                    "action_type": "name_change",
                    "process_date": "2026-10-06",
                    "old_symbol": "EARLY",
                    "new_symbol": "MIDDLE",
                    "old_cusip": a,
                    "new_cusip": a,
                },
                {
                    "id": "split",
                    "action_type": "reverse_split",
                    "process_date": "2026-10-09",
                    "ex_date": "2026-10-09",
                    "symbol": "MIDDLE",
                    "old_cusip": a,
                    "new_cusip": b,
                    "old_rate": 8,
                    "new_rate": 1,
                },
                {
                    "id": "exit",
                    "action_type": "name_change",
                    "process_date": "2026-10-14",
                    "old_symbol": "MIDDLE",
                    "new_symbol": "LAST",
                    "old_cusip": b,
                    "new_cusip": b,
                },
            ],
        )
        self.evidence = EvidenceClient(self.client)
        for name, start, end in (
            ("EARLY", "2026-10-01", "2026-10-06"),
            ("MIDDLE", "2026-10-01", "2026-10-20"),
            ("LAST", "2026-10-14", "2026-10-20"),
        ):
            pin = {"mapping_asof": "2026-10-13", "refresh_all": True} if name == "MIDDLE" else {}
            download_pricing(
                self.store, name, "2026-10-20", client=FakeClient(informative_bars(start, end)), **pin
            )
        # Install a completed, still-readable v1 dated-label result with the two
        # open issue claims produced prior to the new split/rename refinement.
        with patch.object(ownership_interval_rules, "_bound_split_rename", return_value=None):
            self.run_group()
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        before = self.receipts()
        self.pid = before["MIDDLE"]["identity_memberships"][0]["security_id"]
        self.assertTrue(label_updates.applicable(before))
        self.assertTrue(retained_split_renames.possible(before))
        self.assertEqual(
            ["LAST", "MIDDLE"], before["LAST"]["serving_ownership"]["views"]["raw"]["ownership"][-1]["owner"][1]
        )

    def run_group(self, *, context=None):
        result = pipeline.run_lineage(
            self.store,
            ["MIDDLE"],
            identity_client=self.client,
            evidence_client=self.evidence,
            context=context or OperationContext(self.store),
        )
        self.assertEqual(0, result["failed"], result)
        return result

    def receipts(self):
        index = storage.read_index(self.root, "MIDDLE", group=True)
        return {name: storage.read_receipt(self.root, name, index) for name in index["symbols"]}

    def test_normal_selection_carries_dates_and_all_numerical_choices_and_resumes_install(self):
        before = self.receipts()
        reference = serving.price_review_group(self.store, self.pid)[1]
        requests = self.client.request_count

        class Interrupted(BaseException):
            pass

        read = prices.read_artifact

        def selected_metadata(path, **options):
            if path.suffix == ".gz" or options.get("purpose") == "selected_label_dates":
                self.fail(f"retained price payload opened: {path}")
            return read(path, **options)

        context = OperationContext(self.store)
        with (
            operation(self.store),
            patch.object(dated_labels, "plan", side_effect=AssertionError("v1 replay")),
            patch.object(
                ownership_interval_rules,
                "build_ticker_ownership_intervals",
                side_effect=AssertionError("ownership replay"),
            ),
            patch.object(pipeline, "select_observations", side_effect=AssertionError("numerical replay")),
            patch.object(prices, "read_artifact", side_effect=selected_metadata),
            patch.object(serving, "price_review_group", wraps=serving.price_review_group) as checkpoint,
        ):
            with patch.object(storage, "_install", side_effect=Interrupted()):
                with self.assertRaises(Interrupted):
                    self.run_group(context=context)
            self.assertEqual(1, checkpoint.call_count)
            with patch.object(
                retained_split_renames, "plan", side_effect=AssertionError("completed projection replay")
            ):
                result = self.run_group()
                self.assertTrue(result["recovered"])
                with patch.object(
                    serving, "read_groups", side_effect=AssertionError("accepted serving group reread")
                ):
                    installed = maintain(self.store, symbols=["MIDDLE"], context=context)
                self.assertTrue(installed["complete"], installed)
                self.assertEqual((0, 1), (installed["checked_bars"], installed["reused_history_prefixes"]))
                self.assertEqual(1, checkpoint.call_count)
                self.assertEqual("unchanged", self.run_group()["results"][0]["status"])
        self.assertEqual(requests, self.client.request_count)
        after = self.receipts()
        revised = serving.price_review_group(self.store, self.pid)[1]
        self.assertEqual(reference["references"]["pricing"], revised["references"]["pricing"])
        self.assertEqual(
            reference["history_validation"]["identities"], revised["history_validation"]["identities"]
        )
        for name, prior in before.items():
            current = after[name]
            self.assertEqual(prior["identity_memberships"], current["identity_memberships"])
            self.assertEqual(prior["source_sha256"], current["source_sha256"])
        self.assertEqual(
            [
                ("EARLY", "2026-10-01", "2026-10-05"),
                ("LAST", "2026-10-14", "2026-10-19"),
                ("MIDDLE", "2026-10-06", "2026-10-13"),
            ],
            [
                (x["ticker"], x["first_session"], x["last_session"])
                for x in revised["securities"][0]["ticker_ranges"]
            ],
        )
        intervals = revised["securities"][0]["ticker_ownership_intervals"]
        self.assertEqual(
            [("111111111", "2026-10-06", "2026-10-09"), ("222222222", "2026-10-09", "2026-10-14")],
            [
                (x["cusip"], x["start_inclusive"], x["end_exclusive"])
                for x in intervals
                if x["ticker"] == "MIDDLE"
            ],
        )
        self.assertEqual("2026-10-13T04:00:00Z", after["MIDDLE"]["observation_routes"][self.pid][0])
        for name, state in (("LAST", "selected"), ("MIDDLE", "superseded")):
            docs = storage.read_boundaries(self.root, after[name])
            for mode in docs["current_month"]["adjustments"]:
                closing = mode["segments"][-1]
                self.assertEqual(
                    (["LAST"], "LAST", "222222222", state),
                    (
                        closing["observation_tickers"],
                        closing["historical_symbol"],
                        closing["canonical_security"]["cusip"],
                        closing["observation_selection"],
                    ),
                )

    def append_one_session(self):
        for name, start, end in (
            ("MIDDLE", "2026-10-01", "2026-10-21"),
            ("LAST", "2026-10-14", "2026-10-21"),
            ("EARLY", "2026-10-01", "2026-10-06"),
        ):
            download_pricing(self.store, name, "2026-10-21", client=FakeClient(informative_bars(start, end)))

    def test_append_uses_closed_old_route_and_new_rows_only(self):
        self.run_group()
        maintain(self.store, symbols=["MIDDLE"])
        self.append_one_session()
        with patch.object(
            retained_split_renames, "plan", side_effect=AssertionError("historical projection repeated")
        ):
            result = self.run_group()
        self.assertEqual(2 * len(ADJUSTMENTS), result["results"][0]["pricing_rows_processed"])
        published = maintain(self.store, symbols=["MIDDLE"])
        self.assertTrue(published["complete"], published)
        self.assertEqual(8, published["checked_bars"])
        after = self.receipts()
        for name in ("LAST", "MIDDLE"):
            documents = storage.read_boundaries(self.root, after[name])
            for entry in documents["current_month"]["adjustments"]:
                self.assertEqual(["LAST"], entry["segments"][-1]["observation_tickers"])

    def test_first_post_upgrade_append_preserves_existing_prefix_until_selected_adoption(self):
        self.append_one_session()
        with patch.object(
            retained_split_renames, "plan", side_effect=AssertionError("historical replay on append")
        ):
            first = self.run_group()
        self.assertEqual(2 * len(ADJUSTMENTS), first["results"][0]["pricing_rows_processed"])
        serve = maintain(self.store, symbols=["MIDDLE"])
        self.assertTrue(serve["complete"], serve)
        prior = self.receipts()
        older = storage.read_boundaries(self.root, prior["LAST"])["current_month"]["adjustments"][0]["segments"]
        self.assertIn(["LAST", "MIDDLE"], [part["observation_tickers"] for part in older])
        self.assertEqual(["LAST"], older[-1]["observation_tickers"])
        with patch.object(pipeline, "select_observations", side_effect=AssertionError("numerical replay")):
            adoption = self.run_group()
        self.assertEqual(0, adoption["results"][0]["pricing_rows_processed"])
        serve = maintain(self.store, symbols=["MIDDLE"])
        self.assertEqual(0, serve["checked_bars"])
        final = self.receipts()
        self.assertEqual(prior["LAST"]["source_sha256"], final["LAST"]["source_sha256"])
        parts = storage.read_boundaries(self.root, final["LAST"])["current_month"]["adjustments"][0]["segments"]
        self.assertTrue(all(part["observation_tickers"] == ["LAST"] for part in parts))

    def test_competing_owner_or_literal_print_and_unrelated_completion(self):
        prior = self.receipts()
        docs = {name: storage.read_boundaries(self.root, item) for name, item in prior.items()}
        evidence = {name: provider_evidence(item["evidence"]) for name, item in prior.items()}
        context = {
            name: {
                "correlated": {
                    "segments": [
                        {**seg, "adjustment": view["adjustment"]}
                        for doc in parts.values()
                        for view in doc["adjustments"]
                        for seg in view["segments"]
                        if seg["branch"] == "correlated"
                    ]
                }
            }
            for name, parts in docs.items()
        }
        for reason in ("other_owner", "literal_print", "literal_old", "third_label"):
            with self.subTest(reason=reason):
                held, observed, rows = deepcopy(prior), deepcopy(evidence), deepcopy(context)
                if reason == "other_owner":
                    for seg in rows["LAST"]["correlated"]["segments"]:
                        seg["security_identity"]["security_id"] = "another-owner"
                elif reason == "literal_print":
                    observed["EARLY"]["historical_trade_tape"] = {
                        "asof": "-",
                        "outcome": "observed",
                        "http_status": 200,
                        "symbol": "MIDDLE",
                        "session": "2026-10-15",
                    }
                elif reason == "literal_old":
                    held["MIDDLE"]["pricing_request"]["asof"] = "-"
                else:
                    rows["LAST"]["correlated"]["segments"][0]["observation_tickers"] = [
                        "LAST",
                        "MIDDLE",
                        "OTHER",
                    ]
                chosen, reasons = retained_split_renames.plan(held, observed, rows)
                self.assertFalse(chosen)
                self.assertTrue(reasons)
        # The reused endpoint's literal observation is assigned to a separate
        # dated CUSIP and UUID. A price row alone gives no contrary ownership.
        observed = deepcopy(evidence)
        observed["EARLY"]["otc_historical_trade_tape"] = {
            "following_trade_search": {
                "outcome": "observed",
                "from_session": "2026-10-09",
                "through_session": "2026-10-13",
                "observation": {
                    "asof": "-",
                    "outcome": "observed",
                    "http_status": 200,
                    "symbol": "LAST",
                    "session": "2026-10-09",
                    "observed_session": "2026-10-12",
                },
            }
        }
        rejected, reasons = retained_split_renames.plan(prior, observed, context)
        self.assertFalse(rejected)
        self.assertTrue(
            any(item.get("reason_code") == "split_rename_contrary_literal_trading" for item in reasons)
        )
        sample = deepcopy(context["LAST"]["correlated"]["segments"][0])
        sample.update(
            **{"from": "2026-10-12", "to": "2026-10-12"},
            historical_symbol="LAST",
            observation_tickers=["LAST"],
            security_identity={"security_id": "separate", "member_alpaca_asset_ids": ["earlier-issuer"]},
            ticker_ownership_interval={
                "historical_symbol": "LAST",
                "asset_id": "earlier-issuer",
                "cusip": "OTHER",
            },
        )
        context["LAST"]["correlated"]["segments"].append(sample)
        accepted, _ = retained_split_renames.plan(prior, observed, context)
        self.assertEqual([self.pid], [p.owner for p in accepted])
        changed = deepcopy(prior)
        for receipt in changed.values():
            receipt.pop("uuid_lineage_edges")
        with patch.object(
            storage, "read_assignment_projection", side_effect=AssertionError("unrelated assignment read")
        ):
            self.assertFalse(label_updates.split_rename_pending(changed, labels_current=True))

    def test_negative_native_completion_with_legacy_binding_and_changed_counterevidence(self):
        prior = self.receipts()
        for receipt in prior.values():
            receipt.pop("lineage_evidence_sha256", None)
        prior["LAST"]["evidence"]["historical_trade_tape"] = {
            "asof": "-",
            "outcome": "observed",
            "http_status": 200,
            "symbol": "MIDDLE",
            "session": "2026-10-15",
        }
        evidence = {name: provider_evidence(item["evidence"]) for name, item in prior.items()}
        index = storage.read_index(self.root, "MIDDLE", group=True)
        refs = {name: row["sha256"] for name, row in index["symbols"].items()}
        sources = {name: {"symbol": name, "boundaries": r["pricing_boundaries"]} for name, r in prior.items()}
        with patch.object(dated_labels, "plan", side_effect=AssertionError("completed v1 decision replay")):
            resolved, changed, trace = label_updates.complete(
                self.store,
                self.root,
                prior,
                sources,
                evidence,
                refs,
                {},
                assignment_cache={},
                split_renames=True,
                labels_current=True,
            )
        self.assertFalse(changed)
        self.assertTrue(trace["reasons"])
        for receipt in resolved.values():
            label_updates.validate_completion(receipt)
        self.assertTrue(label_updates.applicable(resolved))
        self.assertFalse(label_updates.pending(resolved, evidence))
        self.assertFalse(label_updates.split_rename_pending(resolved, labels_current=True))
        newer = deepcopy(resolved)
        newer["LAST"]["evidence"]["historical_trade_tape"]["asof"] = "2026-10-13"
        self.assertTrue(label_updates.split_rename_pending(newer, labels_current=True))
        malformed = deepcopy(resolved["LAST"])
        malformed["dated_label_completion"]["sources"] = None
        with self.assertRaises(LineageError):
            label_updates.validate_completion(malformed)
        with patch.object(retained_split_renames, "possible", side_effect=AssertionError("single-source scan")):
            self.assertFalse(label_updates.split_rename_pending({"LAST": prior["LAST"]}, labels_current=True))

    def test_declared_session_grid_is_required_and_changed_selector_cannot_lend_dates(self):
        prior = self.receipts()
        generation, group = serving.price_review_group(self.store, self.pid)
        bindings = group["references"]["lineage"]
        cuts = retained_split_renames.SessionCuts(self.store, prior, bindings)
        broken = deepcopy(group)
        broken["history_validation"]["prefix_records"][0].pop("session_dates")
        with patch.object(serving, "price_review_group", return_value=(generation, broken)):
            with self.assertRaisesRegex(LineageError, "session-date projection"):
                cuts._record(self.pid)
        changed = deepcopy(group)
        changed["references"]["lineage"]["MIDDLE"] = "different"
        with patch.object(serving, "price_review_group", return_value=(generation, changed)):
            self.assertIsNone(retained_split_renames.SessionCuts(self.store, prior, bindings)._record(self.pid))
