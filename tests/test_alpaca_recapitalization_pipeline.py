"""Own-common recovery extends accepted history without repeating old decisions."""

import copy
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient
from test_alpaca_recapitalizations import recap_fact

from alpaca_end_of_day import DataLibrary
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy, work
from libs.alpaca_serving_catalogue.producer import maintain


class RecapitalizationPipelineTests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.action = {
            "id": "capital-event",
            "action_type": "stock_and_cash_merger",
            "acquiree_symbol": "SAME",
            "acquirer_symbol": "SAME",
            "acquiree_cusip": "111111111",
            "acquirer_cusip": "222222222",
            "acquiree_rate": 27,
            "acquirer_rate": 26,
            "cash_rate": 1.04,
            "effective_date": "2024-01-05",
            "process_date": "2024-01-05",
        }
        self.assets = {
            "SAME": asset("SAME", "new"),
            "111111111": asset("SAME", "old"),
            "222222222": asset("SAME", "new"),
        }
        self.assets["111111111"]["status"] = "inactive"
        self.assets["000000000"] = {**asset("SAME", "earliest"), "status": "inactive"}
        self.earlier_action = {
            **self.action,
            "id": "earlier-capital-event",
            "acquiree_cusip": "000000000",
            "acquirer_cusip": "111111111",
            "effective_date": "2024-01-03",
            "process_date": "2024-01-03",
        }
        self.client = IdentityClient(
            self.assets,
            [
                self.earlier_action,
                self.action,
                {
                    "id": "later-dividend",
                    "action_type": "cash_dividend",
                    "symbol": "SAME",
                    "cusip": "222222222",
                    "rate": 0.2,
                    "ex_date": "2024-01-10",
                    "process_date": "2024-01-10",
                },
            ],
        )
        download_pricing(
            self.store,
            "SAME",
            "2024-01-12",
            from_date="2024-01-01",
            client=FakeClient(informative_bars("2024-01-02", "2024-01-12")),
        )
        held = pipeline.correlate
        self.completed_initial_reports = {}

        def initial_reports(*args, **kwargs):
            reports = held(*args, **kwargs)
            self.completed_initial_reports.update(copy.deepcopy(reports))
            return reports

        with patch.object(pipeline, "correlate", side_effect=initial_reports):
            built = pipeline.run_lineage(self.store, ["SAME"], identity_client=self.client)
        self.assertEqual(0, built["failed"], built)
        self.prior = storage.read_receipt(self.root, "SAME", storage.read_index(self.root))
        self.assertGreater(self.prior["unresolved_rows"], 0)
        self.assertEqual(0, maintain(self.store, bootstrap=True)["failed"])
        self.fact = recap_fact(self.action)
        self.fact["facts"].update(
            legal_effective_date="2024-01-04",
            exchange_effective_date="2024-01-05",
            predecessor_history_start=None,
        )
        self.earlier_fact = recap_fact(self.earlier_action)
        self.earlier_fact["facts"].update(
            old_cusip="000000000",
            new_cusip="111111111",
            legal_effective_date="2024-01-03",
            exchange_effective_date="2024-01-03",
            predecessor_history_start="2024-01-01",
        )

    def repair(self):
        from libs.alpaca_end_of_day_security_correlation import row_correlation
        from libs.alpaca_lineage_build import observations, transform

        first = min(item["from"] for item in self.prior["identity_intervals"])
        last = max(item["to"] for item in self.prior["identity_intervals"])
        decide, select = row_correlation._decide_row, observations._select_new_observations

        def new_owner(context, **kwargs):
            self.assertFalse(first <= context.session_date.isoformat() <= last, "accepted row rules repeated")
            return decide(context, **kwargs)

        def new_choice(inputs, reports, **kwargs):
            for report in reports.values():
                for segment in report["correlated"]["segments"]:
                    self.assertTrue(
                        segment.get("to", segment["last_timestamp"][:10]) < first
                        or segment.get("from", segment["first_timestamp"][:10]) > last,
                        "accepted numerical decision repeated",
                    )
            return select(inputs, reports, **kwargs)

        from libs.alpaca_end_of_day_security_correlation.security_identity import (
            security_identity_for_alpaca_assets,
        )
        from libs.alpaca_lineage_build import pricing
        from libs.alpaca_lineage_build.context import OperationContext
        from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan

        if not hasattr(self, "completion_context"):
            self.completion_context = OperationContext(self.store)
            self.generic_completion_done = False
        context = self.completion_context
        if not self.generic_completion_done:
            context.completed_membership_plans[("SAME",)] = {"store": str(self.store), "plan": {"changes": []}}

        def completed_decisions(controls, evidence, auxiliary, *, cache, **_):
            # This consumer fixture begins at a completed generic source-decision
            # boundary. Its exact positions and owner result are literals, not
            # researched assertions supplied to an inference engine.
            control = controls["SAME"]
            key = "SAME", control.source_sha256
            values = cache.setdefault("pricing", {})
            if key not in values:
                values[key] = pricing.load_pricing(
                    self.store,
                    "SAME",
                    source=control.source,
                    source_sha256=control.source_sha256,
                    partitions=cache.setdefault("partitions", {}).setdefault("SAME", {}),
                )
            view = values[key]
            identity = security_identity_for_alpaca_assets(
                ["earliest", "old", "new"],
                [],
                tickers=["SAME"],
                basis="completed_provider_component",
            ).as_report_object()
            report = copy.deepcopy(self.completed_initial_reports["SAME"])
            template = report["correlated"]["segments"][0]
            segments = []
            for adjustment, rows in view.views.items():
                for position, row in enumerate(rows, 1):
                    day = row[0][:10]
                    dated = (
                        ("earliest", "000000000")
                        if day < "2024-01-03"
                        else (("old", "111111111") if day < "2024-01-05" else ("new", "222222222"))
                    )
                    segments.append(
                        {
                            **template,
                            "adjustment": adjustment,
                            "row_start": position,
                            "row_end": position,
                            "row_count": 1,
                            "first_timestamp": row[0],
                            "last_timestamp": row[0],
                            "security_identity": identity,
                            "historical_symbol": "SAME",
                            "canonical_security": {
                                "security_id": identity["security_id"],
                                "alpaca_asset_id": dated[0],
                                "cusip": dated[1],
                            },
                            "correlation_rule": {"name": "completed_provider_source_decision"},
                            "observation_tickers": ["SAME"],
                        }
                    )
            report.update(
                source_row_count=len(segments),
                source_downloads=view.local["downloads"],
                correlated={"row_count": len(segments), "segments": segments},
                uncorrelated={"row_count": 0, "segments": []},
                retained_observation_routes=control.previous["observation_routes"],
            )
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            return SourceWorkPlan(
                {"SAME": "B"},
                dict.fromkeys(identity["member_alpaca_asset_ids"], identity),
                empty,
                empty,
                relations,
                relations,
                {},
                False,
                prepared_reports={"SAME": report},
                stable_numerical_identities=frozenset({identity["security_id"]}),
            )

        with (
            patch.object(row_correlation, "_decide_row", side_effect=new_owner),
            patch.object(observations, "_select_new_observations", side_effect=new_choice),
            patch.object(
                transform, "correlate", side_effect=AssertionError("completed owner decision repeated")
            ),
            patch.object(pipeline, "prepare_group_evidence_and_source_work", side_effect=completed_decisions),
        ):
            result = pipeline.run_lineage(self.store, ["SAME"], identity_client=self.client, context=context)
        if result["failed"] == 0:
            self.generic_completion_done = True
            context.completed_membership_plans.pop(("SAME",), None)
        return result

    def test_recovered_prefix_and_new_append_share_the_accepted_middle(self):
        download_pricing(
            self.store, "SAME", "2024-01-13", client=FakeClient(informative_bars("2024-01-02", "2024-01-13"))
        )
        result = self.repair()
        self.assertEqual(0, result["failed"], result)
        check = policy._bar_reasons

        def fresh_only(bar, adjustment):
            self.assertNotIn(bar["session"], {"2024-01-10", "2024-01-11"})
            return check(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=fresh_only):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(self.prior["unresolved_rows"] + 8, served["checked_bars"])

    def test_recovers_earlier_dates_and_only_checks_the_extension(self):
        self._check_recovered_extension(transfer_expected=True)

    def _check_recovered_extension(self, *, transfer_expected):
        repaired = self.repair()
        self.assertEqual(0, repaired["failed"], repaired)
        receipt = storage.read_receipt(self.root, "SAME", storage.read_index(self.root))
        self.assertEqual(0, receipt["unresolved_rows"])
        if transfer_expected:
            self.assertIsNotNone(receipt["membership_only_rebind"])
        else:
            # No new membership decision was needed. Keep the earlier completed
            # donor binding; serving may already have consumed it, but a new
            # publication is not permission to discard that applicable handoff.
            self.assertEqual(self.prior["membership_only_rebind"], receipt["membership_only_rebind"])
        check = policy._bar_reasons
        checked = []
        accepted_first = min(item["from"] for item in self.prior["identity_intervals"])

        def fresh_only(bar, adjustment):
            self.assertLess(bar["session"], accepted_first, "accepted later OHLC rechecked")
            checked.append((bar["session"], adjustment))
            return check(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=fresh_only):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(0, served["full_history_evaluations"])
        self.assertEqual(1, served["reused_history_prefixes"])
        self.assertEqual(self.prior["unresolved_rows"], len(checked))
        self.assertEqual(1, DataLibrary(store=self.store).online_count())
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed extension repeated")):
            self.assertTrue(maintain(self.store)["complete"])

    def _establish_accepted_membership(self):
        """Earlier membership work left its accepted unresolved rows closed."""
        from libs.alpaca_end_of_day_security_correlation.security_identity import (
            security_identity_for_alpaca_assets,
        )
        from libs.alpaca_lineage_build import pricing, source_work
        from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy

        with storage.writer_lock(self.store) as root:
            index = storage.read_index(root)
            with storage.pricing_locks(self.store, {"SAME"}, known=index["symbols"]):
                source = pricing.load_source(self.store / "end_of_day/pricing/SAME", "SAME")
                control = pricing.metadata_source_control(
                    self.store, "SAME", source, self.prior["source_sha256"], self.prior
                )
                control.previous_receipt_generation = index["symbols"]["SAME"]["sha256"]
                identity = security_identity_for_alpaca_assets(
                    ["earliest", "old", "new"], [], tickers=["SAME"], basis="accepted_membership_fixture"
                ).as_report_object()
                empty = {"version": 1, "certificates": [], "issues": []}
                plan = source_work.SourceWorkPlan(
                    {"SAME": "A"},
                    dict.fromkeys(identity["member_alpaca_asset_ids"], identity),
                    empty,
                    empty,
                    {},
                    self.prior["security_relations"],
                    {},
                    True,
                )
                receipts, documents, _, _ = source_work.execute_source_work(
                    self.store,
                    root,
                    {"SAME": control},
                    {"SAME": self.prior["evidence"]},
                    {},
                    plan,
                    load_authorized_policy(),
                    {},
                    loader=lambda *a, **k: self.fail("accepted pricing opened during membership work"),
                )
                storage.publish(
                    self.store,
                    root,
                    index,
                    receipts,
                    documents,
                    {},
                    previous={"SAME": self.prior},
                    sources={"SAME": source},
                )
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("membership work repeated OHLC")):
            self.assertTrue(maintain(self.store)["complete"])
        self.prior = receipts["SAME"]

    def test_same_public_id_prepend_preserves_the_accepted_middle(self):
        self._establish_accepted_membership()
        before = self.prior["identity_memberships"][0]["security_id"]
        self._check_recovered_extension(transfer_expected=False)
        current = storage.read_receipt(self.root, "SAME", storage.read_index(self.root))
        self.assertEqual(before, current["identity_memberships"][0]["security_id"])

    def test_same_public_id_prepend_and_append(self):
        self._establish_accepted_membership()
        self.test_recovered_prefix_and_new_append_share_the_accepted_middle()

    def test_same_public_id_acknowledgement_interruption(self):
        self._establish_accepted_membership()
        self.test_committed_extension_survives_acknowledgement_interruption()

    def test_committed_extension_survives_acknowledgement_interruption(self):
        class Interrupted(BaseException):
            pass

        self.assertEqual(0, self.repair()["failed"])
        with patch.object(work, "acknowledge", side_effect=Interrupted()), self.assertRaises(Interrupted):
            maintain(self.store)
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("committed checks repeated")):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual({}, work.read(self.store))

    def test_later_ordinary_append_keeps_recovered_and_accepted_history_closed(self):
        self.assertEqual(0, self.repair()["failed"])
        self.assertTrue(maintain(self.store)["complete"])
        download_pricing(
            self.store, "SAME", "2024-01-13", client=FakeClient(informative_bars("2024-01-02", "2024-01-13"))
        )
        result = pipeline.run_lineage(self.store, ["SAME"], identity_client=self.client)
        self.assertEqual(0, result["failed"], result)
        check = policy._bar_reasons

        def append_only(bar, adjustment):
            self.assertEqual("2024-01-12", bar["session"])
            return check(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=append_only):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(8, served["checked_bars"])

    def test_authored_interpretation_is_rejected_without_mutating_completed_lineage(self):
        from libs.alpaca_lineage_build.contract import LineageError

        before = storage.read_index(self.root)["symbols"]["SAME"]["sha256"]
        count = self.client.request_count
        with self.assertRaisesRegex(LineageError, "inadmissible"):
            pipeline.run_lineage(
                self.store,
                ["SAME"],
                identity_client=self.client,
                event_supplements=[self.earlier_fact, self.fact],
            )
        self.assertEqual(self.client.request_count, count)
        self.assertEqual(storage.read_index(self.root)["symbols"]["SAME"]["sha256"], before)
