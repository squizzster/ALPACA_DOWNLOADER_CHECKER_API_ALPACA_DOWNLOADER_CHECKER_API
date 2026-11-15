"""Normal selected lineage automatically completes historical label collisions."""

import tempfile
from dataclasses import replace
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, calendar_bars
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_lineage_build import dated_labels, label_updates, pipeline, policy_engine, storage
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.download import download_pricing


class LabelCompletionTests(TestCase):
    def fixture(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        store = Path(directory) / "store"
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-10-06",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        current = asset("ALIAS", "same")
        client = IdentityClient(
            {"OLD": asset("OLD", "same"), "NEW": asset("NEW", "same"), "ALIAS": current, "111111111": current},
            [action],
        )
        for name, start, end in (
            ("OLD", "2026-10-01", "2026-10-05"),
            ("NEW", "2026-10-05", "2026-10-09"),
            ("ALIAS", "2026-10-01", "2026-10-09"),
        ):
            download_pricing(store, name, "2026-10-09", client=FakeClient(calendar_bars(start, end)))
        control = OperationContext(store)
        control._ruleset = replace(control.accept_ruleset(), dated_labels=None)
        evidence = EvidenceClient(client)
        self.assertFalse(
            pipeline.run_lineage(
                store, ["OLD", "NEW", "ALIAS"], identity_client=client, evidence_client=evidence, context=control
            )["failed"]
        )
        return store, client, evidence

    def run_group(self, store, client, evidence, context):
        result = pipeline.run_lineage(
            store, ["OLD", "NEW", "ALIAS"], identity_client=client, evidence_client=evidence, context=context
        )
        self.assertFalse(result["failed"], result)
        return result

    def test_normal_configuration_corrects_history_once_without_owner_or_price_decisions(self):
        store, client, evidence = self.fixture()
        context = OperationContext(store)
        requests = client.request_count
        with (
            patch.object(
                policy_engine, "build_security_correlation_report", side_effect=AssertionError("owner replay")
            ),
            patch.object(pipeline, "select_observations", side_effect=AssertionError("numerical replay")),
            patch.object(dated_labels, "plan", wraps=dated_labels.plan) as plan,
            patch.object(pricing_storage, "read_artifact", wraps=pricing_storage.read_artifact) as payload,
        ):
            result = self.run_group(store, client, evidence, context)
        self.assertEqual(1, plan.call_count)
        self.assertEqual(requests, client.request_count)
        self.assertLessEqual(
            sum(call.kwargs.get("purpose") == "selected_label_dates" for call in payload.call_args_list),
            1,
            "physical adjustment aliases must share date assembly",
        )
        self.assertEqual(0, result["results"][0]["pricing_rows_processed"])
        root = store / "end_of_day/lineage"
        index = storage.read_index(root)
        prior = {name: storage.read_receipt(root, name, index) for name in index["symbols"]}
        self.assertTrue(all(item.get("dated_label_completion") for item in prior.values()))
        self.assertFalse(label_updates.pending(prior, {name: item["evidence"] for name, item in prior.items()}))
        with (
            patch.object(dated_labels, "plan", side_effect=AssertionError("completed label decision replay")),
            patch.object(
                storage, "read_assignment_projection", side_effect=AssertionError("accepted boundary reread")
            ),
            patch.object(label_updates._Dates, "__getitem__", side_effect=AssertionError("old payload reread")),
        ):
            self.run_group(store, client, evidence, OperationContext(store))
        changed = {name: dict(item["evidence"]) for name, item in prior.items()}
        changed["OLD"] = {**changed["OLD"], "corporate_actions": []}
        self.assertTrue(label_updates.pending(prior, changed))
        changed_sources = {name: dict(item) for name, item in prior.items()}
        changed_sources["ALIAS"]["source_sha256"] = "changed"
        self.assertTrue(
            label_updates.pending(changed_sources, {name: item["evidence"] for name, item in prior.items()})
        )

    def test_changed_request_invalidates_even_when_source_and_assignments_are_unchanged(self):
        previous = {
            "A": {
                "source_sha256": "source",
                "boundaries": {},
                "pricing_request": {"start": "2030-01-01"},
                "evidence": {"corporate_actions": []},
            }
        }
        stamped = label_updates.stamp(previous, {"A": {"corporate_actions": []}}, ({}, []))
        self.assertTrue(label_updates.applicable(stamped))
        stamped["A"] = {**stamped["A"], "pricing_request": {"start": "2030-02-01"}}
        self.assertFalse(label_updates.applicable(stamped))
        self.assertNotEqual(
            stamped["A"]["dated_label_completion"],
            label_updates.binding(stamped, {"A": {"corporate_actions": []}}),
        )

    def test_failure_before_journal_preserves_completed_projection_for_retry(self):
        store, client, evidence = self.fixture()
        context = OperationContext(store)
        with (
            patch.object(storage, "publish", side_effect=RuntimeError("publication interruption")),
            patch.object(dated_labels, "plan", wraps=dated_labels.plan) as plan,
        ):
            result = pipeline.run_lineage(
                store, ["OLD", "NEW", "ALIAS"], identity_client=client, evidence_client=evidence, context=context
            )
        self.assertGreater(result["failed"], 0)
        self.assertEqual(1, plan.call_count)
        with (
            patch.object(dated_labels, "plan", side_effect=AssertionError("completed projection replay")),
            patch.object(
                label_updates._Dates, "__getitem__", side_effect=AssertionError("completed dates reread")
            ),
            patch.object(
                storage, "read_assignment_projection", side_effect=AssertionError("completed assignments reread")
            ),
        ):
            self.run_group(store, client, evidence, context)

    def test_append_carries_completed_relation_and_only_labels_new_owned_suffix(self):
        store, client, evidence = self.fixture()
        self.run_group(store, client, evidence, OperationContext(store))
        for name, start, end in (
            ("OLD", "2026-10-01", "2026-10-05"),
            ("NEW", "2026-10-05", "2026-10-12"),
            ("ALIAS", "2026-10-01", "2026-10-12"),
        ):
            download_pricing(store, name, "2026-10-12", client=FakeClient(calendar_bars(start, end)))
        with patch.object(
            dated_labels, "plan", side_effect=AssertionError("accepted historical relation repeated")
        ):
            self.run_group(store, client, evidence, OperationContext(store))
        root = store / "end_of_day/lineage"
        index = storage.read_index(root)
        for name in ("NEW", "ALIAS"):
            receipt = storage.read_receipt(root, name, index)
            self.assertTrue(receipt.get("dated_label_completion"))
            documents = storage.read_boundaries(root, receipt)
            for document in documents.values():
                for entry in document["adjustments"]:
                    for segment in entry["segments"]:
                        if segment["branch"] == "correlated" and segment["from"] == "2026-10-09":
                            self.assertEqual(["NEW"], segment["observation_tickers"])
        with patch.object(dated_labels, "plan", side_effect=AssertionError("append completion repeated")):
            self.run_group(store, client, evidence, OperationContext(store))

    def test_durable_journal_installs_completed_labels_without_projection_or_dates(self):
        class Interrupted(BaseException):
            pass

        store, client, evidence = self.fixture()
        with patch.object(storage, "_install", side_effect=Interrupted()):
            with self.assertRaises(Interrupted):
                self.run_group(store, client, evidence, OperationContext(store))
        with (
            patch.object(dated_labels, "plan", side_effect=AssertionError("journal replayed label inference")),
            patch.object(
                label_updates._Dates, "__getitem__", side_effect=AssertionError("journal reopened dates")
            ),
            patch.object(
                policy_engine,
                "build_security_correlation_report",
                side_effect=AssertionError("journal replayed ownership"),
            ),
        ):
            result = self.run_group(store, client, evidence, OperationContext(store))
        self.assertTrue(result["recovered"])

    def test_fresh_two_source_action_labels_are_completed_once(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        store = Path(directory) / "store"
        current = asset("MAPPED", "same")
        client = IdentityClient(
            {"HISTORIC": asset("HISTORIC", "same"), "MAPPED": current, "111111111": current},
            [
                {
                    "action_type": "cash_dividend",
                    "id": "dividend",
                    "symbol": "HISTORIC",
                    "ex_date": "2026-10-05",
                    "process_date": "2026-10-06",
                    "cusip": "111111111",
                }
            ],
        )
        for name in ("HISTORIC", "MAPPED"):
            download_pricing(
                store, name, "2026-10-09", client=FakeClient(calendar_bars("2026-10-01", "2026-10-09"))
            )
        for name in ("HISTORIC", "MAPPED"):
            download_pricing(
                store, name, "2026-10-12", client=FakeClient(calendar_bars("2026-10-01", "2026-10-09"))
            )
        evidence = EvidenceClient(client)
        result = pipeline.run_lineage(
            store,
            ["HISTORIC", "MAPPED"],
            identity_client=client,
            evidence_client=evidence,
            context=OperationContext(store),
        )
        self.assertFalse(result["failed"], result)
        root = store / "end_of_day/lineage"
        index = storage.read_index(root)
        receipts = {name: storage.read_receipt(root, name, index) for name in index["symbols"]}
        self.assertTrue(all(item.get("dated_label_completion") for item in receipts.values()))
        self.assertTrue(label_updates.applicable(receipts))
        self.assertTrue(all(item["dated_label_plan"]["reasons"] for item in receipts.values()))
        with patch.object(
            dated_labels, "plan", side_effect=AssertionError("completed two-source decision replay")
        ):
            result = pipeline.run_lineage(
                store,
                ["HISTORIC", "MAPPED"],
                identity_client=client,
                evidence_client=evidence,
                context=OperationContext(store),
            )
        self.assertFalse(result["failed"], result)

    def test_consumer_episodes_exclude_unique_superseded_private_label(self):
        from libs.alpaca_lineage_build.metadata import provider_evidence

        store, _client, _evidence = self.fixture()
        root = store / "end_of_day/lineage"
        index = storage.read_index(root)
        receipts = {name: storage.read_receipt(root, name, index) for name in index["symbols"]}
        documents = {name: storage.read_boundaries(root, receipt) for name, receipt in receipts.items()}
        altered = 0
        for parts in documents.values():
            for document in parts.values():
                for view in document["adjustments"]:
                    for segment in view["segments"]:
                        if segment.get("observation_selection") == "superseded":
                            segment["observation_tickers"] = ["PRIVATE"]
                            altered += 1
        self.assertTrue(altered)
        evidence = {name: provider_evidence(receipt["evidence"]) for name, receipt in receipts.items()}
        output, _changed, trace = dated_labels.project_completed_group(
            receipts, documents, evidence, dict.fromkeys(receipts, "generation")
        )
        self.assertTrue(trace["changes"])
        self.assertTrue(
            all(
                episode["symbol"] != "PRIVATE"
                for receipt in output.values()
                for episodes in receipt["dated_market_projection"]["identities"].values()
                for episode in episodes
            )
        )
