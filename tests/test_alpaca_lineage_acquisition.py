"""Pricing requestability must not erase independently supported security history."""

import io
import json
import unittest
import urllib.error
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import test_alpaca_lineage_build as fixtures
from test_alpaca_lineage_build import IdentityClient, asset, calendar_bars
from test_alpaca_pricing_download import FakeClient, FakeClock, snapshot

from alpaca_end_of_day import StandaloneReader, StandaloneReaderError
from libs.alpaca_lineage_build import contract, pipeline, storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.evidence import output_evidence
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.reader import read_lineage
from libs.alpaca_pricing_download.contract import (
    BarsSymbolUnavailable,
    DownloadError,
    InitialPricingUnavailable,
    timestamp,
)
from libs.alpaca_pricing_download.download import download_pricing


class AuxiliaryAcquisitionTests(unittest.TestCase):
    setUp = fixtures.LineageBuildTests.setUp
    download = fixtures.LineageBuildTests.download
    receipt = fixtures.LineageBuildTests.receipt
    notice = fixtures.LineageBuildTests.notice
    install_legacy_rule_contract = fixtures.LineageBuildTests.install_legacy_rule_contract

    def prepare_entitlement(self):
        self.bars = calendar_bars("2026-09-28", "2026-10-15")
        self.download("BUYER", end="2026-10-15")
        self.download("TARGET", end="2026-10-15", bars=self.bars[:3])
        buyer, target, entitlement = (asset(name) for name in ("BUYER", "TARGET", "ENTITLE"))
        target.update(status="inactive", tradable=False)
        entitlement.update(name="Corporate action entitlement", status="inactive", tradable=False)
        return IdentityClient(
            {
                "BUYER": buyer,
                "TARGET": target,
                "ENTITLE": entitlement,
                "111111111": buyer,
                "222222222": target,
                "333333333": entitlement,
            },
            [
                {
                    "id": "distribution",
                    "action_type": "unit_split",
                    "process_date": "2026-10-01",
                    "effective_date": "2026-10-01",
                    "old_symbol": "TARGET",
                    "old_cusip": "222222222",
                    "old_rate": 1,
                    "new_symbol": "BUYER",
                    "new_cusip": "111111111",
                    "new_rate": 2,
                    "alternate_symbol": "ENTITLE",
                    "alternate_cusip": "333333333",
                    "alternate_rate": 1,
                },
                {
                    "id": "entitlement-payment",
                    "action_type": "cash_merger",
                    "process_date": "2026-10-05",
                    "effective_date": "2026-10-05",
                    "acquiree_symbol": "ENTITLE",
                    "acquiree_cusip": "333333333",
                    "rate": 5,
                },
            ],
        )

    def rejected_client(self, *, status=400, message=None):
        calls = []

        def opener(request, **kwargs):
            parameters = parse_qs(urlparse(request.full_url).query)
            calls.append(parameters)
            symbol = parameters["symbols"][0]
            body = json.dumps({"message": message or f"invalid symbol: {symbol}"}).encode()
            raise urllib.error.HTTPError(request.full_url, status, "rejected", {}, io.BytesIO(body))

        client = FakeClock().client(opener=opener)
        return client, calls

    def test_rejected_auxiliary_does_not_withhold_independent_owners(self):
        identities = self.prepare_entitlement()
        pricing, requests = self.rejected_client()
        result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["processed"], ["BUYER", "TARGET"])
        self.assertEqual(len(requests), 1)
        receipt = self.receipt("BUYER")
        self.assertEqual(set(receipt["input_sources"]), {"BUYER", "TARGET"})
        self.assertIn("ENTITLE", receipt["dependencies"])
        self.assertIn("ENTITLE", receipt["unavailable_pricing"])
        self.assertIn(
            "entitlement-payment",
            {
                action["id"]
                for action in receipt["unavailable_pricing"]["ENTITLE"]["evidence"]["corporate_actions"]
            },
        )
        self.assertFalse((self.store / "end_of_day/pricing/ENTITLE/source.manifest.json").exists())
        self.assertFalse((self.root / "ENTITLE/lineage.manifest.json").exists())
        reader = StandaloneReader(self.store)
        history = reader.get_history("BUYER")
        self.assertEqual(history["bar_count"], len(self.bars))
        self.assertEqual(len(reader.resolve_ticker("TARGET")["matches"]), 1)
        self.assertNotEqual(
            history["security"]["public_id"], reader.resolve_ticker("TARGET")["matches"][0]["public_id"]
        )

    def test_unchanged_publication_trusts_absence_but_refresh_retries(self):
        identities = self.prepare_entitlement()
        pricing, requests = self.rejected_client()
        self.assertEqual(
            run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)["failed"], 0
        )
        before = snapshot(self.store)
        count = identities.request_count
        result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(identities.request_count, count)
        self.assertEqual(len(requests), 1)
        self.assertEqual(before, snapshot(self.store))
        result = run_lineage(
            self.store, ["BUYER"], identity_client=identities, pricing_client=pricing, refresh=True
        )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(requests), 2)

    def test_absent_seed_and_non_specific_errors_are_hard_failures(self):
        identities = self.prepare_entitlement()
        for symbol, status, message in (
            ("ENTITLE", 400, None),
            ("BUYER", 404, None),
            ("BUYER", 401, None),
            ("BUYER", 400, "invalid symbol: SOMEONE_ELSE"),
            ("BUYER", 400, "invalid date range"),
        ):
            with self.subTest(symbol=symbol, status=status, message=message):
                pricing, _ = self.rejected_client(status=status, message=message)
                result = run_lineage(self.store, [symbol], identity_client=identities, pricing_client=pricing)
                self.assertEqual(result["failed"], 1, result)
                self.assertIsNone(self.receipt("BUYER"))
                self.assertTrue(self.notice("BUYER").exists())

    def test_rule_only_change_reuses_locked_absence_and_approved_auxiliary_metadata(self):
        identities = self.prepare_entitlement()
        pricing, requests = self.rejected_client()
        first = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(first["failed"], 0, first)
        previous = self.receipt("BUYER")
        self.install_legacy_rule_contract("BUYER")
        count = identities.request_count
        with (
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata reacquisition")),
            patch.object(pipeline, "acquire_unpriced", side_effect=AssertionError("auxiliary reacquisition")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing acquisition")),
        ):
            result = run_lineage(self.store, ["BUYER"], identity_client=identities)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(result["results"][0]["status"], "processed")
            self.assertEqual(self.receipt("BUYER")["versions"], contract.versions())
        self.assertEqual(self.receipt("BUYER")["unavailable_pricing"], previous["unavailable_pricing"])
        self.assertEqual(identities.request_count, count)
        self.assertEqual(len(requests), 1)

    def test_metadata_failure_is_not_hidden_by_bars_rejection(self):
        identities = self.prepare_entitlement()
        pricing, _ = self.rejected_client()
        original = identities.get_corporate_actions

        def actions(symbol, *args):
            if symbol == "ENTITLE":
                raise RuntimeError("metadata transport failed")
            return original(symbol, *args)

        identities.get_corporate_actions = actions
        result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(result["failed"], 1, result)
        self.assertIn("metadata transport failed", result["results"][0]["error"])
        self.assertIsNone(self.receipt("BUYER"))
        self.assertTrue(self.notice("BUYER").exists())

    def test_later_auxiliary_prices_invalidate_every_reader_and_resume_group(self):
        identities = self.prepare_entitlement()
        pricing, _ = self.rejected_client()
        self.assertEqual(
            run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)["failed"], 0
        )
        reader = StandaloneReader(self.store, cache_bytes=1024 * 1024)
        reader.get_history("BUYER")
        with reader.open_group("BUYER") as group:
            self.assertEqual(group.summary["source_symbols"], ["BUYER", "TARGET"])
        group.check_current()
        self.download("ENTITLE", end="2026-10-15", bars=self.bars[3:])
        with self.assertRaisesRegex(StandaloneReaderError, "previously unavailable pricing changed"):
            reader.get_history("BUYER")
        with self.assertRaisesRegex(StandaloneReaderError, "previously unavailable pricing changed"):
            group.check_current()
        with self.assertRaisesRegex(LineageError, "previously unavailable pricing changed"):
            read_lineage(self.store, "BUYER")
        inspection = output_evidence(self.store, "BUYER", offline=True)
        self.assertEqual(inspection["sources"]["BUYER"]["freshness"]["changed_pricing_inputs"], ["ENTITLE"])
        result = run_lineage(self.store, ["ENTITLE"], identity_client=identities)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["processed"], ["BUYER", "ENTITLE", "TARGET"])
        self.assertNotIn("unavailable_pricing", self.receipt("BUYER"))
        self.assertEqual(reader.get_history("BUYER")["bar_count"], len(self.bars))

    def test_rejection_discovers_descendant_and_rechecks_expanded_range(self):
        identities = self.prepare_entitlement()
        self.download("LATER", end="2026-10-16", bars=self.bars)
        identities.assets["LATER"] = asset("LATER")
        identities.actions.append(
            {
                "id": "auxiliary-descendant",
                "action_type": "unit_split",
                "process_date": "2026-10-05",
                "old_symbol": "ENTITLE",
                "new_symbol": "LATER",
                "old_rate": 1,
                "new_rate": 1,
            }
        )
        rejected, requests = self.rejected_client()
        ordinary = FakeClient(self.bars)

        class MixedPricing:
            def fetch(self, symbol, *args, **kwargs):
                return (rejected if symbol == "ENTITLE" else ordinary).fetch(symbol, *args, **kwargs)

        result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=MixedPricing())
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual([request["asof"][0] for request in requests], ["2026-10-15", "2026-10-16"])
        receipt = self.receipt("BUYER")
        self.assertEqual(set(receipt["input_sources"]), {"BUYER", "TARGET", "LATER"})
        absent = receipt["unavailable_pricing"]["ENTITLE"]
        self.assertEqual(absent["evidence"]["query"]["corporate_actions_end"], "2026-10-16")
        self.assertIn("ENTITLE", self.receipt("LATER")["dependencies"])

    def test_failed_or_partial_acquisition_never_becomes_approved_absence(self):
        self.prepare_entitlement()
        before = snapshot(self.store / "end_of_day/pricing/BUYER")
        rejection, _ = self.rejected_client()
        with self.assertRaises(DownloadError) as raised:
            download_pricing(self.store, "BUYER", "2026-10-16", client=rejection)
        self.assertNotIsInstance(raised.exception, InitialPricingUnavailable)
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing/BUYER"))
        client = FakeClient([])
        with patch.object(client, "fetch", side_effect=[[], BarsSymbolUnavailable("EMPTYFIRST", 400)]):
            with self.assertRaisesRegex(DownloadError, "established pricing acquisition") as raised:
                download_pricing(self.store, "EMPTYFIRST", "2026-10-15", client=client)
        self.assertNotIsInstance(raised.exception, InitialPricingUnavailable)
        self.assertFalse((self.store / "end_of_day/pricing/EMPTYFIRST/source.manifest.json").exists())

    def test_recovery_retains_absent_input_locks(self):
        identities = self.prepare_entitlement()
        pricing, _ = self.rejected_client()
        with patch.object(storage, "finish_publication", side_effect=OSError("simulated stop")):
            result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(result["failed"], 1)
        journal = json.loads((self.root / ".pending.json").read_text())
        self.assertEqual(journal["unavailable_pricing"], ["ENTITLE"])
        self.assertEqual(set(journal["sources"]), {"BUYER", "TARGET"})
        with patch.object(storage, "pricing_locks", wraps=storage.pricing_locks) as locks:
            result = run_lineage(self.store, ["BUYER"], identity_client=identities, pricing_client=pricing)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(set(locks.call_args_list[0].args[1]), {"BUYER", "TARGET", "ENTITLE"})


class RejectionProtocolTests(unittest.TestCase):
    def test_malformed_or_nonmatching_body_is_not_unavailability(self):
        for body in (b"not json", b"[]", b'{"message":"invalid symbol: OTHER"}', b'{"message":"invalid date"}'):
            with self.subTest(body=body):

                def opener(request, **kwargs):
                    raise urllib.error.HTTPError(request.full_url, 400, "bad request", {}, io.BytesIO(body))

                client = FakeClock().client(opener=opener)
                with self.assertRaises(DownloadError) as raised:
                    client.fetch(
                        "LABEL", "raw", timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-15T00:00:00Z")
                    )
                self.assertNotIsInstance(raised.exception, BarsSymbolUnavailable)

    def test_later_page_rejection_is_not_initial_absence_even_after_empty_page(self):
        client = FakeClock().client()
        with patch.object(
            client,
            "_get",
            side_effect=[
                {"bars": {"LABEL": []}, "next_page_token": "next"},
                BarsSymbolUnavailable("LABEL", 400),
            ],
        ):
            with self.assertRaisesRegex(DownloadError, "after a successful pricing page") as raised:
                client.fetch(
                    "LABEL", "raw", timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-15T00:00:00Z")
                )
        self.assertNotIsInstance(raised.exception, BarsSymbolUnavailable)
