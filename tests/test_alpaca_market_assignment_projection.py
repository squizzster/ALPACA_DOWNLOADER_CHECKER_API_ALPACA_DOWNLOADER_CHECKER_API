"""Label correction retains completed assignment and numerical handoffs."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import market_fact
from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar

from alpaca_end_of_day import StandaloneReader, standalone
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_lineage_build.assignment_handoff import produce
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError, document_digest
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.market_assignments import project_group
from libs.alpaca_lineage_build.source_work import assignment_documents
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


class MarketAssignmentProjectionTests(TestCase):
    def candidate(self, case):
        download_pricing(
            case.store,
            "AAA",
            "2026-10-17",
            refresh_all=True,
            client=FakeClient([bar("2026-10-15"), bar("2026-10-16")]),
        )
        self.assertEqual(
            0,
            pipeline.run_lineage(case.store, ["AAA"], identity_client=IdentityClient({"AAA": asset("AAA")}))[
                "failed"
            ],
        )
        self.assertTrue(maintain(case.store)["complete"])
        root = case.store / "end_of_day/lineage"
        index = storage.read_index(root)
        prior = storage.read_receipt(root, "AAA", index)
        documents = assignment_documents(root, prior, {})
        evidence = deepcopy(prior["evidence"])
        # Literal completed label output. Neither a researched assertion nor a
        # projection recipe creates this frozen publication fixture.
        changed = {"AAA": deepcopy(documents)}
        receipt = deepcopy(prior)
        receipt["retained_base_policy"] = prior["correlation_policy"]
        receipt["dated_market_projection_version"] = 1
        pid = prior["identity_memberships"][0]["security_id"]
        receipt["dated_market_projection"] = {
            "version": 1,
            "previous_receipt_generation": index["symbols"]["AAA"]["sha256"],
            "pricing_generation": prior["source_sha256"],
            "previous_boundaries": deepcopy(prior["boundaries"]),
            "selection_unchanged": True,
            "identities": {
                pid: [
                    {
                        "symbol": "OLD",
                        "cusip": "111111111",
                        "start_inclusive": "2016-01-01",
                        "end_exclusive": "2026-10-17",
                        "market": "regular_way",
                    },
                    {
                        "symbol": "AAA",
                        "cusip": "222222222",
                        "start_inclusive": "2026-10-17",
                        "end_exclusive": None,
                        "market": "regular_way",
                    },
                ]
            },
        }
        for kind, document in changed["AAA"].items():
            for entry in document["adjustments"]:
                for segment in entry["segments"]:
                    segment["historical_symbol"] = "OLD"
                    segment["observation_tickers"] = ["OLD"]
                    segment["ticker_ownership_interval"] = {
                        **(segment.get("ticker_ownership_interval") or {}),
                        "historical_symbol": "OLD",
                        "start_date_inclusive": "2016-01-01",
                        "end_date_exclusive": "2026-10-17",
                    }
            generation = document_digest(document)
            receipt["boundaries"][kind] = {**prior["boundaries"][kind], "sha256": generation}
            receipt["assignment_handoffs"][kind] = produce(document, generation)
        receipt["assignment_handoff_kinds"] = sorted(receipt["assignment_handoffs"])
        receipt["evidence"] = evidence
        receipts = {"AAA": receipt}
        validate_handoff(receipts["AAA"])
        return root, index, prior, documents, receipts, changed

    def test_metadata_projection_publishes_and_serves_without_old_prices_or_checks(self):
        with fixture() as case:
            root, index, prior, original, receipts, changed = self.candidate(case)
            self.assertTrue(changed["AAA"])
            self.assertEqual(
                {"OLD"},
                {
                    segment["historical_symbol"]
                    for document in changed["AAA"].values()
                    for entry in document["adjustments"]
                    for segment in entry["segments"]
                },
            )
            for kind, document in changed["AAA"].items():
                for before, after in zip(original[kind]["adjustments"], document["adjustments"]):
                    self.assertEqual(
                        [
                            (
                                s["row_start"],
                                s["row_end"],
                                s.get("observation_selection"),
                                s.get("security_identity"),
                            )
                            for s in before["segments"]
                        ],
                        [
                            (
                                s["row_start"],
                                s["row_end"],
                                s.get("observation_selection"),
                                s.get("security_identity"),
                            )
                            for s in after["segments"]
                        ],
                    )
            with storage.writer_lock(case.store), storage.pricing_locks(case.store, {"AAA"}):
                storage.publish(case.store, root, index, receipts, changed, {}, previous={"AAA": prior})
            with (
                patch.object(
                    standalone, "_rows", side_effect=AssertionError("accepted numerical rows reopened")
                ),
                patch.object(
                    policy, "_bar_reasons", side_effect=AssertionError("accepted numbers checked again")
                ),
            ):
                result = maintain(case.store)
            self.assertTrue(result["complete"], result)
            self.assertEqual(0, result["checked_bars"])
            self.assertEqual(1, result["reused_history_prefixes"])
            reader = StandaloneReader(case.store)
            pid = prior["identity_memberships"][0]["security_id"]
            history = reader.get_histories("AAA", public_id=pid)
            self.assertTrue(history)

    def test_missing_or_changed_projection_binding_fails_at_ingress(self):
        with fixture() as case:
            _root, _index, _prior, _documents, receipts, _changed = self.candidate(case)
            for field, value in (
                ("version", 2),
                ("selection_unchanged", False),
                ("pricing_generation", "f" * 64),
            ):
                bad = deepcopy(receipts["AAA"])
                bad["dated_market_projection"][field] = value
                with self.subTest(field=field), self.assertRaisesRegex(LineageError, "dated-market projection"):
                    validate_handoff(bad)
            bad = deepcopy(receipts["AAA"])
            bad.pop("dated_market_projection")
            with self.assertRaisesRegex(LineageError, "dated-market projection"):
                validate_handoff(bad)

    def test_ordinary_fact_ingress_rejects_before_reopening_accepted_history(self):
        with fixture() as case:
            _root, _index, prior, _documents, _receipts, _changed = self.candidate(case)
            with patch.object(
                pipeline, "accept_context", side_effect=AssertionError("accepted history reopened")
            ):
                with self.assertRaisesRegex(LineageError, "inadmissible"):
                    pipeline.run_lineage(
                        case.store, ["AAA"], event_supplements=[market_fact()], identity_client=IdentityClient()
                    )
            self.assertNotIn("sourced_market_occupancy", prior["evidence"])

    def test_raw_plan_cannot_bypass_group_projection_ingress(self):
        from libs.alpaca_end_of_day_security_correlation.market_occupancy import MarketPlan

        plan = MarketPlan((("retired-fact", "public", ()),), {}, {})
        with self.assertRaisesRegex((ValueError, LineageError), "inadmissible|not supported"):
            project_group(
                {},
                {},
                {},
                {},
                load_authorized_policy(),
                plan=plan,
                date_rows=lambda *_: self.fail("plan opened prices"),
            )
