"""Label correction retains completed assignment and numerical handoffs."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import market_fact
from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar

from alpaca_end_of_day import StandaloneReader, standalone
from libs.alpaca_lineage_build import pipeline, source_work, storage
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.market_assignments import project_group
from libs.alpaca_lineage_build.source_work import assignment_documents
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.event_supplements import accept_supplements
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
        fact = market_fact()
        fact["facts"]["predecessor"]["symbol"] = "OLD"
        fact["facts"]["successor"]["symbol"] = "AAA"
        fact["facts"]["episodes"] = [
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
        fact = accept_supplements([fact])[0]
        evidence["sourced_market_occupancy"] = {"version": 1, "facts": [fact]}
        evidence["asset_lookups"].append(
            {
                **evidence["asset_lookups"][0],
                "lookup_kind": "cusip",
                "lookup": "222222222",
                "asset": asset("AAA"),
            }
        )
        evidence["asset_lookups"].append({**evidence["asset_lookups"][-1], "lookup": "111111111"})
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("historical correlation replayed")),
            patch.object(
                pipeline, "select_observations", side_effect=AssertionError("numerical choice replayed")
            ),
        ):
            receipts, changed = project_group(
                {"AAA": prior},
                {"AAA": evidence},
                {"AAA": index["symbols"]["AAA"]["sha256"]},
                {"AAA": documents},
                load_authorized_policy(),
                date_rows=lambda *_args: self.fail("whole accepted segment opened prices"),
            )
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

    def test_ordinary_fact_ingress_selects_metadata_projection_without_replaying_history(self):
        with fixture() as case:
            root, index, prior, _original, receipts, _changed = self.candidate(case)
            fact = receipts["AAA"]["evidence"]["sourced_market_occupancy"]["facts"][0]
            # The lookup is an accepted pre-existing observation; installing a
            # dated fact does not require observing that endpoint again.
            seeded = deepcopy(prior)
            seeded["evidence"]["asset_lookups"] = receipts["AAA"]["evidence"]["asset_lookups"]
            with storage.writer_lock(case.store), storage.pricing_locks(case.store, {"AAA"}):
                storage.publish(
                    case.store, root, index, {"AAA": seeded}, {"AAA": {}}, {}, previous={"AAA": prior}
                )
            plans = []
            prepare = source_work.prepare_group_evidence_and_source_work

            def capture(*args, **kwargs):
                plan = prepare(*args, **kwargs)
                plans.append(plan)
                return plan

            with (
                patch.object(pipeline, "prepare_group_evidence_and_source_work", side_effect=capture),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("historical prices loaded")),
                patch.object(
                    pipeline, "correlate", side_effect=AssertionError("historical correlation repeated")
                ),
                patch.object(
                    source_work, "correlate", create=True, side_effect=AssertionError("old rules replayed")
                ),
                patch.object(
                    source_work.public_common_continuation,
                    "certify_group",
                    side_effect=AssertionError("accepted membership repeated"),
                ),
                patch.object(
                    source_work.security_relations,
                    "certify_group",
                    side_effect=AssertionError("accepted relations repeated"),
                ),
            ):
                result = pipeline.run_lineage(
                    case.store,
                    ["AAA"],
                    event_supplements=[fact],
                    identity_client=IdentityClient({"AAA": asset("AAA"), "222222222": asset("AAA")}),
                )
            self.assertEqual(0, result["failed"], result)
            self.assertTrue(plans)
            self.assertTrue(plans[-1].market_projection, plans[-1])
            self.assertEqual({"AAA": "M"}, plans[-1].dispositions)
