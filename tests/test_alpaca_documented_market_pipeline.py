"""First-owner recovery keeps accepted peers and later appends closed."""

import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_recapitalization_pipeline as recap_fixture
from test_alpaca_documented_markets import documented_fact
from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import documented_markets, row_correlation
from libs.alpaca_lineage_build import observations, pipeline, storage, transform
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy, work
from libs.alpaca_serving_catalogue.producer import maintain


class DocumentedMarketPipelineTests(TestCase):
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
        self.client = IdentityClient(
            {
                "FILL": {**asset("FILL", "fund"), "name": "Later ETF"},
                "PEER": asset("PEER", "peer"),
                "111111111": {**asset("FILL", "bank"), "name": "Wrong Current ETF"},
                "222222222": {**asset("FILL", "fund"), "name": "Later ETF"},
                "333333333": asset("PEER", "peer"),
            },
            [
                {
                    "id": "distinct-target",
                    "action_type": "stock_merger",
                    "acquiree_symbol": "FILL",
                    "acquirer_symbol": "PEER",
                    "acquiree_cusip": "111111111",
                    "acquirer_cusip": "333333333",
                    "acquiree_rate": 1,
                    "acquirer_rate": 0.42,
                    "effective_date": "2024-01-05",
                    "process_date": "2024-01-05",
                }
            ],
        )
        for name in ("FILL", "PEER"):
            download_pricing(
                self.store,
                name,
                "2024-01-10",
                from_date="2024-01-01",
                client=FakeClient(informative_bars("2024-01-02", "2024-01-10")),
            )
        original = pipeline.correlate

        def unresolved_fixture(*args, **kwargs):
            reports = original(*args, **kwargs)
            if "FILL" in reports:
                report = reports["FILL"]
                # Model the earlier completed all-unresolved publication. Its
                # ordinary producer still constructs every assignment handoff.
                segments = report["correlated"]["segments"] + report["uncorrelated"]["segments"]
                report["uncorrelated"] = {
                    "row_count": report["source_row_count"],
                    "segments": [
                        {
                            **{
                                key: item[key]
                                for key in (
                                    "adjustment",
                                    "row_start",
                                    "row_end",
                                    "row_count",
                                    "first_timestamp",
                                    "last_timestamp",
                                )
                            },
                            "reason_codes": ["unresolved_uuid_candidates"],
                            "candidate_securities": [],
                        }
                        for item in segments
                    ],
                }
                report["correlated"] = {"row_count": 0, "segments": []}
            return reports

        with patch.object(pipeline, "correlate", side_effect=unresolved_fixture):
            built = pipeline.run_lineage(self.store, ["FILL"], identity_client=self.client)
        self.assertEqual(0, built["failed"], built)
        index = storage.read_index(self.root)
        self.prior = {name: storage.read_receipt(self.root, name, index) for name in ("FILL", "PEER")}
        self.assertEqual(48, self.prior["FILL"]["unresolved_rows"])
        self.assertEqual(0, maintain(self.store, bootstrap=True)["failed"])
        self.facts = [
            documented_fact(
                "earlier-bank",
                ("FILL", "111111111"),
                ("FILL", "111111111"),
                [("FILL", "111111111", "2024-01-01", "2024-01-05", "Earlier Bank")],
            ),
            documented_fact(
                "later-fund",
                ("FILL", "222222222"),
                ("FILL", "222222222"),
                [("FILL", "222222222", "2024-01-08", None, "Later ETF")],
                kind="ETF",
            ),
        ]

    def repair(self):
        loader = pipeline.load_pricing

        def selected(*args, **kwargs):
            self.assertEqual("FILL", args[1], "accepted peer pricing opened")
            return loader(*args, **kwargs)

        with (
            patch.object(pipeline, "load_pricing", side_effect=selected),
            patch.object(transform, "correlate", side_effect=AssertionError("old row cascade repeated")),
            patch.object(row_correlation, "_decide_row", side_effect=AssertionError("old owner repeated")),
        ):
            result = pipeline.run_lineage(
                self.store, ["FILL"], identity_client=self.client, event_supplements=self.facts
            )
        self.assertEqual(0, result["failed"], result)
        return result

    def test_first_owners_publish_separately_and_current_description_cannot_erase_bank(self):
        self.repair()
        index = storage.read_index(self.root)
        current = storage.read_receipt(self.root, "FILL", index)
        self.assertEqual(8, current["unresolved_rows"])
        self.assertEqual(2, len(current["identity_memberships"]))
        self.assertEqual(
            self.prior["PEER"]["boundaries"], storage.read_receipt(self.root, "PEER", index)["boundaries"]
        )
        check = policy._bar_reasons

        def newly_owned_only(bar, adjustment):
            self.assertIn(bar["session"], {"2024-01-02", "2024-01-03", "2024-01-04", "2024-01-08", "2024-01-09"})
            return check(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=newly_owned_only):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(40, served["checked_bars"])
        bank = next(
            item for item in DataLibrary(store=self.store).list_securities() if item.tickers == ("FILL",)
        )
        self.assertEqual(("Earlier Bank",), bank.names)

    def test_append_consumes_completed_binding_and_choices(self):
        self.repair()
        self.assertTrue(maintain(self.store)["complete"])
        for name in ("FILL", "PEER"):
            download_pricing(
                self.store,
                name,
                "2024-01-11",
                client=FakeClient(informative_bars("2024-01-02", "2024-01-11")),
            )
        decide = row_correlation._decide_row
        select = observations._select_new_observations

        def fresh_owner(context, **kwargs):
            self.assertGreaterEqual(context.session_date.isoformat(), "2024-01-10")
            return decide(context, **kwargs)

        def fresh_choice(inputs, reports, **kwargs):
            self.assertTrue(
                all(
                    segment["first_timestamp"][:10] >= "2024-01-10"
                    for report in reports.values()
                    for segment in report["correlated"]["segments"]
                )
            )
            return select(inputs, reports, **kwargs)

        with (
            patch.object(documented_markets, "bind", side_effect=AssertionError("completed binding repeated")),
            patch.object(row_correlation, "_decide_row", side_effect=fresh_owner),
            patch.object(observations, "_select_new_observations", side_effect=fresh_choice),
        ):
            result = pipeline.run_lineage(self.store, ["PEER"], identity_client=self.client)
        self.assertEqual(0, result["failed"], result)

    def test_declared_binding_missing_or_unknown_requires_owning_recovery(self):
        self.repair()
        receipt = storage.read_receipt(self.root, "FILL", storage.read_index(self.root))
        for value in (None, {"version": 2, "owners": [], "source_routes": {}}):
            changed = {**receipt, "documented_identity_membership": value}
            with self.assertRaisesRegex(LineageError, "documented identity"):
                validate_handoff(changed)

    def test_changed_exact_owner_mapping_cannot_borrow_the_closed_scope(self):
        from libs.alpaca_lineage_build import documented_work, pricing, source_work
        from libs.alpaca_security_lineage.event_supplements import accept_supplements

        self.repair()
        index = storage.read_index(self.root)
        prior = {name: storage.read_receipt(self.root, name, index) for name in ("FILL", "PEER")}
        evidence = {name: deepcopy(receipt["evidence"]) for name, receipt in prior.items()}
        added = deepcopy(self.facts[0])
        added["match"]["source_id"] = "additional-primary-support"
        extension = evidence["FILL"]["sourced_market_occupancy"]
        extension["facts"] = [*extension["facts"], *accept_supplements([added])]
        for lookup in evidence["FILL"]["asset_lookups"]:
            if lookup["lookup_kind"] == "cusip" and lookup["lookup"] == "111111111":
                lookup["asset"] = {**lookup["asset"], "id": "actually-changed-owner"}
        controls = {}
        for name, receipt in prior.items():
            source = pricing.load_source(self.store / "end_of_day/pricing" / name, name)
            controls[name] = pricing.metadata_source_control(
                self.store, name, source, receipt["source_sha256"], receipt, accepted_reuse=(True, True)
            )
        with patch.object(documented_markets, "bind", side_effect=AssertionError("invalid old owner carried")):
            plan = documented_work.prepare(
                controls,
                evidence,
                {},
                {},
                lambda name: source_work._accepted_owners_apply(prior[name], evidence[name]),
            )
        self.assertIsNone(plan)

    def test_old_policy_cannot_execute_new_first_owner_semantics(self):
        from libs.alpaca_security_lineage.event_supplements import accept_supplements

        facts = accept_supplements(self.facts)
        old = load_policy("resources/alpaca_security_correlation_v86_legacy_policy.yaml")
        with patch.object(transform, "evaluate_policy", side_effect=AssertionError("old policy executed")):
            with self.assertRaisesRegex(LineageError, "policy 87"):
                transform.correlate(
                    self.store,
                    {},
                    {
                        "FILL": {
                            "sourced_market_occupancy": {
                                "schema_version": 1,
                                "facts": facts,
                            }
                        }
                    },
                    policy=old,
                )

    def test_publication_acknowledgement_interruption_keeps_completed_first_owners(self):
        self.repair()
        original = work.acknowledge

        def interrupt(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("after completed serving acknowledgement")

        with patch.object(work, "acknowledge", side_effect=interrupt):
            failed = maintain(self.store)
        self.assertFalse(failed["complete"])
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted bars checked again")):
            self.assertTrue(maintain(self.store)["complete"])

    def test_unknown_dated_description_fails_without_replaying_primary_sources(self):
        self.repair()
        group = type("Group", (), {})()
        group.assignments = {
            "raw": [
                (
                    None,
                    None,
                    {
                        "branch": "correlated",
                        "security_identity": {"security_id": "owner", "member_alpaca_asset_ids": ["bank"]},
                        "from": "2024-01-02",
                        "to": "2024-01-04",
                        "ticker_ownership_interval": {"security_description": {"version": 2}},
                    },
                )
            ]
        }
        group.evidence = {"sources": {}}
        from libs.alpaca_serving_catalogue.contract import CatalogueError

        with self.assertRaisesRegex(CatalogueError, "dated security description"):
            policy._metadata(group, "owner")

    def test_same_public_owner_prepend_and_append_preserve_the_accepted_middle(self):
        for append in (False, True):
            with self.subTest(append=append):
                case = recap_fixture.RecapitalizationPipelineTests()
                case.setUp()
                try:
                    case._establish_accepted_membership()
                    first = min(item["from"] for item in case.prior["identity_intervals"])
                    last = max(item["to"] for item in case.prior["identity_intervals"])
                    if append:
                        download_pricing(
                            case.store,
                            "SAME",
                            "2024-01-13",
                            client=FakeClient(informative_bars("2024-01-02", "2024-01-13")),
                        )
                    fact = documented_fact(
                        "documented-unchanged-public-common",
                        ("SAME", "000000000"),
                        ("SAME", "222222222"),
                        [
                            ("SAME", "000000000", "2024-01-01", "2024-01-03", "Original Company"),
                            ("SAME", "111111111", "2024-01-03", "2024-01-05", "Continuing Company"),
                            ("SAME", "222222222", "2024-01-05", None, "Continuing Company"),
                        ],
                        treatment="preexisting_common_remained_outstanding",
                    )
                    select = observations._select_new_observations

                    def new_choices(inputs, reports, **kwargs):
                        for report in reports.values():
                            for segment in report["correlated"]["segments"]:
                                self.assertTrue(
                                    segment["last_timestamp"][:10] < first
                                    or segment["first_timestamp"][:10] > last
                                )
                        return select(inputs, reports, **kwargs)

                    with (
                        patch.object(
                            transform, "correlate", side_effect=AssertionError("completed chronology replayed")
                        ),
                        patch.object(
                            row_correlation,
                            "_decide_row",
                            side_effect=AssertionError("accepted row rule repeated"),
                        ),
                        patch.object(observations, "_select_new_observations", side_effect=new_choices),
                    ):
                        repaired = pipeline.run_lineage(
                            case.store, ["SAME"], identity_client=case.client, event_supplements=[fact]
                        )
                    self.assertEqual(0, repaired["failed"], repaired)
                    check = policy._bar_reasons

                    def new_bars(bar, adjustment):
                        self.assertTrue(bar["session"] < first or bar["session"] > last)
                        return check(bar, adjustment)

                    with patch.object(policy, "_bar_reasons", side_effect=new_bars):
                        served = maintain(case.store)
                    self.assertTrue(served["complete"], served)
                    self.assertEqual(
                        case.prior["unresolved_rows"] + (8 if append else 0), served["checked_bars"]
                    )
                finally:
                    case.doCleanups()
