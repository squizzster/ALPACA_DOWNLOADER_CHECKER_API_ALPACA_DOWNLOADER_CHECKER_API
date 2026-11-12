"""First ownership comes from dated securities, not from an existing owned row."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import documented_markets as documented
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage import event_supplements


def documented_fact(
    identifier, old, new, episodes, *, kind="public_common_stock", treatment="same_cusip_security"
):
    return {
        "kind": "dated_market_occupancy",
        "match": {"source_id": identifier},
        "facts": {
            "schema_version": 3,
            "ownership_support": "documented_public_security",
            "predecessor": {"symbol": old[0], "cusip": old[1], "instrument_kind": kind},
            "successor": {"symbol": new[0], "cusip": new[1], "instrument_kind": kind},
            "identity_treatment": treatment,
            "registrant_cik": None if kind == "ETF" else "123",
            "episodes": [
                {
                    "symbol": symbol,
                    "cusip": cusip,
                    "start_inclusive": first,
                    "end_exclusive": last,
                    "market": "regular_way",
                    "name": name,
                    "exchange": "NASDAQ",
                }
                for symbol, cusip, first, last, name in episodes
            ],
        },
        "sources": [
            {
                "url": "https://issuer.example/dated-security",
                "title": "Dated listed security",
                "authority": "issuer",
                "document_date": None,
            }
        ],
    }


def fixture():
    facts = event_supplements.accept_supplements(
        [
            documented_fact(
                "former-bank",
                ("REUSE", "111111111"),
                ("REUSE", "111111111"),
                [("REUSE", "111111111", "2024-01-01", "2024-01-05", "Former Bank")],
            ),
            documented_fact(
                "later-fund",
                ("REUSE", "222222222"),
                ("REUSE", "222222222"),
                [("REUSE", "222222222", "2024-01-08", None, "Later ETF")],
                kind="ETF",
            ),
            documented_fact(
                "continuing-bank",
                ("OLD", "333333333"),
                ("CONT", "444444444"),
                [
                    ("OLD", "333333333", "2024-01-01", "2024-01-05", "Original Bank"),
                    ("CONT", "444444444", "2024-01-05", None, "Continuing Bank"),
                ],
                treatment="preexisting_common_remained_outstanding",
            ),
            documented_fact(
                "other-continuing-bank",
                ("BEFORE", "555555555"),
                ("OLD", "555555555"),
                [
                    ("BEFORE", "555555555", "2024-01-01", "2024-01-08", "Different Bank"),
                    ("OLD", "555555555", "2024-01-08", None, "Renamed Different Bank"),
                ],
            ),
        ]
    )
    rows = tuple(
        (day + "T05:00:00Z", "10", "11", "9", "10", "100", "5", "10")
        for day in ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09")
    )
    identities = {
        name: security_identity_for_alpaca_assets(
            [name], [], tickers=labels, basis="accepted_fixture"
        ).as_report_object()
        for name, labels in (("continuing", ["OLD", "CONT"]), ("frozen", ["FROZEN"]))
    }

    def segment(view, first, last, identity=None):
        value = {
            "adjustment": view,
            "row_start": first,
            "row_end": last,
            "row_count": last - first + 1,
            "from": rows[first - 1][0][:10],
            "to": rows[last - 1][0][:10],
            "first_timestamp": rows[first - 1][0],
            "last_timestamp": rows[last - 1][0],
        }
        if identity is None:
            value.update(reason_codes=["unresolved_uuid_candidates"], candidate_securities=[])
        else:
            value.update(
                security_identity=identity,
                canonical_security={
                    "security_id": identity["security_id"],
                    "alpaca_asset_id": identity["member_alpaca_asset_ids"][0],
                    "cusip": "444444444",
                },
                historical_symbol="CONT",
                observation_selection="superseded",
                observation_tickers=["CONT"],
            )
        return value

    reports, inputs, evidence = {}, {}, {}
    exact = [
        {
            "lookup_kind": "cusip",
            "lookup": cu,
            "outcome": "found" if uid else "not_found",
            "http_status": 200 if uid else 404,
            "asset": {"id": uid} if uid else None,
        }
        for cu, uid in (
            ("111111111", "old-bank"),
            ("222222222", "new-fund"),
            ("333333333", None),
            ("444444444", "continuing"),
            ("555555555", "replacement"),
        )
    ]
    for name, route in (
        ("REUSE", "new-fund"),
        ("CONT", "continuing"),
        ("OLD", "replacement"),
        ("FROZEN", "frozen"),
    ):
        positive = (
            [
                segment(
                    view, 4 if name == "CONT" else 1, 6, identities["continuing" if name == "CONT" else "frozen"]
                )
                for view in ADJUSTMENTS
            ]
            if name in {"CONT", "FROZEN"}
            else []
        )
        negative = (
            [] if name == "FROZEN" else [segment(view, 1, 3 if name == "CONT" else 6) for view in ADJUSTMENTS]
        )
        reports[name] = {
            "source_row_count": 48,
            "correlated": {"row_count": sum(s["row_count"] for s in positive), "segments": positive},
            "uncorrelated": {"row_count": sum(s["row_count"] for s in negative), "segments": negative},
        }
        inputs[name] = SimpleNamespace(views=None if name == "FROZEN" else dict.fromkeys(ADJUSTMENTS, rows))
        evidence[name] = {
            "query": {"input_symbol": name},
            "sourced_market_occupancy": {"schema_version": 1, "facts": facts},
            "asset_lookups": [
                *exact,
                {
                    "lookup_kind": "symbol",
                    "lookup": name,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": {"id": route},
                },
            ],
        }
    accepted = {"CONT": {"continuing": identities["continuing"]}, "FROZEN": {"frozen": identities["frozen"]}}
    return inputs, reports, evidence, accepted


class DocumentedMarketTests(TestCase):
    def test_documented_owner_consumes_one_completed_containing_component(self):
        inputs, reports, evidence, accepted = fixture()
        completed = security_identity_for_alpaca_assets(
            ["continuing", "later-observed"], [], tickers=["OLD", "CONT"], basis="accepted_later_component"
        ).as_report_object()
        accepted["later-scope"] = {identifier: completed for identifier in completed["member_alpaca_asset_ids"]}
        with self.assertRaisesRegex(SecurityCorrelationError, "source-scoped public membership"):
            documented.bind(evidence, accepted_memberships=accepted)
        plan = documented.bind(evidence, accepted_memberships=accepted, allow_containing_membership=True)
        owner = next(item for item in plan["owners"] if item["source_fact_id"] == "continuing-bank")
        self.assertIs(completed, owner["identity"])
        self.assertEqual("completed_containing_component", owner["membership_resolution"])
        with patch.object(
            documented, "bind", side_effect=AssertionError("completed containing binding repeated")
        ):
            result = documented.project(inputs, reports, evidence, plan=plan)
        restored = next(item for item in result["CONT"]["correlated"]["segments"] if item["row_start"] == 1)
        self.assertEqual(completed["security_id"], restored["security_identity"]["security_id"])
        self.assertEqual("OLD", restored["historical_symbol"])

    def test_incomparable_accepted_components_cannot_be_unioned_by_a_shared_uuid(self):
        _, _, evidence, accepted = fixture()
        for name in ("one", "two"):
            component = security_identity_for_alpaca_assets(
                ["continuing", name], [], tickers=["OLD", "CONT"], basis="accepted_separate_scope"
            ).as_report_object()
            accepted[name] = dict.fromkeys(component["member_alpaca_asset_ids"], component)
        with self.assertRaisesRegex(SecurityCorrelationError, "source-scoped public membership"):
            documented.bind(evidence, accepted_memberships=accepted, allow_containing_membership=True)

    def test_held_report_rebind_changes_only_the_public_component_references(self):
        from libs.alpaca_lineage_build.source_work import rebind_retained_report

        _, reports, _, accepted = fixture()
        old = accepted["CONT"]["continuing"]
        new = security_identity_for_alpaca_assets(
            ["continuing", "later-observed"], [], tickers=old["member_tickers"], basis="accepted_later_component"
        ).as_report_object()
        report = reports["CONT"]
        prior = {"identity_memberships": [old]}
        with patch.object(documented, "bind", side_effect=AssertionError("accepted membership re-proved")):
            result = rebind_retained_report(report, prior, {"continuing": new})
        self.assertIs(report["uncorrelated"], result["uncorrelated"])
        for before, after in zip(report["correlated"]["segments"], result["correlated"]["segments"]):
            self.assertEqual(new, after["security_identity"])
            self.assertEqual(new["security_id"], after["canonical_security"]["security_id"])
            for field in (
                "row_start",
                "row_end",
                "historical_symbol",
                "observation_selection",
                "observation_tickers",
            ):
                self.assertEqual(before[field], after[field])
        self.assertIs(report, rebind_retained_report(report, prior, {"continuing": old}))

    def test_first_owners_and_reuse_gap_are_separate_from_later_description(self):
        inputs, reports, evidence, accepted = fixture()
        plan = documented.bind(evidence, accepted_memberships=accepted)
        with patch.object(documented, "bind", side_effect=AssertionError("accepted identity binding replayed")):
            result = documented.project(inputs, reports, evidence, plan=plan)
        self.assertEqual(40, result["REUSE"]["correlated"]["row_count"])
        self.assertEqual(8, result["REUSE"]["uncorrelated"]["row_count"])
        raw = [s for s in result["REUSE"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual([(1, 3), (5, 6)], [(s["row_start"], s["row_end"]) for s in raw])
        self.assertEqual(["old-bank", "new-fund"], [s["canonical_security"]["alpaca_asset_id"] for s in raw])
        self.assertEqual(
            ["Former Bank", "Later ETF"],
            [s["ticker_ownership_interval"]["security_description"]["name"] for s in raw],
        )
        self.assertTrue(all("observation_selection" not in s for s in result["REUSE"]["correlated"]["segments"]))
        self.assertEqual(reports["FROZEN"]["correlated"], result["FROZEN"]["correlated"])

    def test_missing_dated_uuid_keeps_known_public_owner_and_old_supersessions(self):
        inputs, reports, evidence, accepted = fixture()
        result = documented.project(
            inputs, reports, evidence, plan=documented.bind(evidence, accepted_memberships=accepted)
        )
        held = reports["CONT"]["correlated"]["segments"]
        self.assertTrue(all(result["CONT"]["correlated"]["segments"][i] is s for i, s in enumerate(held)))
        new = [s for s in result["CONT"]["correlated"]["segments"] if s["row_start"] == 1]
        self.assertEqual(8, len(new))
        self.assertTrue(all(s["canonical_security"]["alpaca_asset_id"] is None for s in new))
        self.assertTrue(all(s["security_identity"] is accepted["CONT"]["continuing"] for s in new))
        self.assertTrue(all(s["observation_selection"] == "superseded" for s in held))

    def test_source_continuation_beats_an_unrelated_historical_ticker(self):
        inputs, reports, evidence, accepted = fixture()
        result = documented.project(
            inputs, reports, evidence, plan=documented.bind(evidence, accepted_memberships=accepted)
        )
        raw = [s for s in result["OLD"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual(["BEFORE", "OLD"], [s["historical_symbol"] for s in raw])
        self.assertEqual({"replacement"}, {s["canonical_security"]["alpaca_asset_id"] for s in raw})

    def test_unknown_or_conflicting_exact_mapping_requires_metadata_recovery(self):
        _, _, evidence, accepted = fixture()
        missing = deepcopy(evidence)
        for report in missing.values():
            report["asset_lookups"] = [x for x in report["asset_lookups"] if x["lookup"] != "333333333"]
        with self.assertRaisesRegex(SecurityCorrelationError, "exact metadata recovery"):
            documented.bind(missing, accepted_memberships=accepted)
        bad = deepcopy(evidence)
        bad["CONT"]["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "444444444",
                "outcome": "found",
                "http_status": 200,
                "asset": {"id": "rival"},
            }
        )
        with self.assertRaisesRegex(SecurityCorrelationError, "conflicting completed"):
            documented.bind(bad, accepted_memberships=accepted)

    def test_tied_former_owners_remain_explicit_instead_of_using_uuid_order(self):
        inputs, reports, evidence, accepted = fixture()
        rival = event_supplements.accept_supplements(
            [
                documented_fact(
                    "another-former-bank",
                    ("REUSE", "666666666"),
                    ("REUSE", "666666666"),
                    [("REUSE", "666666666", "2024-01-01", "2024-01-05", "Another Bank")],
                )
            ]
        )[0]
        evidence["REUSE"]["sourced_market_occupancy"]["facts"] = [
            *evidence["REUSE"]["sourced_market_occupancy"]["facts"],
            rival,
        ]
        evidence["REUSE"]["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "666666666",
                "outcome": "found",
                "http_status": 200,
                "asset": {"id": "other-old-bank"},
            }
        )
        result = documented.project(
            inputs, reports, evidence, plan=documented.bind(evidence, accepted_memberships=accepted)
        )
        first = next(
            s
            for s in result["REUSE"]["uncorrelated"]["segments"]
            if s["adjustment"] == "raw" and s["row_start"] == 1
        )
        self.assertEqual(
            {"old-bank", "other-old-bank"}, {s["alpaca_asset_id"] for s in first["candidate_securities"]}
        )
        self.assertIn("competing_documented_public_owners", first["reason_codes"])

    def test_schema_rejects_unlike_instruments_and_unsupported_share_treatment(self):
        value = documented_fact(
            "bad",
            ("ONE", "111111111"),
            ("TWO", "222222222"),
            [
                ("ONE", "111111111", "2024-01-01", "2024-01-05", "One"),
                ("TWO", "222222222", "2024-01-05", None, "Two"),
            ],
        )
        with self.assertRaisesRegex(ValueError, "continuing security treatment"):
            event_supplements.accept_supplements([value])
        value["facts"]["identity_treatment"] = "preexisting_common_remained_outstanding"
        value["facts"]["successor"]["instrument_kind"] = "ETF"
        with self.assertRaisesRegex(ValueError, "unlike instruments"):
            event_supplements.accept_supplements([value])
