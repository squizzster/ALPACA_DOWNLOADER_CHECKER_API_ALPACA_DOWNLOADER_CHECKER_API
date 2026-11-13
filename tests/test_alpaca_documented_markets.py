"""Authored market plans reject; completed generic components remain closed."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import documented_markets as documented
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build.source_work_contract import compose_memberships
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
    facts = list(
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
    def test_generic_membership_consumes_a_completed_containing_component(self):
        narrow = security_identity_for_alpaca_assets(
            ["continuing"], [], tickers=["OLD", "CONT"], basis="accepted"
        ).as_report_object()
        whole = security_identity_for_alpaca_assets(
            ["continuing", "later"], [], tickers=["OLD", "CONT"], basis="accepted"
        ).as_report_object()
        previous = {"continuing": narrow, "later": whole}
        edge = SimpleNamespace(
            predecessor_asset_id="continuing",
            successor_asset_id="later",
            predecessor_symbol="OLD",
            successor_symbol="CONT",
        )
        with patch(
            "libs.alpaca_lineage_build.source_work_contract.security_identity_for_alpaca_assets",
            side_effect=AssertionError("completed component recalculated"),
        ):
            result = compose_memberships(previous, [edge])
        self.assertTrue(all(value is whole for value in result.values()))

    def test_shared_uuid_does_not_invent_a_join_without_a_completed_edge(self):
        one = security_identity_for_alpaca_assets(
            ["shared", "one"], [], tickers=["OLD"], basis="accepted"
        ).as_report_object()
        two = security_identity_for_alpaca_assets(
            ["shared", "two"], [], tickers=["NEW"], basis="accepted"
        ).as_report_object()
        previous = {"one": one, "two": two}
        with patch(
            "libs.alpaca_lineage_build.source_work_contract.security_identity_for_alpaca_assets",
            side_effect=AssertionError("unsupported join calculated"),
        ):
            self.assertEqual(previous, compose_memberships(previous, []))

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

    def test_well_formed_documented_owner_rejects_before_storage(self):
        _, _, evidence, accepted = fixture()
        with patch.object(Path, "open", side_effect=AssertionError("metadata opened")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                documented.bind(evidence, accepted_memberships=accepted, allow_containing_membership=True)

    def test_rejected_projection_keeps_completed_positive_and_ownerless_rows(self):
        inputs, reports, evidence, _ = fixture()
        original = deepcopy(reports)
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            documented.project(inputs, reports, evidence)
        self.assertEqual(original, reports)

    def test_raw_plan_cannot_bypass_assertion_ingress(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            documented.project(None, None, {}, plan={"owners": [{"identity": "authored"}]})

    def test_missing_or_conflicting_endpoint_cannot_authorize_research(self):
        for missing in (True, False):
            _, _, evidence, accepted = fixture()
            if missing:
                evidence["CONT"]["asset_lookups"] = []
            else:
                evidence["CONT"]["asset_lookups"].append(
                    {"lookup_kind": "cusip", "lookup": "444444444", "asset": {"id": "rival"}}
                )
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, "inadmissible"):
                documented.bind(evidence, accepted_memberships=accepted)

    def test_no_research_request_preserves_closed_labels_and_ownerless_rows(self):
        inputs, reports, evidence, _ = fixture()
        for value in evidence.values():
            value.pop("sourced_market_occupancy")
        with patch.object(Path, "open", side_effect=AssertionError("accepted prices reopened")):
            self.assertIs(reports, documented.project(inputs, reports, evidence))
        self.assertEqual(48, reports["REUSE"]["uncorrelated"]["row_count"])
        self.assertTrue(
            all(s["observation_selection"] == "superseded" for s in reports["CONT"]["correlated"]["segments"])
        )

    def test_all_authored_instrument_and_treatment_variants_reject(self):
        for kind, treatment in (
            ("public_common_stock", "same_cusip_security"),
            ("ETF", "same_cusip_security"),
            ("warrant", "preexisting_common_remained_outstanding"),
        ):
            fact = documented_fact(
                "asserted",
                ("OLD", "111111111"),
                ("NEW", "222222222"),
                [("OLD", "111111111", "2024-01-01", "2024-01-05", "Old")],
                kind=kind,
                treatment=treatment,
            )
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "inadmissible"):
                event_supplements.accept_supplements([fact])
