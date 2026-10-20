import argparse
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path

from cli_support import parser_for

from libs.alpaca_end_of_day_security_correlation import (
    AuthenticatedSecurityMaster,
    audit_security_master_catalogue,
    run_catalog_audit_command,
    security_id_for_alpaca_asset_ids,
)
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES


def parse_catalog_audit_arguments(args):
    return parser_for("legacy", "audit").parse_args(args)


FIRST_ASSET_ID = "11111111-1111-4111-8111-111111111111"
SECOND_ASSET_ID = "22222222-2222-4222-8222-222222222222"


def security_record(
    asset_ids: list[str],
    *,
    source_symbol: str,
    ticker: str,
    name: str,
    cusip: str,
) -> dict:
    master_id = security_id_for_alpaca_asset_ids(asset_ids)
    members = [
        {
            "alpaca_asset_id": asset_id,
            "tickers": [ticker],
            "names": [name],
            "cusips": [cusip],
            "exchanges": ["NASDAQ"],
            "asset_classes": ["us_equity"],
            "statuses": ["active"],
            "figi_identifiers": [],
            "ticker_ownership_periods": [
                {
                    "ticker": ticker,
                    "cusip": cusip,
                    "start_date_inclusive": "2024-01-02",
                    "end_date_exclusive": None,
                    "start_basis": "test",
                    "end_basis": None,
                }
            ],
        }
        for asset_id in asset_ids
    ]
    return {
        "master_id": master_id,
        "master_id_scheme": {
            "kind": "sha256",
            "version": 1,
            "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
        },
        "alpaca_asset_ids": sorted(asset_ids),
        "tickers": [ticker],
        "names": [name],
        "cusips": [cusip],
        "figi_identifiers": [],
        "exchanges": ["NASDAQ"],
        "asset_classes": ["us_equity"],
        "statuses": ["active"],
        "membership_bases": ["test"],
        "first_observed_session_date": "2024-01-02",
        "last_observed_session_date": "2024-01-03",
        "available_adjustments": sorted(ALPACA_ADJUSTMENT_MODES),
        "observed_history_ranges": [
            {
                "source_symbol": source_symbol,
                "ticker": ticker,
                "first_session_date": "2024-01-02",
                "last_session_date": "2024-01-03",
                "adjustments": sorted(ALPACA_ADJUSTMENT_MODES),
            }
        ],
        "identity_events": [],
        "source_symbols": [source_symbol],
        "alpaca_identities": members,
        "online": True,
        "availability_status": "online",
    }


def authenticated_catalogue(securities: list[dict], *, digest: str = "a" * 64) -> AuthenticatedSecurityMaster:
    securities_by_master_id = {security["master_id"]: security for security in securities}
    sources = {
        security["source_symbols"][0]: {
            "symbol": security["source_symbols"][0],
            "identities": [{"master_id": security["master_id"]}],
        }
        for security in securities
    }
    return AuthenticatedSecurityMaster(
        receipt={},
        master={
            "security_master_version": 5,
            "security_master_generation_sha256": digest,
            "created_at": "2026-10-11T00:00:00Z",
        },
        master_sha256=digest,
        sources_by_symbol=sources,
        identities_by_master_id=securities_by_master_id,
    )


class CatalogAuditTests(unittest.TestCase):
    def test_clean_catalogue_passes_every_check(self) -> None:
        authenticated = authenticated_catalogue(
            [
                security_record(
                    [FIRST_ASSET_ID],
                    source_symbol="FIRST",
                    ticker="FIRST",
                    name="First Corp.",
                    cusip="000000001",
                )
            ]
        )

        report = audit_security_master_catalogue(
            Path("/unused"),
            authenticator=lambda _root: authenticated,
        )

        self.assertEqual("passed", report["status"])
        self.assertEqual([], report["findings"])
        self.assertEqual(7, report["summary"]["passed_check_count"])
        self.assertTrue(report["read_only"])

    def test_reports_every_overlapping_uuid_and_current_ticker_claim(self) -> None:
        lineage = security_record(
            [FIRST_ASSET_ID, SECOND_ASSET_ID],
            source_symbol="OLD",
            ticker="CURRENT",
            name="Current Corp.",
            cusip="000000002",
        )
        singleton = security_record(
            [SECOND_ASSET_ID],
            source_symbol="CURRENT",
            ticker="CURRENT",
            name="Current Corp.",
            cusip="000000002",
        )
        authenticated = authenticated_catalogue([lineage, singleton])

        report = audit_security_master_catalogue(
            Path("/unused"),
            authenticator=lambda _root: authenticated,
        )

        findings_by_code = {finding["code"]: finding for finding in report["findings"]}
        overlap = findings_by_code["alpaca_asset_id_multiple_master_ids"]
        ticker_claim = findings_by_code["online_active_ticker_multiple_master_ids"]
        self.assertEqual("failed", report["status"])
        self.assertEqual(SECOND_ASSET_ID, overlap["alpaca_asset_id"])
        self.assertEqual(sorted((lineage["master_id"], singleton["master_id"])), overlap["master_ids"])
        self.assertEqual("CURRENT", ticker_claim["ticker"])
        self.assertEqual("warning", ticker_claim["severity"])

    def test_source_closure_and_projection_checks_accumulate_findings(self) -> None:
        security = security_record(
            [FIRST_ASSET_ID],
            source_symbol="FIRST",
            ticker="FIRST",
            name="First Corp.",
            cusip="000000001",
        )
        security["alpaca_identities"][0]["names"] = ["Unprojected Name"]
        authenticated = authenticated_catalogue([security])
        authenticated.sources_by_symbol["ORPHAN"] = {
            "symbol": "ORPHAN",
            "identities": [{"master_id": "f" * 64}],
        }

        report = audit_security_master_catalogue(
            Path("/unused"),
            authenticator=lambda _root: authenticated,
        )

        codes = {finding["code"] for finding in report["findings"]}
        self.assertIn("source_identity_not_published", codes)
        self.assertIn("member_metadata_missing_from_identity", codes)
        self.assertGreaterEqual(report["summary"]["failed_check_count"], 2)

    def test_changed_generation_during_audit_fails_snapshot_stability(self) -> None:
        security = security_record(
            [FIRST_ASSET_ID],
            source_symbol="FIRST",
            ticker="FIRST",
            name="First Corp.",
            cusip="000000001",
        )
        first = authenticated_catalogue([security], digest="a" * 64)
        second = copy.deepcopy(first)
        object.__setattr__(second, "master_sha256", "b" * 64)
        second.master["security_master_generation_sha256"] = "b" * 64
        authentications = iter((first, second))

        report = audit_security_master_catalogue(
            Path("/unused"),
            authenticator=lambda _root: next(authentications),
        )

        self.assertEqual("failed", report["status"])
        self.assertEqual("catalog_changed_during_audit", report["findings"][-1]["code"])

    def test_command_prints_json_and_accepts_an_alternate_repository_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            arguments = parse_catalog_audit_arguments(["--repository-root", temporary_directory])
            output = io.StringIO()
            expected = {"status": "passed", "read_only": True}

            returned = run_catalog_audit_command(
                arguments,
                output_stream=output,
                auditor=lambda root: expected if root == Path(temporary_directory) else {},
            )

        self.assertEqual(expected, returned)
        self.assertEqual(expected, json.loads(output.getvalue()))

    def test_command_has_no_mutating_options(self) -> None:
        arguments = parse_catalog_audit_arguments([])

        self.assertEqual({"repository_root", "command", "operation"}, set(vars(arguments)))
        self.assertIsInstance(arguments, argparse.Namespace)


if __name__ == "__main__":
    unittest.main()
