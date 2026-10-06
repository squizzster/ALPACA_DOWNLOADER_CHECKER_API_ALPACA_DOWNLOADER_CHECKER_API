import argparse
import io
import json
import os
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from libs.alpaca_security_lineage import (
    AlpacaIdentityClient,
    build_identity_report,
    inspect_alpaca_security_lineage,
    inspect_local_downloads,
    run_security_lineage_command,
)

META_ASSET = {
    "id": "meta-id",
    "class": "us_equity",
    "exchange": "NASDAQ",
    "symbol": "META",
    "name": "Meta Platforms, Inc.",
    "status": "active",
    "tradable": True,
}
FB_ETF_ASSET = {
    "id": "fb-etf-id",
    "class": "us_equity",
    "exchange": "BATS",
    "symbol": "FB",
    "name": "ProShares S&P 500 Dynamic Buffer ETF",
    "status": "active",
    "tradable": True,
}
METV_ASSET = {
    "id": "metv-id",
    "class": "us_equity",
    "exchange": "BATS",
    "symbol": "METV",
    "name": "Roundhill Metaverse ETF",
    "status": "active",
    "tradable": True,
}


class FakeIdentityClient:
    def __init__(self, api_key_id: str = "unused", api_secret_key: str = "unused") -> None:
        self.credentials = (api_key_id, api_secret_key)
        self.request_count = 0
        facebook_name_change = {
            "action_type": "name_change",
            "id": "facebook-name-change",
            "process_date": "2022-06-09",
            "old_symbol": "FB",
            "old_cusip": "30303M102",
            "new_symbol": "META",
            "new_cusip": "30303M102",
        }
        metaverse_name_change = {
            "action_type": "name_change",
            "id": "metaverse-name-change",
            "process_date": "2022-01-31",
            "old_symbol": "META",
            "old_cusip": "53656F417",
            "new_symbol": "METV",
            "new_cusip": "53656F417",
        }
        fb_dividend = {
            "action_type": "cash_dividend",
            "id": "fb-dividend",
            "process_date": "2025-09-30",
            "symbol": "FB",
            "cusip": "74349Y613",
        }
        self.actions = {
            "FB": [facebook_name_change, fb_dividend],
            "META": [facebook_name_change, metaverse_name_change],
            "METV": [metaverse_name_change],
        }
        self.assets = {
            "FB": FB_ETF_ASSET,
            "META": META_ASSET,
            "METV": METV_ASSET,
            "30303M102": META_ASSET,
            "53656F417": METV_ASSET,
            "74349Y613": FB_ETF_ASSET,
        }

    def get_corporate_actions(self, symbol: str, start_date: str, end_date: str) -> list[dict]:
        self.request_count += 1
        self.last_date_range = (start_date, end_date)
        return self.actions[symbol]

    def get_asset(self, identifier: str) -> dict | None:
        self.request_count += 1
        return self.assets.get(identifier)


class FakeRateLimiter:
    def __init__(self) -> None:
        self.wait_count = 0
        self.deferred_seconds = []

    def wait_until_request_allowed(self) -> None:
        self.wait_count += 1

    def defer_requests_for_seconds(self, delay_seconds: float) -> None:
        self.deferred_seconds.append(delay_seconds)


class FakeHttpResponse(io.BytesIO):
    def __init__(self, payload: bytes, headers: dict[str, str] | None = None) -> None:
        super().__init__(payload)
        self.headers = headers or {}


class IdentityReportTests(unittest.TestCase):
    def test_follows_name_changes_and_resolves_reused_ticker(self) -> None:
        local_report = {
            "present": True,
            "downloads": [
                {
                    "request_from": "2016-01-01T00:00:00Z",
                    "asof": "2026-10-06",
                    "stat_matches_sidecar": True,
                }
            ],
        }
        client = FakeIdentityClient()

        report = build_identity_report("FB", local_report, client)

        self.assertEqual(["FB", "META", "METV"], report["queried_symbols"])
        self.assertEqual(9, report["api_request_count"])
        self.assertEqual(3, len(report["identities"]))
        self.assertEqual(
            ["30303M102", "74349Y613"],
            [occupant["cusip"] for occupant in report["input_symbol_occupants"]],
        )
        self.assertEqual(
            ["meta-id", "fb-etf-id"],
            [occupant["asset_id"] for occupant in report["input_symbol_occupants"]],
        )
        self.assertEqual(("2016-01-01", "2026-10-06"), client.last_date_range)
        self.assertEqual([], report["unresolved_cusips"])

    def test_public_api_returns_report_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            paths_before = list(repository_root.rglob("*"))

            report = inspect_alpaca_security_lineage(
                "fb",
                "key",
                "secret",
                repository_root=repository_root,
                client_factory=FakeIdentityClient,
                generated_at=datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc),
            )

            paths_after = list(repository_root.rglob("*"))

        self.assertEqual(paths_before, paths_after)
        self.assertEqual(1, report["report_version"])
        self.assertEqual("2026-10-06T12:00:00Z", report["generated_at"])
        self.assertEqual("FB", report["symbol"])
        self.assertFalse(report["local"]["present"])

    def test_command_writes_one_json_document_to_selected_stream(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_stream = io.StringIO()
            returned_report = run_security_lineage_command(
                argparse.Namespace(ticker="fb"),
                repository_root=Path(temporary_directory),
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                output_stream=output_stream,
                client_factory=FakeIdentityClient,
            )

        serialized_report = output_stream.getvalue()
        self.assertTrue(serialized_report.endswith("\n"))
        self.assertEqual(returned_report, json.loads(serialized_report))


class LocalInspectionTests(unittest.TestCase):
    def test_summarizes_sidecar_receipt_and_integrity_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            symbol_directory = repository_root / "data" / "end_of_day" / "TEST"
            symbol_directory.mkdir(parents=True)
            data_path = symbol_directory / "raw.csv.gz"
            data_path.write_bytes(b"x")
            os.utime(data_path, ns=(1_000_000_000, 1_000_000_000))
            data_stat = data_path.stat()
            sidecar = {
                "file": "raw.csv.gz",
                "adjustment": "raw",
                "sha256": "abc",
                "bytes": data_stat.st_size,
                "mtime_ns": data_stat.st_mtime_ns,
                "rows": 2,
                "sessions": 2,
                "from": "2024-01-02T05:00:00Z",
                "to": "2024-01-03T05:00:00Z",
                "request_from": "2016-01-01T00:00:00Z",
                "request_to": "2024-01-04T00:00:00Z",
                "asof": "2024-01-04",
            }
            (symbol_directory / "raw.csv.gz.manifest.json").write_text(json.dumps(sidecar))
            pipeline_manifest = {
                "pipeline_version": 14,
                "receipt_count": 1,
                "receipts": [{"data_file": "raw.csv.gz", "download_sha256": "abc"}],
            }
            (symbol_directory / "pipeline.manifest.json").write_text(json.dumps(pipeline_manifest))
            integrity_manifest = {
                "pipeline_version": 14,
                "failure_count": 1,
                "failures": [
                    {
                        "data_file": "raw.csv.gz",
                        "pipeline_version": 14,
                        "row_failure_count": 0,
                        "dataset_failure_count": 1,
                        "dataset_failures": [
                            {
                                "validation_error": "US market session coverage failed",
                                "missing_sessions": ["2024-01-04", "2024-01-05"],
                                "unexpected_sessions": [],
                            }
                        ],
                    }
                ],
            }
            (symbol_directory / "integrity_failure.manifest.json").write_text(json.dumps(integrity_manifest))

            report = inspect_local_downloads(repository_root, "TEST")

        self.assertTrue(report["present"])
        self.assertFalse(report["download_sha256_authentication_performed"])
        self.assertEqual(1, report["download_count"])
        download = report["downloads"][0]
        self.assertTrue(download["stat_matches_sidecar"])
        self.assertTrue(download["pipeline_receipt_matches"])
        dataset_failure = download["integrity_failure"]["dataset_failures"][0]
        self.assertEqual(2, dataset_failure["missing_sessions_count"])
        self.assertEqual("2024-01-04", dataset_failure["first_missing_sessions"])
        self.assertEqual("2024-01-05", dataset_failure["last_missing_sessions"])

    def test_reports_gzip_without_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            symbol_directory = repository_root / "data" / "end_of_day" / "TEST"
            symbol_directory.mkdir(parents=True)
            (symbol_directory / "raw.csv.gz").write_bytes(b"orphan")

            report = inspect_local_downloads(repository_root, "TEST")

        self.assertEqual(1, report["download_count"])
        self.assertTrue(report["downloads"][0]["gzip_present"])
        self.assertFalse(report["downloads"][0]["sidecar_present"])
        self.assertFalse(report["downloads"][0]["stat_matches_sidecar"])

    def test_missing_symbol_directory_is_reported_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            report = inspect_local_downloads(repository_root, "ABSENT")

            self.assertFalse((repository_root / "data").exists())

        self.assertFalse(report["present"])
        self.assertEqual([], report["downloads"])
        self.assertEqual([], report["manifest_errors"])


class AlpacaIdentityClientTests(unittest.TestCase):
    def test_asset_not_found_returns_none_and_closes_error_response(self) -> None:
        response_body = io.BytesIO(b'{"message":"not found"}')
        http_error = urllib.error.HTTPError(
            "https://example.test/asset",
            404,
            "Not Found",
            {},
            response_body,
        )
        client = AlpacaIdentityClient("key", "secret", rate_limiter=FakeRateLimiter())

        with mock.patch("urllib.request.urlopen", side_effect=http_error):
            result = client.request_json("https://example.test/asset", allow_not_found=True)

        self.assertIsNone(result)
        self.assertTrue(response_body.closed)
        self.assertEqual(1, client.request_count)

    def test_rate_limit_response_is_deferred_and_retried(self) -> None:
        response_body = io.BytesIO(b'{"message":"slow down"}')
        http_error = urllib.error.HTTPError(
            "https://example.test/asset",
            429,
            "Too Many Requests",
            {"Retry-After": "0"},
            response_body,
        )
        success_response = FakeHttpResponse(b'{"id":"asset-id"}')
        rate_limiter = FakeRateLimiter()
        client = AlpacaIdentityClient("key", "secret", rate_limiter=rate_limiter)

        with mock.patch("urllib.request.urlopen", side_effect=[http_error, success_response]):
            result = client.request_json("https://example.test/asset")

        self.assertEqual({"id": "asset-id"}, result)
        self.assertEqual([0.25], rate_limiter.deferred_seconds)
        self.assertEqual(2, rate_limiter.wait_count)
        self.assertEqual(2, client.request_count)
        self.assertTrue(response_body.closed)

    def test_invalid_json_is_reported_as_runtime_error(self) -> None:
        client = AlpacaIdentityClient("key", "secret", rate_limiter=FakeRateLimiter())

        with mock.patch("urllib.request.urlopen", return_value=FakeHttpResponse(b"not-json")):
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                client.request_json("https://example.test/asset")

    def test_corporate_actions_are_paginated_and_normalized(self) -> None:
        client = AlpacaIdentityClient("key", "secret", rate_limiter=FakeRateLimiter())
        client.request_json = mock.Mock(
            side_effect=[
                {
                    "corporate_actions": {
                        "name_changes": [{"id": "one", "old_symbol": "FB", "new_symbol": "META"}]
                    },
                    "next_page_token": "next token",
                },
                {
                    "corporate_actions": {"cash_dividends": [{"id": "two", "symbol": "META"}]},
                    "next_page_token": None,
                },
            ]
        )

        actions = client.get_corporate_actions("FB", "2016-01-01", "2026-10-06")

        self.assertEqual(["name_change", "cash_dividend"], [item["action_type"] for item in actions])
        second_url = client.request_json.call_args_list[1].args[0]
        self.assertIn("page_token=next+token", second_url)

    def test_repeated_corporate_action_page_token_is_rejected(self) -> None:
        client = AlpacaIdentityClient("key", "secret", rate_limiter=FakeRateLimiter())
        page = {"corporate_actions": {}, "next_page_token": "repeated"}
        client.request_json = mock.Mock(side_effect=[page, page])

        with self.assertRaisesRegex(RuntimeError, "repeated a page token"):
            client.get_corporate_actions("FB", "2016-01-01", "2026-10-06")


if __name__ == "__main__":
    unittest.main()
