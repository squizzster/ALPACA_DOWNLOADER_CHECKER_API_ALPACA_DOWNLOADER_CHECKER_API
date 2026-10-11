import io
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import libs.alpaca_end_of_day_ticker_pipeline.command_line as ticker_command_line_module
from libs.alpaca_end_of_day_ticker_pipeline import (
    EndOfDayTickerPipelineResult,
    SecurityOnlineEvaluation,
    parse_end_of_day_ticker_pipeline_arguments,
    resolve_authoritative_alpaca_symbol,
    run_end_of_day_ticker_pipeline,
    run_end_of_day_tickers_pipeline_command,
)
from libs.alpaca_end_of_day_ticker_pipeline.online_status import (
    _positive_raw_activity_during_exclusion,
    _session_grid_has_unresolved_boundary,
    _short_same_security_name_change_exclusions,
)
from libs.download_alpaca_end_of_day_bars import (
    REQUESTS_PER_MINUTE,
    AlpacaInvalidTickerCodeError,
    EndOfDayDownloadPlan,
    EndOfDayDownloadStageResult,
)


def correlation_receipt() -> dict:
    return {
        "publication_status": "published",
        "correlation_generation_sha256": "c" * 64,
        "partition": {
            "source_row_count": 10,
            "correlated_row_count": 6,
            "uncorrelated_row_count": 4,
            "row_partition_complete": True,
        },
    }


def warning_pipeline_receipt() -> dict:
    return {
        "publication_status": "published",
        "warning_pipeline_generation_sha256": "w" * 64,
        "summary": {
            "warning_count": 2,
            "information_count": 1,
        },
    }


def security_master_receipt() -> dict:
    return {
        "publication_status": "published",
        "security_master_version": 4,
        "security_master_generation_sha256": "m" * 64,
        "master_id_count": 1,
        "output": {"sha256": "i" * 64},
    }


def ticker_pipeline_result(
    symbol: str,
    *,
    integrity_failure_count: int = 0,
    disposition: dict | None = None,
) -> EndOfDayTickerPipelineResult:
    lineage = {"symbol_disposition": disposition} if disposition is not None else {}
    return EndOfDayTickerPipelineResult(
        download_stage=EndOfDayDownloadStageResult(
            symbol=symbol,
            requested_adjustment_modes=("raw",),
            pricing_data_integrity_failure_count=integrity_failure_count,
        ),
        security_lineage_report={"symbol": symbol, "lineage": lineage},
        security_correlation_receipt=correlation_receipt(),
        warning_pipeline_receipt=warning_pipeline_receipt(),
    )


def quarantined_empty_symbol_disposition() -> dict:
    return {
        "classification": "unsupported_empty_symbol",
        "data_policy": "quarantine",
        "recurring_pipeline_action": "skip",
        "input_symbol_asset_lookup": {"outcome": "not_found", "http_status": 404},
        "resolved_alpaca_asset_ids": [],
        "local_market_data": {
            "complete_row_count_evidence": True,
            "nonempty_download_count": 0,
            "maximum_rows": 0,
        },
        "market_data_series_identity": None,
        "test_symbol_evidence": None,
    }


class VerifiedSessionExclusionRuleTests(unittest.TestCase):
    @staticmethod
    def _name_change_fixture(process_date: str = "2024-01-03"):
        adjustments = (
            "all",
            "dividend",
            "dividend,spin-off",
            "raw",
            "spin-off",
            "split",
            "split,dividend",
            "split,spin-off",
        )
        bars = [
            {"timestamp": "2024-01-02T05:00:00Z"},
            {"timestamp": "2024-01-04T05:00:00Z"},
        ]
        identity = {
            "alpaca_asset_ids": ["asset-1"],
            "cusips": ["123456789"],
            "exchanges": ["NASDAQ"],
            "available_adjustments": list(adjustments),
            "observed_history_ranges": [
                {
                    "ticker": "OLD",
                    "first_session_date": "2024-01-02",
                    "last_session_date": "2024-01-02",
                },
                {
                    "ticker": "NEW",
                    "first_session_date": "2024-01-04",
                    "last_session_date": "2024-01-04",
                },
            ],
            "identity_events": [{"action_type": "name_change", "event_id": "event-1"}],
        }
        results = {adjustment: {"bars": list(bars)} for adjustment in adjustments}
        lineage = {
            "corporate_actions": [
                {
                    "action_type": "name_change",
                    "id": "event-1",
                    "old_symbol": "OLD",
                    "new_symbol": "NEW",
                    "old_cusip": "123456789",
                    "new_cusip": "123456789",
                    "process_date": process_date,
                }
            ],
            "asset_lookups": [
                {
                    "lookup_kind": "symbol",
                    "lookup": "OLD",
                    "outcome": "not_found",
                    "asset": None,
                },
                {
                    "lookup_kind": "symbol",
                    "lookup": "NEW",
                    "outcome": "found",
                    "asset": {"id": "asset-1"},
                },
                {
                    "lookup_kind": "cusip",
                    "lookup": "123456789",
                    "outcome": "found",
                    "asset": {"id": "asset-1"},
                },
            ],
            "identities": [{"id": "asset-1", "symbol": "NEW"}],
            "unresolved_cusips": [],
        }
        receipts = (
            {
                "evidence_issues": [],
                "lineage_report": {"lineage": lineage},
            },
        )
        raw_source_session_dates = frozenset(("2024-01-02", "2024-01-04"))
        return identity, results, receipts, raw_source_session_dates

    def test_one_session_same_security_name_change_is_resolved(self) -> None:
        for process_date in ("2024-01-03", "2024-01-04"):
            with self.subTest(process_date=process_date):
                identity, results, receipts, raw_dates = self._name_change_fixture(process_date)
                self.assertEqual(
                    frozenset(("2024-01-03",)),
                    _short_same_security_name_change_exclusions(
                        identity,
                        results,
                        receipts,
                        raw_dates,
                    ),
                )

    def test_up_to_five_sessions_resolve_but_six_or_disagreement_do_not(self) -> None:
        identity, results, receipts, raw_dates = self._name_change_fixture("2024-01-09")
        for result in results.values():
            result["bars"][-1] = {"timestamp": "2024-01-09T05:00:00Z"}
        raw_dates = frozenset(("2024-01-02", "2024-01-09"))
        identity["observed_history_ranges"][-1]["first_session_date"] = "2024-01-09"
        identity["observed_history_ranges"][-1]["last_session_date"] = "2024-01-09"
        self.assertEqual(
            frozenset(("2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture("2024-01-11")
        for result in results.values():
            result["bars"][-1] = {"timestamp": "2024-01-11T05:00:00Z"}
        raw_dates = frozenset(("2024-01-02", "2024-01-11"))
        identity["observed_history_ranges"][-1]["first_session_date"] = "2024-01-11"
        identity["observed_history_ranges"][-1]["last_session_date"] = "2024-01-11"
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture()
        results["all"]["bars"].insert(1, {"timestamp": "2024-01-03T05:00:00Z"})
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

    def test_competing_lookup_or_duplicate_action_is_not_resolved(self) -> None:
        identity, results, receipts, raw_dates = self._name_change_fixture()
        receipts[0]["lineage_report"]["lineage"]["asset_lookups"][0] = {
            "lookup_kind": "symbol",
            "lookup": "OLD",
            "outcome": "found",
            "asset": {"id": "different-asset"},
        }
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture()
        competing = dict(receipts[0]["lineage_report"]["lineage"]["corporate_actions"][0])
        competing["id"] = "event-2"
        receipts[0]["lineage_report"]["lineage"]["corporate_actions"].append(competing)
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture()
        receipts[0]["lineage_report"]["lineage"]["identities"].append({"id": "different-asset", "symbol": "OLD"})
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture()
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(
                identity,
                results,
                receipts,
                raw_dates | {"2024-01-03"},
            ),
        )

        identity, results, receipts, raw_dates = self._name_change_fixture()
        receipts[0]["lineage_report"]["lineage"]["corporate_actions"].append(
            {
                "action_type": "stock_merger",
                "id": "event-2",
                "process_date": "2024-01-03",
            }
        )
        self.assertEqual(
            frozenset(),
            _short_same_security_name_change_exclusions(identity, results, receipts, raw_dates),
        )

    def test_only_covered_missing_sessions_without_positive_activity_are_resolved(self) -> None:
        active_bar = {
            "timestamp": "2024-01-02T05:00:00Z",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10",
            "volume": "100",
            "trade_count": "10",
            "vwap": "10",
        }
        later_bar = {**active_bar, "timestamp": "2024-01-04T05:00:00Z"}
        excluded_dates = frozenset(("2024-01-03",))

        self.assertEqual(
            (True, False),
            _session_grid_has_unresolved_boundary([active_bar, later_bar]),
        )
        self.assertEqual(
            (False, False),
            _session_grid_has_unresolved_boundary(
                [active_bar, later_bar],
                excluded_dates,
            ),
        )

        zero_observation = {
            **active_bar,
            "timestamp": "2024-01-03T05:00:00Z",
            "open": "10",
            "high": "10",
            "low": "10",
            "close": "10",
            "volume": "0",
            "trade_count": "0",
            "vwap": "0",
        }
        positive_observation = {**active_bar, "timestamp": "2024-01-03T05:00:00Z"}
        self.assertEqual(
            (False, False),
            _positive_raw_activity_during_exclusion(
                [active_bar, zero_observation, later_bar],
                excluded_dates,
            ),
        )
        self.assertEqual(
            (True, False),
            _positive_raw_activity_during_exclusion(
                [active_bar, positive_observation, later_bar],
                excluded_dates,
            ),
        )


class EndOfDayTickerPipelineTests(unittest.TestCase):
    def test_authoritative_symbol_resolution_uses_exact_alpaca_asset_symbol(self) -> None:
        rate_limiter = object()
        asset = {
            "id": "664c71d6-c62a-4618-9831-756a3ef74ae8",
            "class": "us_equity",
            "exchange": "NYSE",
            "symbol": "BF.A",
            "name": "Brown-Forman Corporation Class A",
            "status": "active",
            "tradable": True,
            "non_contract_field": "ignored",
        }
        with patch("libs.alpaca_end_of_day_ticker_pipeline.orchestration.AlpacaIdentityClient") as client_type:
            client_type.return_value.get_asset_lookup.return_value = {
                "outcome": "found",
                "http_status": 200,
                "asset": asset,
            }
            resolution = resolve_authoritative_alpaca_symbol(
                "BFA",
                "key",
                "secret",
                request_rate_limiter=rate_limiter,
            )

        client_type.assert_called_once_with("key", "secret", rate_limiter=rate_limiter)
        client_type.return_value.get_asset_lookup.assert_called_once_with("BFA")
        self.assertEqual("BFA", resolution["submitted_symbol"])
        self.assertEqual("BF.A", resolution["authoritative_symbol"])
        self.assertTrue(resolution["changed"])
        self.assertEqual(asset["id"], resolution["asset"]["id"])
        self.assertNotIn("non_contract_field", resolution["asset"])

    def test_missing_alpaca_asset_keeps_the_submitted_symbol(self) -> None:
        with patch("libs.alpaca_end_of_day_ticker_pipeline.orchestration.AlpacaIdentityClient") as client_type:
            client_type.return_value.get_asset_lookup.return_value = {
                "outcome": "not_found",
                "http_status": 404,
                "asset": None,
            }
            resolution = resolve_authoritative_alpaca_symbol("REEDD", "key", "secret")

        self.assertEqual("REEDD", resolution["authoritative_symbol"])
        self.assertFalse(resolution["changed"])
        self.assertEqual("alpaca_asset_lookup_not_found", resolution["basis"])

    def test_batch_accepts_up_to_three_workers_at_199_requests_per_minute(self) -> None:
        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(["--symbol", "DELL", "--workers", "3"])

        self.assertEqual(3, cli_arguments.workers)
        self.assertEqual(199, REQUESTS_PER_MINUTE)

        with self.assertRaises(SystemExit):
            parse_end_of_day_ticker_pipeline_arguments(["--symbol", "DELL", "--workers", "4"])

    def test_three_workers_process_tickers_concurrently_and_return_input_order(self) -> None:
        worker_barrier = threading.Barrier(3)
        active_worker_lock = threading.Lock()
        active_worker_count = 0
        maximum_active_worker_count = 0

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            nonlocal active_worker_count, maximum_active_worker_count
            self.assertIs(bar_request_rate_limiter, identity_request_rate_limiter)
            with active_worker_lock:
                active_worker_count += 1
                maximum_active_worker_count = max(maximum_active_worker_count, active_worker_count)
            try:
                worker_barrier.wait(timeout=5)
            finally:
                with active_worker_lock:
                    active_worker_count -= 1
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            [
                "--symbol",
                "DELL",
                "-s",
                "FB",
                "-s",
                "TWTR",
                "--adjustment",
                "raw",
                "--workers",
                "3",
            ]
        )
        results = run_end_of_day_tickers_pipeline_command(
            cli_arguments,
            output_stream=io.StringIO(),
            error_stream=io.StringIO(),
            ticker_pipeline_runner=run_ticker_pipeline,
        )

        self.assertEqual(3, maximum_active_worker_count)
        self.assertEqual(
            ["DELL", "FB", "TWTR"],
            [result.download_stage.symbol for result in results],
        )

    def test_batch_freezes_default_request_end_timestamp_before_first_ticker(self) -> None:
        observed_request_end_timestamps = []

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            observed_request_end_timestamps.append(request_end_timestamp)
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            ["--symbol", "DELL", "-s", "FB", "--adjustment", "raw"]
        )
        with patch("libs.alpaca_end_of_day_ticker_pipeline.command_line.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(
                2026,
                10,
                6,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            )
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=io.StringIO(),
                error_stream=io.StringIO(),
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(
            ["2026-10-06T00:00:00Z", "2026-10-06T00:00:00Z"],
            observed_request_end_timestamps,
        )
        mocked_datetime.now.assert_called_once_with(timezone.utc)

    def test_explicit_request_end_date_is_used_without_reading_the_clock(self) -> None:
        observed_request_end_timestamps = []

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            observed_request_end_timestamps.append(request_end_timestamp)
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            [
                "--symbol",
                "DELL",
                "-s",
                "FB",
                "--adjustment",
                "raw",
                "--request-end",
                "2026-10-06",
            ]
        )
        with patch("libs.alpaca_end_of_day_ticker_pipeline.command_line.datetime") as mocked_datetime:
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=io.StringIO(),
                error_stream=io.StringIO(),
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(
            ["2026-10-06T00:00:00Z", "2026-10-06T00:00:00Z"],
            observed_request_end_timestamps,
        )
        mocked_datetime.now.assert_not_called()

    def test_symbol_file_normalizes_and_deduplicates_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text(" dell \n\nFB\nDell\n", encoding="utf-8")

            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
                ["--file", str(symbol_file), "--adjustment", "raw"]
            )
            processed_symbols = []

            def run_ticker_pipeline(
                download_plan,
                *,
                repository_root,
                environment,
                request_end_timestamp,
                bar_request_rate_limiter,
                identity_request_rate_limiter,
            ):
                processed_symbols.append(download_plan.symbol)
                return EndOfDayTickerPipelineResult(
                    download_stage=EndOfDayDownloadStageResult(
                        symbol=download_plan.symbol,
                        requested_adjustment_modes=download_plan.adjustment_modes,
                        pricing_data_integrity_failure_count=0,
                    ),
                    security_lineage_report={"symbol": download_plan.symbol},
                    security_correlation_receipt=correlation_receipt(),
                    warning_pipeline_receipt=warning_pipeline_receipt(),
                )

            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=io.StringIO(),
                error_stream=io.StringIO(),
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(["DELL", "FB"], processed_symbols)

    def test_symbol_file_reports_invalid_entry_with_line_number(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text("DELL\nnot a symbol\n", encoding="utf-8")
            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(["--file", str(symbol_file)])

            with self.assertRaisesRegex(
                SystemExit,
                rf"invalid symbol at {symbol_file}:2: 'not a symbol'",
            ):
                run_end_of_day_tickers_pipeline_command(
                    cli_arguments,
                    output_stream=io.StringIO(),
                    error_stream=io.StringIO(),
                )

    def test_symbol_file_must_contain_a_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text("\n  \n", encoding="utf-8")
            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(["--file", str(symbol_file)])

            with self.assertRaisesRegex(SystemExit, "symbol file contains no symbols"):
                run_end_of_day_tickers_pipeline_command(
                    cli_arguments,
                    output_stream=io.StringIO(),
                    error_stream=io.StringIO(),
                )

    def test_symbol_file_skips_authenticated_prior_recurring_disposition(self) -> None:
        disposition = quarantined_empty_symbol_disposition()
        processed_symbols = []

        def run_ticker_pipeline(download_plan, **_kwargs):
            processed_symbols.append(download_plan.symbol)
            return ticker_pipeline_result(download_plan.symbol)

        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text("AACC\nAAPL\n", encoding="utf-8")
            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
                ["--file", str(symbol_file), "--adjustment", "raw"]
            )
            output_stream = io.StringIO()
            with patch(
                "libs.alpaca_end_of_day_ticker_pipeline.command_line._authenticated_recurring_skip_disposition",
                side_effect=lambda _root, symbol: disposition if symbol == "AACC" else None,
            ):
                results = run_end_of_day_tickers_pipeline_command(
                    cli_arguments,
                    repository_root=Path(temporary_directory),
                    output_stream=output_stream,
                    error_stream=io.StringIO(),
                    ticker_pipeline_runner=run_ticker_pipeline,
                )

        self.assertEqual(["AAPL"], processed_symbols)
        self.assertEqual(["AAPL"], [result.download_stage.symbol for result in results])
        self.assertIn("AACC: skipped authenticated recurring disposition", output_stream.getvalue())
        self.assertIn("1 ticker(s) skipped", output_stream.getvalue())

    def test_recurring_skip_fails_closed_when_frozen_sources_do_not_authenticate(self) -> None:
        disposition = quarantined_empty_symbol_disposition()
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            receipt_path = (
                repository_root / "data" / "end_of_day" / "AACC" / "security_correlation.manifest.json"
            )
            receipt_path.parent.mkdir(parents=True)
            receipt_path.write_text(
                json.dumps({"symbol_disposition": disposition}),
                encoding="utf-8",
            )
            authenticated = SimpleNamespace(
                receipt={"symbol_disposition": disposition},
                sources_by_filename={},
            )
            with (
                patch.object(
                    ticker_command_line_module,
                    "authenticate_security_correlation",
                    return_value=authenticated,
                ),
                patch.object(
                    ticker_command_line_module,
                    "_authenticated_frozen_sources_match_correlation",
                    return_value=False,
                ),
            ):
                observed = ticker_command_line_module._authenticated_recurring_skip_disposition(
                    repository_root,
                    "AACC",
                )
            with (
                patch.object(
                    ticker_command_line_module,
                    "authenticate_security_correlation",
                    return_value=authenticated,
                ),
                patch.object(
                    ticker_command_line_module,
                    "_authenticated_frozen_sources_match_correlation",
                    return_value=True,
                ),
            ):
                authenticated_observed = ticker_command_line_module._authenticated_recurring_skip_disposition(
                    repository_root,
                    "AACC",
                )

        self.assertIsNone(observed)
        self.assertEqual(disposition, authenticated_observed)

    def test_include_skipped_symbols_overrides_recurring_disposition(self) -> None:
        processed_symbols = []

        def run_ticker_pipeline(download_plan, **_kwargs):
            processed_symbols.append(download_plan.symbol)
            return ticker_pipeline_result(download_plan.symbol)

        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text("AACC\n", encoding="utf-8")
            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
                [
                    "--file",
                    str(symbol_file),
                    "--adjustment",
                    "raw",
                    "--include-skipped-symbols",
                ]
            )
            with patch(
                "libs.alpaca_end_of_day_ticker_pipeline.command_line._authenticated_recurring_skip_disposition"
            ) as skip_lookup:
                run_end_of_day_tickers_pipeline_command(
                    cli_arguments,
                    repository_root=Path(temporary_directory),
                    output_stream=io.StringIO(),
                    error_stream=io.StringIO(),
                    ticker_pipeline_runner=run_ticker_pipeline,
                )

        self.assertEqual(["AACC"], processed_symbols)
        skip_lookup.assert_not_called()

    def test_new_quarantine_disposition_is_not_a_recurring_batch_failure(self) -> None:
        disposition = quarantined_empty_symbol_disposition()

        def run_ticker_pipeline(download_plan, **_kwargs):
            return ticker_pipeline_result(
                download_plan.symbol,
                integrity_failure_count=8,
                disposition=disposition,
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            symbol_file = Path(temporary_directory) / "symbols.txt"
            symbol_file.write_text("AACC\n", encoding="utf-8")
            cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
                ["--file", str(symbol_file), "--adjustment", "raw"]
            )
            output_stream = io.StringIO()
            with patch(
                "libs.alpaca_end_of_day_ticker_pipeline.command_line._authenticated_recurring_skip_disposition",
                return_value=None,
            ):
                results = run_end_of_day_tickers_pipeline_command(
                    cli_arguments,
                    repository_root=Path(temporary_directory),
                    output_stream=output_stream,
                    error_stream=io.StringIO(),
                    ticker_pipeline_runner=run_ticker_pipeline,
                )

        self.assertEqual(1, len(results))
        self.assertIn("8 recorded integrity-failure artifact(s)", output_stream.getvalue())
        self.assertIn("omitted from the recurring failure total", output_stream.getvalue())

    def test_explicit_symbol_still_surfaces_retained_integrity_failure(self) -> None:
        disposition = quarantined_empty_symbol_disposition()

        def run_ticker_pipeline(download_plan, **_kwargs):
            return ticker_pipeline_result(
                download_plan.symbol,
                integrity_failure_count=1,
                disposition=disposition,
            )

        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(["--symbol", "AACC", "--adjustment", "raw"])
        with self.assertRaisesRegex(SystemExit, "1"):
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=io.StringIO(),
                error_stream=io.StringIO(),
                ticker_pipeline_runner=run_ticker_pipeline,
            )

    def test_recorded_integrity_failures_continue_into_security_correlation(self) -> None:
        pipeline_events = []
        download_plan = EndOfDayDownloadPlan(
            symbol="FB",
            adjustment_modes=("raw",),
            force_authenticated_replacement=False,
            repair_orphaned_download_pair=False,
        )

        def run_download_and_validation_stage(
            supplied_download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            request_rate_limiter,
        ):
            pipeline_events.append("download_and_validation")
            self.assertEqual(download_plan, supplied_download_plan)
            self.assertEqual("2026-10-06T00:00:00Z", request_end_timestamp)
            return EndOfDayDownloadStageResult(
                symbol="FB",
                requested_adjustment_modes=("raw",),
                pricing_data_integrity_failure_count=1,
            )

        def resolve_authoritative_symbol(
            submitted_symbol,
            api_key_id,
            api_secret_key,
            *,
            request_rate_limiter,
        ):
            pipeline_events.append("resolve_symbol")
            self.assertEqual(("FB", "key", "secret"), (submitted_symbol, api_key_id, api_secret_key))
            return {
                "submitted_symbol": "FB",
                "authoritative_symbol": "FB",
                "changed": False,
                "basis": "alpaca_asset_lookup",
                "http_status": 200,
                "asset": {"id": "asset-id", "class": "us_equity", "symbol": "FB"},
            }

        def inspect_security_lineage(
            ticker,
            api_key_id,
            api_secret_key,
            *,
            repository_root,
            request_rate_limiter,
        ):
            pipeline_events.append("identity")
            self.assertEqual(("FB", "key", "secret"), (ticker, api_key_id, api_secret_key))
            return {"symbol": ticker}

        def build_security_correlation_report(lineage_report, repository_root):
            pipeline_events.append("correlate")
            return {
                "symbol": lineage_report["symbol"],
                "correlated": {
                    "segments": [
                        {"canonical_security": {"security_id": "a" * 64}},
                    ],
                },
            }

        def publish_security_correlation(repository_root, security_correlation_report):
            pipeline_events.append("publish_correlation")
            return correlation_receipt()

        def build_warning_pipeline_report(repository_root, symbol):
            pipeline_events.append("validate_market_data")
            self.assertEqual("FB", symbol)
            return {"symbol": symbol}

        def publish_warning_pipeline(repository_root, report):
            pipeline_events.append("publish_warning_pipeline")
            self.assertEqual("FB", report["symbol"])
            return warning_pipeline_receipt()

        def evaluate_online_status(repository_root, master_ids):
            pipeline_events.append("evaluate_online_status")
            self.assertEqual(["a" * 64], master_ids)
            return SecurityOnlineEvaluation(
                online_by_master_id={"a" * 64: False},
                availability_status_by_master_id={"a" * 64: "offline"},
                security_object_sha256_by_master_id={"a" * 64: "s" * 64},
                source_object_sha256_by_symbol={"FB": "o" * 64},
            )

        def publish_security_master(
            repository_root,
            report,
            receipt,
            *,
            online_by_master_id,
        ):
            pipeline_events.append("publish_security_master_provisional")
            self.assertIsNone(online_by_master_id)
            return security_master_receipt()

        def publish_online_status(
            repository_root,
            online_by_master_id,
            *,
            availability_status_by_master_id,
            expected_security_object_sha256_by_master_id,
            expected_source_object_sha256_by_symbol,
        ):
            pipeline_events.append("publish_online_status")
            self.assertEqual({"a" * 64: False}, online_by_master_id)
            self.assertEqual({"a" * 64: "offline"}, availability_status_by_master_id)
            self.assertEqual(
                {"a" * 64: "s" * 64},
                expected_security_object_sha256_by_master_id,
            )
            self.assertEqual({"FB": "o" * 64}, expected_source_object_sha256_by_symbol)
            return security_master_receipt()

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = run_end_of_day_ticker_pipeline(
                download_plan,
                repository_root=Path(temporary_directory),
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                request_end_timestamp="2026-10-06T00:00:00Z",
                authoritative_symbol_resolver=resolve_authoritative_symbol,
                download_and_validation_stage_runner=run_download_and_validation_stage,
                security_lineage_inspector=inspect_security_lineage,
                security_correlation_report_builder=build_security_correlation_report,
                security_correlation_publisher=publish_security_correlation,
                warning_pipeline_report_builder=build_warning_pipeline_report,
                warning_pipeline_publisher=publish_warning_pipeline,
                security_online_status_evaluator=evaluate_online_status,
                security_master_publisher=publish_security_master,
                security_master_online_status_publisher=publish_online_status,
            )

        self.assertEqual(
            [
                "resolve_symbol",
                "download_and_validation",
                "identity",
                "correlate",
                "publish_correlation",
                "validate_market_data",
                "publish_warning_pipeline",
                "publish_security_master_provisional",
                "evaluate_online_status",
                "publish_online_status",
            ],
            pipeline_events,
        )
        self.assertTrue(result.download_stage.has_pricing_data_integrity_failures)
        self.assertTrue(result.security_correlation_receipt["partition"]["row_partition_complete"])
        self.assertEqual(2, result.warning_pipeline_receipt["summary"]["warning_count"])

    def test_security_master_finalization_is_serialized_between_workers(self) -> None:
        workers_ready = threading.Barrier(2)
        state_lock = threading.Lock()
        first_evaluation_started = threading.Event()
        overlapping_finalization_started = threading.Event()
        active_finalization_thread = None
        events = []

        def resolve_authoritative_symbol(
            submitted_symbol,
            _api_key_id,
            _api_secret_key,
            *,
            request_rate_limiter,
        ):
            return {
                "submitted_symbol": submitted_symbol,
                "authoritative_symbol": submitted_symbol,
                "changed": False,
                "basis": "alpaca_asset_lookup",
                "http_status": 200,
                "asset": {
                    "id": f"asset-{submitted_symbol}",
                    "class": "us_equity",
                    "symbol": submitted_symbol,
                },
            }

        def run_download_stage(download_plan, **_kwargs):
            return EndOfDayDownloadStageResult(
                symbol=download_plan.symbol,
                requested_adjustment_modes=download_plan.adjustment_modes,
                pricing_data_integrity_failure_count=0,
            )

        def inspect_lineage(ticker, *_args, **_kwargs):
            return {"symbol": ticker}

        def build_correlation(lineage_report, _repository_root):
            master_id = ("a" if lineage_report["symbol"] == "FIRST" else "b") * 64
            return {
                "symbol": lineage_report["symbol"],
                "correlated": {
                    "segments": [
                        {"canonical_security": {"security_id": master_id}},
                    ],
                },
            }

        def publish_correlation(_repository_root, _report):
            return correlation_receipt()

        def build_warnings(_repository_root, symbol):
            return {"symbol": symbol}

        def publish_warnings(_repository_root, _report):
            workers_ready.wait(timeout=5)
            return warning_pipeline_receipt()

        def publish_security_master(
            _repository_root,
            report,
            _receipt,
            *,
            online_by_master_id,
        ):
            nonlocal active_finalization_thread
            self.assertIsNone(online_by_master_id)
            thread_id = threading.get_ident()
            with state_lock:
                if active_finalization_thread is not None:
                    overlapping_finalization_started.set()
                active_finalization_thread = thread_id
                events.append((report["symbol"], "provisional"))
            return security_master_receipt()

        def evaluate_online_status(_repository_root, master_ids):
            thread_id = threading.get_ident()
            with state_lock:
                self.assertEqual(thread_id, active_finalization_thread)
                events.append((master_ids[0][0], "evaluate"))
            if not first_evaluation_started.is_set():
                first_evaluation_started.set()
                overlapping_finalization_started.wait(timeout=0.2)
            master_id = master_ids[0]
            return SecurityOnlineEvaluation(
                online_by_master_id={master_id: True},
                availability_status_by_master_id={master_id: "online"},
                security_object_sha256_by_master_id={master_id: "s" * 64},
                source_object_sha256_by_symbol={"SOURCE": "o" * 64},
            )

        def publish_online_status(
            _repository_root,
            _online_by_master_id,
            *,
            availability_status_by_master_id,
            expected_security_object_sha256_by_master_id,
            expected_source_object_sha256_by_symbol,
        ):
            nonlocal active_finalization_thread
            self.assertEqual(["online"], list(availability_status_by_master_id.values()))
            self.assertEqual(["s" * 64], list(expected_security_object_sha256_by_master_id.values()))
            self.assertEqual({"SOURCE": "o" * 64}, expected_source_object_sha256_by_symbol)
            thread_id = threading.get_ident()
            with state_lock:
                self.assertEqual(thread_id, active_finalization_thread)
                events.append((next(iter(_online_by_master_id))[0], "online"))
                active_finalization_thread = None
            return security_master_receipt()

        def run_pipeline(symbol, repository_root):
            return run_end_of_day_ticker_pipeline(
                EndOfDayDownloadPlan(
                    symbol=symbol,
                    adjustment_modes=("raw",),
                    force_authenticated_replacement=False,
                    repair_orphaned_download_pair=False,
                ),
                repository_root=repository_root,
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                authoritative_symbol_resolver=resolve_authoritative_symbol,
                download_and_validation_stage_runner=run_download_stage,
                security_lineage_inspector=inspect_lineage,
                security_correlation_report_builder=build_correlation,
                security_correlation_publisher=publish_correlation,
                warning_pipeline_report_builder=build_warnings,
                warning_pipeline_publisher=publish_warnings,
                security_online_status_evaluator=evaluate_online_status,
                security_master_publisher=publish_security_master,
                security_master_online_status_publisher=publish_online_status,
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(run_pipeline, symbol, repository_root) for symbol in ("FIRST", "SECOND")
                ]
                results = [future.result(timeout=5) for future in futures]

        self.assertFalse(overlapping_finalization_started.is_set())
        self.assertEqual({"FIRST", "SECOND"}, {result.download_stage.symbol for result in results})
        self.assertIn(
            events,
            (
                [
                    ("FIRST", "provisional"),
                    ("a", "evaluate"),
                    ("a", "online"),
                    ("SECOND", "provisional"),
                    ("b", "evaluate"),
                    ("b", "online"),
                ],
                [
                    ("SECOND", "provisional"),
                    ("b", "evaluate"),
                    ("b", "online"),
                    ("FIRST", "provisional"),
                    ("a", "evaluate"),
                    ("a", "online"),
                ],
            ),
        )

    def test_alpaca_returned_symbol_is_authoritative_for_every_pipeline_stage(self) -> None:
        events = []
        supplied_plan = EndOfDayDownloadPlan(
            symbol="BFA",
            adjustment_modes=("raw",),
            force_authenticated_replacement=True,
            repair_orphaned_download_pair=False,
        )

        def resolve_authoritative_symbol(
            submitted_symbol,
            api_key_id,
            api_secret_key,
            *,
            request_rate_limiter,
        ):
            events.append(("resolve", submitted_symbol))
            return {
                "submitted_symbol": submitted_symbol,
                "authoritative_symbol": "BF.A",
                "changed": True,
                "basis": "alpaca_asset_lookup",
                "http_status": 200,
                "asset": {
                    "id": "664c71d6-c62a-4618-9831-756a3ef74ae8",
                    "class": "us_equity",
                    "symbol": "BF.A",
                },
            }

        def run_download_stage(download_plan, **_kwargs):
            events.append(("download", download_plan.symbol))
            self.assertTrue(download_plan.force_authenticated_replacement)
            return EndOfDayDownloadStageResult(
                symbol=download_plan.symbol,
                requested_adjustment_modes=download_plan.adjustment_modes,
                pricing_data_integrity_failure_count=0,
            )

        def inspect_lineage(ticker, *_args, **_kwargs):
            events.append(("lineage", ticker))
            return {"symbol": ticker, "lineage": {}}

        observed_correlation_report = {}

        def build_correlation(lineage_report, _repository_root):
            events.append(("correlate", lineage_report["symbol"]))
            self.assertEqual("BFA", lineage_report["symbol_resolution"]["submitted_symbol"])
            self.assertEqual("BF.A", lineage_report["symbol_resolution"]["authoritative_symbol"])
            return {
                "symbol": lineage_report["symbol"],
                "lineage_report": lineage_report,
                "correlated": {
                    "segments": [
                        {"canonical_security": {"security_id": "a" * 64}},
                    ],
                },
            }

        def publish_correlation(_repository_root, report):
            observed_correlation_report.update(report)
            events.append(("publish_correlation", report["symbol"]))
            return correlation_receipt()

        def build_warnings(_repository_root, symbol):
            events.append(("warnings", symbol))
            return {"symbol": symbol}

        def publish_warnings(_repository_root, report):
            events.append(("publish_warnings", report["symbol"]))
            return warning_pipeline_receipt()

        def evaluate_online_status(_repository_root, master_ids):
            events.append(("evaluate_online_status", "BF.A"))
            self.assertEqual(["a" * 64], master_ids)
            return SecurityOnlineEvaluation(
                online_by_master_id={"a" * 64: True},
                availability_status_by_master_id={"a" * 64: "online"},
                security_object_sha256_by_master_id={"a" * 64: "s" * 64},
                source_object_sha256_by_symbol={"BF.A": "o" * 64},
            )

        def publish_security_master(
            _repository_root,
            report,
            _receipt,
            *,
            online_by_master_id,
        ):
            events.append(("publish_security_master_provisional", report["symbol"]))
            self.assertIsNone(online_by_master_id)
            return security_master_receipt()

        def publish_online_status(
            _repository_root,
            online_by_master_id,
            *,
            availability_status_by_master_id,
            expected_security_object_sha256_by_master_id,
            expected_source_object_sha256_by_symbol,
        ):
            events.append(("publish_online_status", "BF.A"))
            self.assertEqual({"a" * 64: True}, online_by_master_id)
            self.assertEqual({"a" * 64: "online"}, availability_status_by_master_id)
            self.assertEqual(
                {"a" * 64: "s" * 64},
                expected_security_object_sha256_by_master_id,
            )
            self.assertEqual({"BF.A": "o" * 64}, expected_source_object_sha256_by_symbol)
            return security_master_receipt()

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = run_end_of_day_ticker_pipeline(
                supplied_plan,
                repository_root=Path(temporary_directory),
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                authoritative_symbol_resolver=resolve_authoritative_symbol,
                download_and_validation_stage_runner=run_download_stage,
                security_lineage_inspector=inspect_lineage,
                security_correlation_report_builder=build_correlation,
                security_correlation_publisher=publish_correlation,
                warning_pipeline_report_builder=build_warnings,
                warning_pipeline_publisher=publish_warnings,
                security_online_status_evaluator=evaluate_online_status,
                security_master_publisher=publish_security_master,
                security_master_online_status_publisher=publish_online_status,
            )

        self.assertEqual("BF.A", result.download_stage.symbol)
        self.assertEqual("BF.A", observed_correlation_report["symbol"])
        self.assertEqual(
            [
                ("resolve", "BFA"),
                ("download", "BF.A"),
                ("lineage", "BF.A"),
                ("correlate", "BF.A"),
                ("publish_correlation", "BF.A"),
                ("warnings", "BF.A"),
                ("publish_warnings", "BF.A"),
                ("publish_security_master_provisional", "BF.A"),
                ("evaluate_online_status", "BF.A"),
                ("publish_online_status", "BF.A"),
            ],
            events,
        )

    def test_repeatable_symbols_share_rate_limiters_and_finish_before_failure_exit(self) -> None:
        processed_symbols = []
        bar_rate_limiters = []
        identity_rate_limiters = []

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            processed_symbols.append(download_plan.symbol)
            bar_rate_limiters.append(bar_request_rate_limiter)
            identity_rate_limiters.append(identity_request_rate_limiter)
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=2 if download_plan.symbol == "FB" else 0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        output_stream = io.StringIO()
        error_stream = io.StringIO()
        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            [
                "--symbol",
                "DELL",
                "-s",
                "fb",
                "-s",
                "TWTR",
                "-s",
                "dell",
                "--adjustment",
                "raw",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "1"):
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                output_stream=output_stream,
                error_stream=error_stream,
                request_end_timestamp="2026-10-06T00:00:00Z",
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(["DELL", "FB", "TWTR"], processed_symbols)
        self.assertEqual(1, len({id(rate_limiter) for rate_limiter in bar_rate_limiters}))
        self.assertEqual(1, len({id(rate_limiter) for rate_limiter in identity_rate_limiters}))
        self.assertIs(bar_rate_limiters[0], identity_rate_limiters[0])
        self.assertEqual(3, output_stream.getvalue().count("correlated=6, uncorrelated=4"))
        self.assertEqual(3, output_stream.getvalue().count("warnings=2, information=1"))
        self.assertIn(
            "2 artifact(s) across 1 ticker(s) failed pricing/data integrity",
            error_stream.getvalue(),
        )

    def test_one_ticker_runtime_failure_does_not_cancel_the_remaining_batch(self) -> None:
        processed_symbols = []

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            processed_symbols.append(download_plan.symbol)
            if download_plan.symbol == "FB":
                raise RuntimeError("no correlatable rows")
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        output_stream = io.StringIO()
        error_stream = io.StringIO()
        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            [
                "--symbol",
                "DELL",
                "-s",
                "FB",
                "-s",
                "TWTR",
                "--adjustment",
                "raw",
                "--workers",
                "3",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "1"):
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=output_stream,
                error_stream=error_stream,
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual({"DELL", "FB", "TWTR"}, set(processed_symbols))
        self.assertEqual(2, output_stream.getvalue().count("correlated=6, uncorrelated=4"))
        self.assertIn(
            "FB: ticker pipeline failed; continuing batch: RuntimeError: no correlatable rows",
            error_stream.getvalue(),
        )
        self.assertIn("1 unclassified ticker pipeline failure(s) during the batch", error_stream.getvalue())

    def test_invalid_ticker_code_is_reported_as_a_known_error_and_still_fails_batch(self) -> None:
        processed_symbols = []

        def run_ticker_pipeline(download_plan, **_kwargs):
            processed_symbols.append(download_plan.symbol)
            if download_plan.symbol == "AMR1":
                raise AlpacaInvalidTickerCodeError(
                    "AMR1",
                    "raw",
                    400,
                    '{"message":"invalid symbol: AMR1"}',
                )
            return ticker_pipeline_result(download_plan.symbol)

        output_stream = io.StringIO()
        error_stream = io.StringIO()
        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            ["--symbol", "AMR1", "-s", "DELL", "--adjustment", "raw"]
        )

        with self.assertRaisesRegex(SystemExit, "1"):
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                output_stream=output_stream,
                error_stream=error_stream,
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(["AMR1", "DELL"], processed_symbols)
        self.assertIn(
            "AMR1: invalid_ticker_code; continuing batch",
            error_stream.getvalue(),
        )
        self.assertIn(
            "1 known invalid_ticker_code error(s) during the batch",
            error_stream.getvalue(),
        )
        self.assertNotIn("unclassified ticker pipeline failure", error_stream.getvalue())


if __name__ == "__main__":
    unittest.main()
