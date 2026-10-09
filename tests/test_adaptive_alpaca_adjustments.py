import contextlib
import hashlib
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_pipeline import (
    ALPACA_DOWNLOAD_BOUNDARY,
    EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    download_manifest_path,
    inspect_cached_download,
    validate_and_publish_alpaca_download,
)
from libs.download_alpaca_end_of_day_bars import (
    ALPACA_ADJUSTMENT_MODES,
    AlpacaInvalidTickerCodeError,
    EndOfDayDownloadPlan,
    decide_adaptive_adjustment_downloads,
    end_of_day_download_plan_from_command_line,
    parse_command_line_arguments,
    run_end_of_day_download_stage,
)

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")
REQUEST_END_TIMESTAMP = "2024-01-17T00:00:00Z"

PATTERN_GROUPS = {
    "all_equal": (tuple(ALPACA_ADJUSTMENT_MODES),),
    "dividend_only": (
        ("raw", "split", "spin-off", "split,spin-off"),
        ("dividend", "split,dividend", "dividend,spin-off", "all"),
    ),
    "split_only": (
        ("raw", "dividend", "spin-off", "dividend,spin-off"),
        ("split", "split,dividend", "split,spin-off", "all"),
    ),
    "split_and_dividend": (
        ("raw", "spin-off"),
        ("split", "split,spin-off"),
        ("dividend", "dividend,spin-off"),
        ("split,dividend", "all"),
    ),
    "dividend_and_spin_off": (
        ("raw", "split"),
        ("dividend", "split,dividend"),
        ("spin-off", "split,spin-off"),
        ("dividend,spin-off", "all"),
    ),
    "spin_off_only": (
        ("raw", "split", "dividend", "split,dividend"),
        ("spin-off", "split,spin-off", "dividend,spin-off", "all"),
    ),
    "all_distinct": tuple((adjustment,) for adjustment in ALPACA_ADJUSTMENT_MODES),
    "observed_pattern_8": (
        ("raw", "dividend"),
        ("split", "split,dividend"),
        ("spin-off", "dividend,spin-off"),
        ("split,spin-off", "all"),
    ),
    "observed_pattern_9": (
        ("raw", "dividend"),
        ("split", "split,dividend", "split,spin-off", "all"),
        ("spin-off", "dividend,spin-off"),
    ),
}

EXPECTED_DOWNLOAD_COUNTS = {
    "all_equal": 2,
    "dividend_only": 3,
    "split_only": 4,
    "split_and_dividend": 5,
    "dividend_and_spin_off": 5,
    "spin_off_only": 4,
    "all_distinct": 8,
    "observed_pattern_8": 5,
    "observed_pattern_9": 5,
}


def fixture_bars() -> list[dict]:
    return json.loads((TEST_FIXTURES_DIRECTORY / "ten_valid_daily_bars.json").read_text(encoding="utf-8"))


def adjustment_sha256s(groups: tuple[tuple[str, ...], ...]) -> dict[str, str]:
    return {
        adjustment: hashlib.sha256(f"equivalence-group-{index}".encode()).hexdigest()
        for index, group in enumerate(groups)
        for adjustment in group
    }


def request_identity(adjustment: str, symbol: str = "TEST") -> dict:
    return {
        "symbol": symbol,
        "adjustment": adjustment,
        "feed": "sip",
        "timeframe": "1Day",
        "request_from": "2016-01-01T00:00:00Z",
        "request_to": REQUEST_END_TIMESTAMP,
        "asof": "2024-01-17",
    }


def adjustment_path(repository_root: Path, adjustment: str, symbol: str = "TEST") -> Path:
    return repository_root / "data" / "end_of_day" / symbol / (adjustment.replace(",", "_") + ".csv.gz")


def adaptive_plan(symbol: str = "TEST") -> EndOfDayDownloadPlan:
    return EndOfDayDownloadPlan(
        symbol=symbol,
        adjustment_modes=ALPACA_ADJUSTMENT_MODES,
        force_authenticated_replacement=False,
        repair_orphaned_download_pair=False,
        adaptive_adjustment_downloads=True,
    )


class AdaptiveDecisionTests(unittest.TestCase):
    def test_all_observed_patterns_follow_the_expected_minimal_tree(self) -> None:
        for expected_pattern, groups in PATTERN_GROUPS.items():
            with self.subTest(pattern=expected_pattern):
                available_sha256s = adjustment_sha256s(groups)
                observed_sha256s = {}
                downloaded_adjustments = []

                while True:
                    decision = decide_adaptive_adjustment_downloads(observed_sha256s)
                    if decision.complete:
                        break
                    self.assertTrue(decision.next_adjustments)
                    for adjustment in decision.next_adjustments:
                        self.assertNotIn(adjustment, observed_sha256s)
                        observed_sha256s[adjustment] = available_sha256s[adjustment]
                        downloaded_adjustments.append(adjustment)

                self.assertEqual(expected_pattern, decision.pattern)
                self.assertEqual(
                    EXPECTED_DOWNLOAD_COUNTS[expected_pattern],
                    len(downloaded_adjustments),
                )
                for adjustment in ALPACA_ADJUSTMENT_MODES:
                    source_adjustment = decision.source_adjustment_for(adjustment)
                    self.assertIn(source_adjustment, observed_sha256s)
                    self.assertEqual(
                        available_sha256s[adjustment],
                        observed_sha256s[source_adjustment],
                    )

    def test_unseen_detectable_pattern_falls_back_to_all_eight(self) -> None:
        labels = {
            "raw": "a",
            "split": "a",
            "dividend": "a",
            "spin-off": "c",
            "split,dividend": "d",
            "split,spin-off": "e",
            "dividend,spin-off": "f",
            "all": "b",
        }
        available_sha256s = {
            adjustment: hashlib.sha256(label.encode()).hexdigest() for adjustment, label in labels.items()
        }
        observed_sha256s = {}
        downloaded_adjustments = []

        while True:
            decision = decide_adaptive_adjustment_downloads(observed_sha256s)
            if decision.complete:
                break
            for adjustment in decision.next_adjustments:
                observed_sha256s[adjustment] = available_sha256s[adjustment]
                downloaded_adjustments.append(adjustment)

        self.assertEqual("unseen_full_fallback", decision.pattern)
        self.assertTrue(decision.fallback_used)
        self.assertEqual(set(ALPACA_ADJUSTMENT_MODES), set(downloaded_adjustments))
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            self.assertEqual(
                available_sha256s[adjustment],
                observed_sha256s[decision.source_adjustment_for(adjustment)],
            )

    def test_cli_enables_adaptive_mode_and_rejects_conflicting_modes(self) -> None:
        cli_arguments = parse_command_line_arguments(["--symbol", "TEST", "--adaptive-adjustments"])
        plan = end_of_day_download_plan_from_command_line(cli_arguments)
        self.assertTrue(plan.adaptive_adjustment_downloads)
        self.assertEqual(ALPACA_ADJUSTMENT_MODES, plan.adjustment_modes)

        for conflicting_arguments in (
            ("--adjustment", "raw"),
            ("--force",),
            ("--repair",),
        ):
            with self.subTest(conflicting_arguments=conflicting_arguments):
                cli_arguments = parse_command_line_arguments(
                    ["--symbol", "TEST", "--adaptive-adjustments", *conflicting_arguments]
                )
                with self.assertRaises(SystemExit):
                    end_of_day_download_plan_from_command_line(cli_arguments)


class AdaptiveDownloadIntegrationTests(unittest.TestCase):
    def test_exact_alpaca_invalid_symbol_response_is_a_typed_known_error(self) -> None:
        http_error = urllib.error.HTTPError(
            "https://data.alpaca.markets/v2/stocks/bars",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"message":"invalid symbol: AMR1"}'),
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch(
                "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                side_effect=http_error,
            ):
                with self.assertRaises(AlpacaInvalidTickerCodeError) as captured:
                    run_end_of_day_download_stage(
                        adaptive_plan("AMR1"),
                        repository_root=Path(temporary_directory),
                        environment={
                            "ALPACA_API_KEY_ID": "key",
                            "ALPACA_API_SECRET_KEY": "secret",
                        },
                        request_end_timestamp=REQUEST_END_TIMESTAMP,
                    )

        self.assertEqual("invalid_ticker_code", captured.exception.reason_code)
        self.assertEqual("AMR1", captured.exception.symbol)
        self.assertEqual(400, captured.exception.http_status)

    def test_zero_row_response_remains_a_recorded_failure_and_does_not_abort_batch(self) -> None:
        requested_adjustments = []

        def request_no_bars(
            symbol,
            adjustment,
            api_key_id,
            api_secret_key,
            request_end,
            rate_limiter,
        ):
            requested_adjustments.append(adjustment)
            return []

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with (
                patch(
                    "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                    side_effect=request_no_bars,
                ),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                result = run_end_of_day_download_stage(
                    adaptive_plan(),
                    repository_root=repository_root,
                    environment={
                        "ALPACA_API_KEY_ID": "key",
                        "ALPACA_API_SECRET_KEY": "secret",
                    },
                    request_end_timestamp=REQUEST_END_TIMESTAMP,
                )

            self.assertEqual(["raw", "all"], requested_adjustments)
            self.assertEqual(8, result.pricing_data_integrity_failure_count)
            paths = [adjustment_path(repository_root, adjustment) for adjustment in ALPACA_ADJUSTMENT_MODES]
            self.assertEqual(1, len({path.stat().st_ino for path in paths}))
            for adjustment, path in zip(ALPACA_ADJUSTMENT_MODES, paths, strict=True):
                inspection = inspect_cached_download(path, request_identity(adjustment))
                self.assertEqual(
                    RECORDED_PRICING_DATA_FAILURE_ACTION,
                    inspection.required_action,
                )

    def test_all_equal_downloads_only_raw_and_all_then_publishes_eight_hard_links(self) -> None:
        requested_adjustments = []

        def request_bars(symbol, adjustment, api_key_id, api_secret_key, request_end, rate_limiter):
            self.assertEqual("TEST", symbol)
            self.assertEqual(("key", "secret"), (api_key_id, api_secret_key))
            self.assertEqual(REQUEST_END_TIMESTAMP, request_end)
            self.assertIsNotNone(rate_limiter)
            requested_adjustments.append(adjustment)
            return fixture_bars()

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with (
                patch(
                    "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                    side_effect=request_bars,
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                result = run_end_of_day_download_stage(
                    adaptive_plan(),
                    repository_root=repository_root,
                    environment={
                        "ALPACA_API_KEY_ID": "key",
                        "ALPACA_API_SECRET_KEY": "secret",
                    },
                    request_end_timestamp=REQUEST_END_TIMESTAMP,
                )

            self.assertEqual(["raw", "all"], requested_adjustments)
            self.assertEqual(ALPACA_ADJUSTMENT_MODES, result.requested_adjustment_modes)
            self.assertFalse(result.has_pricing_data_integrity_failures)

            paths = [adjustment_path(repository_root, adjustment) for adjustment in ALPACA_ADJUSTMENT_MODES]
            self.assertEqual(1, len({path.stat().st_ino for path in paths}))
            self.assertGreaterEqual(paths[0].stat().st_nlink, 8)
            for adjustment, path in zip(ALPACA_ADJUSTMENT_MODES, paths, strict=True):
                manifest = json.loads(download_manifest_path(path).read_text(encoding="utf-8"))
                expected_boundary = (
                    ALPACA_DOWNLOAD_BOUNDARY
                    if adjustment in {"raw", "all"}
                    else EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY
                )
                self.assertEqual(expected_boundary, manifest["boundary"])
                self.assertEqual(
                    "cached",
                    inspect_cached_download(path, request_identity(adjustment)).required_action,
                )

            with (
                patch(
                    "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                    side_effect=AssertionError("a resumed adaptive set must not use the network"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                resumed_result = run_end_of_day_download_stage(
                    adaptive_plan(),
                    repository_root=repository_root,
                    environment={},
                    request_end_timestamp=REQUEST_END_TIMESTAMP,
                )
            self.assertFalse(resumed_result.has_pricing_data_integrity_failures)

    def test_existing_direct_downloads_are_compacted_without_becoming_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            paths = []
            for adjustment in ALPACA_ADJUSTMENT_MODES:
                path = adjustment_path(repository_root, adjustment)
                validate_and_publish_alpaca_download(
                    path,
                    fixture_bars(),
                    request_identity(adjustment),
                )
                paths.append(path)
            self.assertEqual(8, len({path.stat().st_ino for path in paths}))

            with (
                patch(
                    "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                    side_effect=AssertionError("complete direct downloads must not use the network"),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                run_end_of_day_download_stage(
                    adaptive_plan(),
                    repository_root=repository_root,
                    environment={},
                    request_end_timestamp=REQUEST_END_TIMESTAMP,
                )

            self.assertEqual(1, len({path.stat().st_ino for path in paths}))
            for path in paths:
                manifest = json.loads(download_manifest_path(path).read_text(encoding="utf-8"))
                self.assertEqual(ALPACA_DOWNLOAD_BOUNDARY, manifest["boundary"])

    def test_alias_rejects_a_changed_direct_evidence_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with (
                patch(
                    "libs.download_alpaca_end_of_day_bars.command_line.request_alpaca_daily_bars",
                    return_value=fixture_bars(),
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                run_end_of_day_download_stage(
                    adaptive_plan(),
                    repository_root=repository_root,
                    environment={
                        "ALPACA_API_KEY_ID": "key",
                        "ALPACA_API_SECRET_KEY": "secret",
                    },
                    request_end_timestamp=REQUEST_END_TIMESTAMP,
                )

            raw_manifest_path = download_manifest_path(adjustment_path(repository_root, "raw"))
            raw_manifest = json.loads(raw_manifest_path.read_text(encoding="utf-8"))
            raw_manifest["unexpected_change"] = True
            raw_manifest_path.write_text(json.dumps(raw_manifest) + "\n", encoding="utf-8")

            split_inspection = inspect_cached_download(
                adjustment_path(repository_root, "split"),
                request_identity("split"),
            )
            self.assertEqual("invalid_frozen_download_boundary", split_inspection.required_action)
            self.assertIn("manifest SHA-256", split_inspection.diagnostic)


if __name__ == "__main__":
    unittest.main()
