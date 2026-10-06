import argparse
import os
import re
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from libs.alpaca_end_of_day_pipeline import (
    INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
    PIPELINE_VERSION,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    PricingDataIntegrityError,
    authenticate_and_revalidate_cached_download,
    authenticate_frozen_download,
    inspect_cached_download,
    validate_and_publish_alpaca_download,
    validate_pipeline_change_log,
)

from .alpaca_http import AlpacaRequestRateLimiter, request_alpaca_daily_bars
from .contracts import EndOfDayDownloadPlan, EndOfDayDownloadStageResult
from .settings import (
    ALPACA_ADJUSTMENT_MODES,
    ALPACA_BAR_TIMEFRAME,
    ALPACA_DATA_FEED,
    EARLIEST_REQUEST_TIMESTAMP,
)


def _read_alpaca_credentials(environment: Mapping[str, str]) -> tuple[str, str]:
    api_key_id = environment.get("ALPACA_API_KEY_ID")
    api_secret_key = environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")
    return api_key_id, api_secret_key


def create_end_of_day_download_argument_parser(
    *,
    description: str = "Download daily Alpaca bars as compressed CSV.",
    repeatable_symbols: bool = False,
) -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(description=description)
    argument_parser.add_argument(
        "--symbol",
        "-s",
        dest="symbols" if repeatable_symbols else "symbol",
        action="append" if repeatable_symbols else "store",
        required=True,
        help=(
            "stock ticker; repeat for multiple tickers, for example --symbol DELL -s FB"
            if repeatable_symbols
            else "stock ticker, for example DELL"
        ),
    )
    argument_parser.add_argument(
        "--adjustment",
        action="append",
        choices=ALPACA_ADJUSTMENT_MODES,
        metavar="MODE",
        help=(
            "raw, split, dividend, spin-off, split,dividend, split,spin-off, "
            "dividend,spin-off, or all; repeat for multiple modes "
            "(default: every mode)"
        ),
    )
    replacement_mode_arguments = argument_parser.add_mutually_exclusive_group()
    replacement_mode_arguments.add_argument(
        "--force",
        action="store_true",
        help="replace a coherent, authenticated Alpaca download pair",
    )
    replacement_mode_arguments.add_argument(
        "--repair",
        dest="repair_orphaned_download_pair",
        action="store_true",
        help="redownload both boundary files when exactly one frozen download-pair file is missing",
    )
    return argument_parser


def parse_command_line_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    return create_end_of_day_download_argument_parser().parse_args(arguments)


def end_of_day_download_plan_from_command_line(
    cli_arguments: argparse.Namespace,
) -> EndOfDayDownloadPlan:
    download_plans = end_of_day_download_plans_from_command_line(cli_arguments)
    if len(download_plans) != 1:
        raise ValueError("a single-symbol command received multiple symbols")
    return download_plans[0]


def end_of_day_download_plans_from_command_line(
    cli_arguments: argparse.Namespace,
) -> tuple[EndOfDayDownloadPlan, ...]:
    supplied_symbols = getattr(cli_arguments, "symbols", None)
    if supplied_symbols is None:
        supplied_symbols = [cli_arguments.symbol]
    supplied_and_normalized_symbols = tuple((symbol, symbol.upper()) for symbol in supplied_symbols)
    for supplied_symbol, normalized_symbol in supplied_and_normalized_symbols:
        if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", normalized_symbol):
            raise SystemExit(f"invalid symbol: {supplied_symbol!r}")
    normalized_symbols = tuple(
        dict.fromkeys(normalized_symbol for _, normalized_symbol in supplied_and_normalized_symbols)
    )
    requested_adjustment_modes = tuple(dict.fromkeys(cli_arguments.adjustment or ALPACA_ADJUSTMENT_MODES))
    return tuple(
        EndOfDayDownloadPlan(
            symbol=normalized_symbol,
            adjustment_modes=requested_adjustment_modes,
            force_authenticated_replacement=cli_arguments.force,
            repair_orphaned_download_pair=cli_arguments.repair_orphaned_download_pair,
        )
        for normalized_symbol in normalized_symbols
    )


def run_end_of_day_download_stage(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    request_rate_limiter: AlpacaRequestRateLimiter | None = None,
) -> EndOfDayDownloadStageResult:
    validate_pipeline_change_log()
    resolved_repository_root = Path.cwd() if repository_root is None else repository_root
    command_environment = os.environ if environment is None else environment
    normalized_symbol = download_plan.symbol
    requested_adjustment_modes = download_plan.adjustment_modes
    request_end_timestamp = request_end_timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    request_asof_date = request_end_timestamp[:10]
    symbol_download_directory = resolved_repository_root / "data" / "end_of_day" / normalized_symbol
    symbol_download_directory.mkdir(parents=True, exist_ok=True)
    rate_limiter = request_rate_limiter or AlpacaRequestRateLimiter()
    api_key_id = None
    api_secret_key = None
    failed_integrity_artifact_count = 0

    for adjustment_mode in requested_adjustment_modes:
        download_filename = adjustment_mode.replace(",", "_") + ".csv.gz"
        download_path = symbol_download_directory / download_filename
        request_identity = {
            "symbol": normalized_symbol,
            "adjustment": adjustment_mode,
            "feed": ALPACA_DATA_FEED,
            "timeframe": ALPACA_BAR_TIMEFRAME,
            "request_from": EARLIEST_REQUEST_TIMESTAMP,
            "request_to": request_end_timestamp,
            "asof": request_asof_date,
        }
        cache_inspection = inspect_cached_download(download_path, request_identity)
        download_pair_is_orphaned = cache_inspection.required_action == "repair_required"

        if cache_inspection.required_action == "error":
            print(f"error: {download_path}: {cache_inspection.diagnostic}", file=sys.stderr)
            raise SystemExit(2)
        if cache_inspection.required_action == INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION:
            print(
                f"error: {download_path}: Alpaca download boundary failed: "
                f"{cache_inspection.diagnostic}; aborting without repair or replacement",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if download_pair_is_orphaned and not download_plan.repair_orphaned_download_pair:
            print(
                f"error: {download_path}: cache corruption: {cache_inspection.diagnostic}; "
                "use --repair to repair",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if download_plan.repair_orphaned_download_pair and not download_pair_is_orphaned:
            if cache_inspection.download_manifest is not None:
                authenticate_frozen_download(download_path, cache_inspection.download_manifest)
            print(
                f"error: {download_path}: --repair applies only when exactly one download "
                "boundary file is missing",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if (
            cache_inspection.required_action == "refresh_required"
            and not download_plan.force_authenticated_replacement
        ):
            print(
                f"error: {download_path}: {cache_inspection.diagnostic}; use --force to replace "
                "the Alpaca download boundary",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if (
            cache_inspection.required_action == RECORDED_PRICING_DATA_FAILURE_ACTION
            and not download_plan.force_authenticated_replacement
        ):
            print(
                f"error: {download_path}: pricing/data integrity failed: "
                f"{cache_inspection.diagnostic}; use --force to fetch a new Alpaca download",
                file=sys.stderr,
            )
            failed_integrity_artifact_count += 1
            continue

        if download_plan.force_authenticated_replacement and cache_inspection.download_manifest is not None:
            authenticate_frozen_download(download_path, cache_inspection.download_manifest)

        replacement_authorized = download_plan.force_authenticated_replacement or download_pair_is_orphaned

        if not replacement_authorized and cache_inspection.required_action == "cached":
            cached_download_manifest = cache_inspection.download_manifest
            if cached_download_manifest is None:
                raise RuntimeError("cache inspection returned no manifest")
            print(
                f"{download_path}: cached {cached_download_manifest['rows']} rows "
                f"({cached_download_manifest['from']} to {cached_download_manifest['to']}), "
                f"pipeline=v{PIPELINE_VERSION}"
            )
            continue

        if not replacement_authorized and cache_inspection.required_action == "repipeline":
            cached_download_manifest = cache_inspection.download_manifest
            if cached_download_manifest is None:
                raise RuntimeError("re-pipeline inspection returned no manifest")
            try:
                pipeline_receipt = authenticate_and_revalidate_cached_download(
                    download_path,
                    request_identity,
                    cached_download_manifest,
                )
            except PricingDataIntegrityError as integrity_error:
                print(f"error: {integrity_error}", file=sys.stderr)
                failed_integrity_artifact_count += 1
                continue
            print(
                f"{download_path}: validated {pipeline_receipt['rows_seen']} frozen rows, "
                f"pipeline=v{PIPELINE_VERSION}, "
                f"sha256={cached_download_manifest['sha256']}"
            )
            continue

        if download_pair_is_orphaned:
            print(f"{download_path}: repairing cache ({cache_inspection.diagnostic}); downloading")
        elif cache_inspection.diagnostic and cache_inspection.diagnostic != "data and manifest are absent":
            print(f"{download_path}: cache unusable ({cache_inspection.diagnostic}); downloading")

        if api_key_id is None or api_secret_key is None:
            api_key_id, api_secret_key = _read_alpaca_credentials(command_environment)

        try:
            downloaded_alpaca_bars = request_alpaca_daily_bars(
                normalized_symbol,
                adjustment_mode,
                api_key_id,
                api_secret_key,
                request_end_timestamp,
                rate_limiter,
            )
        except urllib.error.HTTPError as http_error:
            response_body = http_error.read().decode("utf-8", "replace")
            raise SystemExit(
                f"{adjustment_mode}: Alpaca returned HTTP {http_error.code}: {response_body}"
            ) from http_error
        except urllib.error.URLError as request_error:
            raise SystemExit(f"{adjustment_mode}: request failed: {request_error.reason}") from request_error

        try:
            published_download_manifest = validate_and_publish_alpaca_download(
                download_path,
                downloaded_alpaca_bars,
                request_identity,
                replacement_authorized=replacement_authorized,
            )
        except PricingDataIntegrityError as integrity_error:
            print(f"error: {integrity_error}", file=sys.stderr)
            failed_integrity_artifact_count += 1
            continue
        print(
            f"{download_path}: {published_download_manifest['rows']} rows "
            f"({published_download_manifest['from']} to {published_download_manifest['to']}), "
            f"sha256={published_download_manifest['sha256']}"
        )

    return EndOfDayDownloadStageResult(
        symbol=normalized_symbol,
        requested_adjustment_modes=requested_adjustment_modes,
        pricing_data_integrity_failure_count=failed_integrity_artifact_count,
    )


def run_end_of_day_download_command(
    cli_arguments: argparse.Namespace,
) -> EndOfDayDownloadStageResult:
    download_stage_result = run_end_of_day_download_stage(
        end_of_day_download_plan_from_command_line(cli_arguments)
    )
    if download_stage_result.has_pricing_data_integrity_failures:
        print(
            "error: "
            f"{download_stage_result.pricing_data_integrity_failure_count} "
            "artifact(s) failed pricing/data integrity",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return download_stage_result
