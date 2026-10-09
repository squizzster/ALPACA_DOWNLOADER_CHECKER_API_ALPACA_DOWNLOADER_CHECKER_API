import argparse
import json
import os
import re
import sys
import urllib.error
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping

from libs.alpaca_end_of_day_pipeline import (
    ALPACA_DOWNLOAD_BOUNDARY,
    EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY,
    EMPIRICAL_ADJUSTMENT_POLICY,
    INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
    PIPELINE_VERSION,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    PricingDataIntegrityError,
    authenticate_and_revalidate_cached_download,
    authenticate_frozen_download,
    deduplicate_authenticated_download_storage,
    download_manifest_path,
    inspect_cached_download,
    publish_empirical_adjustment_alias,
    validate_and_publish_alpaca_download,
    validate_pipeline_change_log,
)

from .adaptive_adjustments import decide_adaptive_adjustment_downloads
from .alpaca_http import AlpacaRequestRateLimiter, request_alpaca_daily_bars
from .contracts import EndOfDayDownloadPlan, EndOfDayDownloadStageResult
from .errors import AlpacaInvalidTickerCodeError
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


def _response_is_exact_invalid_ticker_code(
    symbol: str,
    http_status: int,
    response_body: str,
) -> bool:
    if http_status != 400:
        return False
    try:
        response_document = json.loads(response_body)
    except json.JSONDecodeError:
        return False
    return isinstance(response_document, dict) and response_document.get("message") == (
        f"invalid symbol: {symbol}"
    )


def _request_end_timestamp_argument(value: str) -> str:
    try:
        request_end_date = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an ISO date in YYYY-MM-DD form") from error
    return f"{request_end_date.isoformat()}T00:00:00Z"


def create_end_of_day_download_argument_parser(
    *,
    description: str = "Download daily Alpaca bars as compressed CSV.",
    repeatable_symbols: bool = False,
) -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(description=description)
    if repeatable_symbols:
        symbol_source_arguments = argument_parser.add_mutually_exclusive_group(required=True)
        symbol_source_arguments.add_argument(
            "--symbol",
            "-s",
            dest="symbols",
            action="append",
            help="stock ticker; repeat for multiple tickers, for example --symbol DELL -s FB",
        )
        symbol_source_arguments.add_argument(
            "--file",
            "--symbols-file",
            dest="symbol_file",
            metavar="PATH",
            help="read tickers from PATH, one per nonblank line; duplicates are processed once",
        )
    else:
        argument_parser.add_argument(
            "--symbol",
            "-s",
            dest="symbol",
            required=True,
            help="stock ticker, for example DELL",
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
    argument_parser.add_argument(
        "--adaptive-adjustments",
        action="store_true",
        help=(
            "infer all eight logical adjustment views from the empirically minimal "
            "download sequence, falling back to all eight when observations are unfamiliar"
        ),
    )
    argument_parser.add_argument(
        "--request-end",
        dest="request_end_timestamp",
        type=_request_end_timestamp_argument,
        metavar="YYYY-MM-DD",
        help=("exclusive UTC request boundary; defaults to the UTC date frozen when the command starts"),
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
    adaptive_adjustment_downloads = getattr(cli_arguments, "adaptive_adjustments", False)
    if adaptive_adjustment_downloads and cli_arguments.adjustment:
        raise SystemExit("--adaptive-adjustments cannot be combined with --adjustment")
    if adaptive_adjustment_downloads and (cli_arguments.force or cli_arguments.repair_orphaned_download_pair):
        raise SystemExit("--adaptive-adjustments cannot be combined with --force or --repair")
    symbol_file = getattr(cli_arguments, "symbol_file", None)
    supplied_symbols = getattr(cli_arguments, "symbols", None)
    symbols_from_file = symbol_file is not None
    if symbols_from_file:
        symbol_file_path = Path(symbol_file)
        try:
            symbol_file_lines = symbol_file_path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError) as file_error:
            raise SystemExit(
                f"unable to read symbol file {str(symbol_file_path)!r}: {file_error}"
            ) from file_error
        supplied_symbols_with_locations = [
            (line.strip(), f"{symbol_file_path}:{line_number}")
            for line_number, line in enumerate(symbol_file_lines, start=1)
            if line.strip()
        ]
        if not supplied_symbols_with_locations:
            raise SystemExit(f"symbol file contains no symbols: {str(symbol_file_path)!r}")
    else:
        if supplied_symbols is None:
            supplied_symbols = [cli_arguments.symbol]
        supplied_symbols_with_locations = [(symbol, None) for symbol in supplied_symbols]

    supplied_and_normalized_symbols = tuple(
        (symbol, symbol.upper(), location) for symbol, location in supplied_symbols_with_locations
    )
    for supplied_symbol, normalized_symbol, location in supplied_and_normalized_symbols:
        if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", normalized_symbol):
            if location is not None:
                raise SystemExit(f"invalid symbol at {location}: {supplied_symbol!r}")
            raise SystemExit(f"invalid symbol: {supplied_symbol!r}")
    normalized_symbols = tuple(
        dict.fromkeys(normalized_symbol for _, normalized_symbol, _ in supplied_and_normalized_symbols)
    )
    requested_adjustment_modes = tuple(dict.fromkeys(cli_arguments.adjustment or ALPACA_ADJUSTMENT_MODES))
    return tuple(
        EndOfDayDownloadPlan(
            symbol=normalized_symbol,
            adjustment_modes=requested_adjustment_modes,
            force_authenticated_replacement=cli_arguments.force,
            repair_orphaned_download_pair=cli_arguments.repair_orphaned_download_pair,
            adaptive_adjustment_downloads=adaptive_adjustment_downloads,
        )
        for normalized_symbol in normalized_symbols
    )


def _run_standard_end_of_day_download_stage(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    request_rate_limiter: AlpacaRequestRateLimiter | None = None,
    storage_deduplication_candidates: tuple[Path, ...] = (),
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
            if _response_is_exact_invalid_ticker_code(
                normalized_symbol,
                http_error.code,
                response_body,
            ):
                raise AlpacaInvalidTickerCodeError(
                    normalized_symbol,
                    adjustment_mode,
                    http_error.code,
                    response_body,
                ) from http_error
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
                storage_deduplication_candidates=storage_deduplication_candidates,
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


def _adjustment_download_path(
    symbol_download_directory: Path,
    adjustment_mode: str,
) -> Path:
    return symbol_download_directory / (adjustment_mode.replace(",", "_") + ".csv.gz")


def _adjustment_request_identity(
    symbol: str,
    adjustment_mode: str,
    request_end_timestamp: str,
) -> dict:
    return {
        "symbol": symbol,
        "adjustment": adjustment_mode,
        "feed": ALPACA_DATA_FEED,
        "timeframe": ALPACA_BAR_TIMEFRAME,
        "request_from": EARLIEST_REQUEST_TIMESTAMP,
        "request_to": request_end_timestamp,
        "asof": request_end_timestamp[:10],
    }


def _read_download_manifest(download_path: Path) -> dict:
    manifest_path = download_manifest_path(download_path)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"could not read {manifest_path}: {error}") from error
    if not isinstance(manifest, dict):
        raise RuntimeError(f"download manifest is not an object: {manifest_path}")
    return manifest


def _single_adjustment_plan(symbol: str, adjustment_mode: str) -> EndOfDayDownloadPlan:
    return EndOfDayDownloadPlan(
        symbol=symbol,
        adjustment_modes=(adjustment_mode,),
        force_authenticated_replacement=False,
        repair_orphaned_download_pair=False,
        adaptive_adjustment_downloads=False,
    )


def _run_adaptive_end_of_day_download_stage(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    request_rate_limiter: AlpacaRequestRateLimiter | None = None,
) -> EndOfDayDownloadStageResult:
    if download_plan.adjustment_modes != ALPACA_ADJUSTMENT_MODES:
        raise ValueError("adaptive adjustment downloads require all eight adjustment modes")
    if download_plan.force_authenticated_replacement or download_plan.repair_orphaned_download_pair:
        raise ValueError("adaptive adjustment downloads do not support --force or --repair")

    resolved_repository_root = Path.cwd() if repository_root is None else repository_root
    command_environment = os.environ if environment is None else environment
    frozen_request_end_timestamp = request_end_timestamp or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    rate_limiter = request_rate_limiter or AlpacaRequestRateLimiter()
    symbol = download_plan.symbol
    symbol_download_directory = resolved_repository_root / "data" / "end_of_day" / symbol
    symbol_download_directory.mkdir(parents=True, exist_ok=True)

    direct_downloads: dict[str, tuple[Path, dict]] = {}
    empirical_aliases: dict[str, tuple[Path, dict]] = {}
    observed_sha256_by_adjustment: dict[str, str] = {}

    # Authenticate every already-present logical view first. This makes a
    # partially completed adaptive run resumable without trusting filenames.
    for adjustment_mode in ALPACA_ADJUSTMENT_MODES:
        download_path = _adjustment_download_path(
            symbol_download_directory,
            adjustment_mode,
        )
        manifest_path = download_manifest_path(download_path)
        if download_path.exists() != manifest_path.exists():
            print(
                f"error: {download_path}: adaptive cache has an orphaned data/manifest pair; "
                "aborting without replacement",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if not download_path.exists():
            continue

        _run_standard_end_of_day_download_stage(
            _single_adjustment_plan(symbol, adjustment_mode),
            repository_root=resolved_repository_root,
            environment=command_environment,
            request_end_timestamp=frozen_request_end_timestamp,
            request_rate_limiter=rate_limiter,
        )
        manifest = _read_download_manifest(download_path)
        observed_sha256 = authenticate_frozen_download(download_path, manifest)
        if manifest.get("boundary") == ALPACA_DOWNLOAD_BOUNDARY:
            direct_downloads[adjustment_mode] = (download_path, manifest)
            observed_sha256_by_adjustment[adjustment_mode] = observed_sha256
        elif manifest.get("boundary") == EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY:
            empirical_aliases[adjustment_mode] = (download_path, manifest)
        else:
            raise RuntimeError(f"unsupported download boundary for adaptive processing: {download_path}")

    decision = decide_adaptive_adjustment_downloads(observed_sha256_by_adjustment)
    while not decision.complete:
        for adjustment_mode in decision.next_adjustments:
            if adjustment_mode in empirical_aliases:
                raise RuntimeError(
                    "an empirical alias occupies an adjustment that now requires a direct "
                    f"download: {adjustment_mode}"
                )
            if adjustment_mode in direct_downloads:
                raise RuntimeError(f"adaptive policy repeatedly requested observed adjustment {adjustment_mode}")
            _run_standard_end_of_day_download_stage(
                _single_adjustment_plan(symbol, adjustment_mode),
                repository_root=resolved_repository_root,
                environment=command_environment,
                request_end_timestamp=frozen_request_end_timestamp,
                request_rate_limiter=rate_limiter,
                storage_deduplication_candidates=tuple(path for path, _manifest in direct_downloads.values()),
            )
            download_path = _adjustment_download_path(
                symbol_download_directory,
                adjustment_mode,
            )
            manifest = _read_download_manifest(download_path)
            if manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY:
                raise RuntimeError(f"adaptive observation is not a direct Alpaca download: {download_path}")
            observed_sha256 = authenticate_frozen_download(download_path, manifest)
            direct_downloads[adjustment_mode] = (download_path, manifest)
            observed_sha256_by_adjustment[adjustment_mode] = observed_sha256
        decision = decide_adaptive_adjustment_downloads(observed_sha256_by_adjustment)

    if decision.pattern is None:
        raise RuntimeError("adaptive policy completed without identifying an equivalence pattern")

    # Direct manifests are evidence for every alias. Compact them before
    # publishing aliases, then freeze their exact manifest hashes in that evidence.
    if not empirical_aliases:
        storage_result = deduplicate_authenticated_download_storage(
            {
                adjustment_mode: download_path
                for adjustment_mode, (download_path, _manifest) in direct_downloads.items()
            }
        )
        if storage_result["linked_file_count"]:
            print(
                f"{symbol}: compacted {storage_result['linked_file_count']} authenticated "
                "duplicate download(s) into hard links, "
                f"reclaimed={storage_result['reclaimed_bytes']} bytes"
            )
        observed_sha256_by_adjustment = {}
        for adjustment_mode, (download_path, _manifest) in direct_downloads.items():
            refreshed_manifest = _read_download_manifest(download_path)
            observed_sha256_by_adjustment[adjustment_mode] = authenticate_frozen_download(
                download_path,
                refreshed_manifest,
            )
            direct_downloads[adjustment_mode] = (download_path, refreshed_manifest)
        decision = decide_adaptive_adjustment_downloads(observed_sha256_by_adjustment)
        if not decision.complete:
            raise RuntimeError("storage compaction changed the adaptive adjustment decision")

    direct_adjustments = set(direct_downloads)
    for adjustment_mode, (_alias_path, alias_manifest) in empirical_aliases.items():
        equivalence = alias_manifest.get("empirical_equivalence")
        expected_source_adjustment = decision.source_adjustment_for(adjustment_mode)
        observed_adjustments = (
            {
                observation.get("adjustment")
                for observation in equivalence.get("observed_downloads", [])
                if isinstance(observation, dict)
            }
            if isinstance(equivalence, dict)
            else set()
        )
        if (
            not isinstance(equivalence, dict)
            or equivalence.get("policy") != EMPIRICAL_ADJUSTMENT_POLICY
            or equivalence.get("pattern") != decision.pattern
            or equivalence.get("source_adjustment") != expected_source_adjustment
            or observed_adjustments != direct_adjustments
        ):
            raise RuntimeError(
                f"existing empirical alias does not match the current adaptive decision: {adjustment_mode}"
            )

    for adjustment_mode in ALPACA_ADJUSTMENT_MODES:
        if adjustment_mode in direct_downloads or adjustment_mode in empirical_aliases:
            continue
        source_adjustment = decision.source_adjustment_for(adjustment_mode)
        try:
            source_download_path, source_download_manifest = direct_downloads[source_adjustment]
        except KeyError as error:
            raise RuntimeError(
                f"adaptive policy source {source_adjustment!r} was not downloaded directly"
            ) from error
        alias_path = _adjustment_download_path(
            symbol_download_directory,
            adjustment_mode,
        )
        alias_manifest = publish_empirical_adjustment_alias(
            alias_path,
            source_download_path,
            source_download_manifest,
            _adjustment_request_identity(
                symbol,
                adjustment_mode,
                frozen_request_end_timestamp,
            ),
            policy=EMPIRICAL_ADJUSTMENT_POLICY,
            pattern=decision.pattern,
            observed_downloads=direct_downloads,
        )
        empirical_aliases[adjustment_mode] = (alias_path, alias_manifest)
        print(
            f"{alias_path}: empirical {decision.pattern} hard link to "
            f"{source_download_path.name}, sha256={alias_manifest['sha256']}"
        )

    return _run_standard_end_of_day_download_stage(
        EndOfDayDownloadPlan(
            symbol=symbol,
            adjustment_modes=ALPACA_ADJUSTMENT_MODES,
            force_authenticated_replacement=False,
            repair_orphaned_download_pair=False,
            adaptive_adjustment_downloads=False,
        ),
        repository_root=resolved_repository_root,
        environment=command_environment,
        request_end_timestamp=frozen_request_end_timestamp,
        request_rate_limiter=rate_limiter,
    )


def run_end_of_day_download_stage(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    request_rate_limiter: AlpacaRequestRateLimiter | None = None,
) -> EndOfDayDownloadStageResult:
    if download_plan.adaptive_adjustment_downloads:
        return _run_adaptive_end_of_day_download_stage(
            download_plan,
            repository_root=repository_root,
            environment=environment,
            request_end_timestamp=request_end_timestamp,
            request_rate_limiter=request_rate_limiter,
        )
    return _run_standard_end_of_day_download_stage(
        download_plan,
        repository_root=repository_root,
        environment=environment,
        request_end_timestamp=request_end_timestamp,
        request_rate_limiter=request_rate_limiter,
    )


def run_end_of_day_download_command(
    cli_arguments: argparse.Namespace,
) -> EndOfDayDownloadStageResult:
    download_stage_result = run_end_of_day_download_stage(
        end_of_day_download_plan_from_command_line(cli_arguments),
        request_end_timestamp=getattr(cli_arguments, "request_end_timestamp", None),
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
