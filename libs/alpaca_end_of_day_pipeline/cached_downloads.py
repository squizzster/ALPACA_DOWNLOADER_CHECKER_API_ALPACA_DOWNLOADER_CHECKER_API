import json
from dataclasses import dataclass
from pathlib import Path

from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import (
    download_manifest_path,
    integrity_failure_manifest_path,
    pipeline_receipt_manifest_path,
)
from .contracts import (
    ALPACA_DOWNLOAD_BOUNDARY,
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY,
    EMPIRICAL_ADJUSTMENT_PATTERNS,
    EMPIRICAL_ADJUSTMENT_POLICY,
    INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
    MANIFEST_SCHEMA_VERSION,
    PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
)
from .daily_bar_validation import read_frozen_alpaca_daily_bars, validate_end_of_day_bars
from .errors import AlpacaDownloadIntegrityError, EndOfDayPipelineError
from .pipeline_version import PIPELINE_VERSION, validate_pipeline_change_log
from .symbol_manifests import read_integrity_failures, read_pipeline_receipts, record_validation_result


@dataclass(frozen=True)
class CachedDownloadInspection:
    required_action: str
    download_manifest: dict | None = None
    diagnostic: str | None = None


def _read_manifest_object(path: Path, description: str) -> dict:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EndOfDayPipelineError(f"could not read {description}: {error}") from error
    if not isinstance(document, dict):
        raise EndOfDayPipelineError(f"{description} is not an object")
    return document


def _empirical_alias_diagnostic(download_path: Path, download_manifest: dict) -> str | None:
    equivalence = download_manifest.get("empirical_equivalence")
    storage = download_manifest.get("storage")
    if not isinstance(equivalence, dict) or not isinstance(storage, dict):
        return "empirical alias has no equivalence or storage evidence"
    source_filename = equivalence.get("source_file")
    source_manifest_filename = equivalence.get("source_manifest")
    source_adjustment = equivalence.get("source_adjustment")
    if (
        not isinstance(source_filename, str)
        or Path(source_filename).name != source_filename
        or not source_filename.endswith(".csv.gz")
        or not isinstance(source_manifest_filename, str)
        or source_manifest_filename != source_filename + ".manifest.json"
        or not isinstance(source_adjustment, str)
        or not source_adjustment
        or source_filename != source_adjustment.replace(",", "_") + ".csv.gz"
    ):
        return "empirical alias has unsafe or invalid source evidence"
    if storage.get("kind") != "hard_link" or storage.get("canonical_file") != source_filename:
        return "empirical alias storage does not identify its source hard link"
    if equivalence.get("policy") != EMPIRICAL_ADJUSTMENT_POLICY:
        return "empirical alias has an unsupported policy identity"
    if equivalence.get("pattern") not in EMPIRICAL_ADJUSTMENT_PATTERNS:
        return "empirical alias has an unsupported pattern identity"

    source_path = download_path.parent / source_filename
    source_manifest_path = download_path.parent / source_manifest_filename
    try:
        source_manifest = _read_manifest_object(source_manifest_path, "empirical alias source manifest")
        source_stat = source_path.stat()
        files_share_inode = source_path.samefile(download_path)
    except (OSError, EndOfDayPipelineError) as error:
        return str(error)
    if not files_share_inode:
        return "empirical alias does not share its source inode"
    if calculate_file_sha256(source_manifest_path) != equivalence.get("source_manifest_sha256"):
        return "empirical alias source manifest SHA-256 does not match"
    if (
        source_manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY
        or source_manifest.get("file") != source_filename
        or source_manifest.get("adjustment") != source_adjustment
        or source_manifest.get("sha256") != download_manifest.get("sha256")
        or source_manifest.get("bytes") != source_stat.st_size
        or source_manifest.get("mtime_ns") != source_stat.st_mtime_ns
        or source_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY) is not True
    ):
        return "empirical alias source is not an authenticated direct download boundary"
    for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS:
        if field_name != "adjustment" and source_manifest.get(field_name) != download_manifest.get(field_name):
            return f"empirical alias source has inconsistent {field_name}"

    observations = equivalence.get("observed_downloads")
    if not isinstance(observations, list) or len(observations) < 2:
        return "empirical alias has insufficient observed-download evidence"
    observed_adjustments = set()
    source_was_observed = False
    for observation in observations:
        if not isinstance(observation, dict):
            return "empirical alias has malformed observed-download evidence"
        observed_filename = observation.get("file")
        observed_manifest_filename = observation.get("manifest")
        observed_adjustment = observation.get("adjustment")
        if (
            not isinstance(observed_filename, str)
            or Path(observed_filename).name != observed_filename
            or not observed_filename.endswith(".csv.gz")
            or not isinstance(observed_manifest_filename, str)
            or observed_manifest_filename != observed_filename + ".manifest.json"
            or not isinstance(observed_adjustment, str)
            or not observed_adjustment
            or observed_filename != observed_adjustment.replace(",", "_") + ".csv.gz"
            or observed_adjustment in observed_adjustments
        ):
            return "empirical alias has invalid observed-download identity"
        observed_adjustments.add(observed_adjustment)
        observed_path = download_path.parent / observed_filename
        observed_manifest_path = download_path.parent / observed_manifest_filename
        try:
            observed_manifest = _read_manifest_object(
                observed_manifest_path,
                "empirical alias observation manifest",
            )
            observed_stat = observed_path.stat()
        except (OSError, EndOfDayPipelineError) as error:
            return str(error)
        if (
            calculate_file_sha256(observed_manifest_path) != observation.get("manifest_sha256")
            or observed_manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY
            or observed_manifest.get("file") != observed_filename
            or observed_manifest.get("adjustment") != observed_adjustment
            or observed_manifest.get("sha256") != observation.get("sha256")
            or observed_manifest.get("bytes") != observed_stat.st_size
            or observed_manifest.get("mtime_ns") != observed_stat.st_mtime_ns
            or observed_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY) is not True
        ):
            return "empirical alias observation is not an authenticated direct download boundary"
        for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS:
            if field_name != "adjustment" and observed_manifest.get(field_name) != download_manifest.get(
                field_name
            ):
                return f"empirical alias observation has inconsistent {field_name}"
        source_was_observed = source_was_observed or observed_filename == source_filename
    if not source_was_observed:
        return "empirical alias source is absent from its observed-download evidence"
    return None


def inspect_cached_download(download_path: Path | str, request_identity: dict) -> CachedDownloadInspection:
    """Respect download ownership, then choose cached use, local validation, or network work."""
    validate_pipeline_change_log()
    download_path = Path(download_path)
    sidecar_manifest_path = download_manifest_path(download_path)
    try:
        download_file_stat = download_path.stat()
    except FileNotFoundError:
        download_file_stat = None
    except OSError as download_stat_error:
        return CachedDownloadInspection(
            required_action="error",
            diagnostic=f"could not stat data file: {download_stat_error}",
        )
    try:
        sidecar_manifest_stat = sidecar_manifest_path.stat()
    except FileNotFoundError:
        sidecar_manifest_stat = None
    except OSError as manifest_stat_error:
        return CachedDownloadInspection(
            required_action="error",
            diagnostic=f"could not stat manifest: {manifest_stat_error}",
        )

    if download_file_stat is None and sidecar_manifest_stat is None:
        return CachedDownloadInspection(
            required_action="download",
            diagnostic="data and manifest are absent",
        )
    if download_file_stat is None:
        return CachedDownloadInspection(
            required_action="repair_required",
            diagnostic="data file is missing while its manifest exists",
        )
    if sidecar_manifest_stat is None:
        return CachedDownloadInspection(
            required_action="repair_required",
            diagnostic="manifest is missing while its data file exists",
        )

    try:
        with sidecar_manifest_path.open(encoding="utf-8") as sidecar_manifest_file:
            download_manifest = json.load(sidecar_manifest_file)
    except (OSError, json.JSONDecodeError) as manifest_load_error:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic=f"invalid manifest: {manifest_load_error}",
        )

    if not isinstance(download_manifest, dict):
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest is not an object",
        )
    manifest_schema_version = download_manifest.get("manifest_version", MANIFEST_SCHEMA_VERSION)
    if manifest_schema_version != MANIFEST_SCHEMA_VERSION:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest schema does not match",
        )
    publication_sha256_was_verified = download_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY)
    if publication_sha256_was_verified is not True:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest does not record publication-time SHA-256 verification",
        )
    if download_manifest.get("file") != download_path.name:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest filename does not match",
        )
    if any(
        download_manifest.get(field_name) != request_identity.get(field_name)
        for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS
    ):
        return CachedDownloadInspection(
            required_action="refresh_required",
            download_manifest=download_manifest,
            diagnostic="request identity differs from the authenticated download",
        )

    recorded_download_sha256 = download_manifest.get("sha256")
    if not isinstance(recorded_download_sha256, str) or len(recorded_download_sha256) != 64:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid SHA-256 in manifest",
        )
    try:
        int(recorded_download_sha256, 16)
    except ValueError:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid SHA-256 in manifest",
        )

    recorded_row_count = download_manifest.get("rows")
    if type(recorded_row_count) is not int or recorded_row_count < 0:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid row count in manifest",
        )
    if recorded_row_count == 0:
        coverage_is_valid = download_manifest.get("from") is None and download_manifest.get("to") is None
    else:
        coverage_is_valid = bool(download_manifest.get("from") and download_manifest.get("to"))
    if not coverage_is_valid:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest row count and date coverage are inconsistent",
        )

    if (
        download_manifest.get("bytes") != download_file_stat.st_size
        or download_manifest.get("mtime_ns") != download_file_stat.st_mtime_ns
    ):
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            download_manifest=download_manifest,
            diagnostic="download file stat no longer matches its authenticated manifest",
        )

    boundary = download_manifest.get("boundary")
    if boundary == EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY:
        alias_diagnostic = _empirical_alias_diagnostic(download_path, download_manifest)
        if alias_diagnostic is not None:
            return CachedDownloadInspection(
                required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
                download_manifest=download_manifest,
                diagnostic=alias_diagnostic,
            )
    elif boundary != ALPACA_DOWNLOAD_BOUNDARY:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            download_manifest=download_manifest,
            diagnostic="download manifest has an unsupported boundary",
        )

    try:
        receipt_manifest_state = read_pipeline_receipts(
            pipeline_receipt_manifest_path(download_path), request_identity["symbol"]
        )
    except EndOfDayPipelineError as receipt_manifest_error:
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=str(receipt_manifest_error),
        )

    if receipt_manifest_state is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="pipeline manifest is absent",
        )
    receipt_pipeline_version, receipts_by_data_filename = receipt_manifest_state
    if receipt_pipeline_version != PIPELINE_VERSION:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic=(f"receipt manifest pipeline v{receipt_pipeline_version} != v{PIPELINE_VERSION}"),
        )
    matching_receipt = receipts_by_data_filename.get(download_path.name)
    if matching_receipt is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="no pipeline receipt exists for this download",
        )
    if matching_receipt.get("download_sha256") != recorded_download_sha256:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="the pipeline receipt belongs to a different download SHA-256",
        )
    try:
        failure_manifest_state = read_integrity_failures(
            integrity_failure_manifest_path(download_path), request_identity["symbol"]
        )
    except EndOfDayPipelineError as failure_manifest_error:
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=str(failure_manifest_error),
        )
    if failure_manifest_state is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="integrity failure manifest is absent",
        )
    failure_pipeline_version, failures_by_data_filename = failure_manifest_state
    if failure_pipeline_version != PIPELINE_VERSION:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic=(f"failure manifest pipeline v{failure_pipeline_version} != v{PIPELINE_VERSION}"),
        )
    matching_failure = failures_by_data_filename.get(download_path.name)
    if matching_failure is None:
        return CachedDownloadInspection(required_action="cached", download_manifest=download_manifest)
    if (
        matching_failure.get("download_sha256") != recorded_download_sha256
        or matching_failure.get("pipeline_version") != PIPELINE_VERSION
    ):
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=f"stale failure entry in {integrity_failure_manifest_path(download_path)}",
        )
    return CachedDownloadInspection(
        required_action=RECORDED_PRICING_DATA_FAILURE_ACTION,
        download_manifest=download_manifest,
        diagnostic=f"failures are recorded in {integrity_failure_manifest_path(download_path)}",
    )


def authenticate_frozen_download(download_path: Path | str, download_manifest: dict) -> str:
    """Authenticate a gzip against its download-boundary sidecar."""
    recorded_download_sha256 = download_manifest.get("sha256")
    observed_download_sha256 = calculate_file_sha256(download_path)
    if observed_download_sha256 != recorded_download_sha256:
        raise AlpacaDownloadIntegrityError(
            f"{download_path}: Alpaca download SHA-256 mismatch "
            f"(manifest={recorded_download_sha256}, actual={observed_download_sha256}); no pricing/data "
            "integrity result was published and this data is not eligible for "
            "repair; aborting without replacing either boundary file"
        )
    return observed_download_sha256


def authenticate_and_revalidate_cached_download(
    download_path: Path | str, request_identity: dict, download_manifest: dict
) -> dict:
    """Authenticate, then validate a download without writing either download file."""
    validate_pipeline_change_log()
    download_path = Path(download_path)
    authenticate_frozen_download(download_path, download_manifest)
    frozen_alpaca_bars = read_frozen_alpaca_daily_bars(download_path)
    validation_result = validate_end_of_day_bars(frozen_alpaca_bars, request_identity)
    return record_validation_result(
        download_path,
        request_identity,
        download_manifest,
        validation_result,
    )
