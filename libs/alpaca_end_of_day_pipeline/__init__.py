# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.
from .artifact_paths import (
    download_manifest_path,
    integrity_failure_manifest_path,
    pipeline_receipt_manifest_path,
)
from .cached_downloads import (
    CachedDownloadInspection,
    authenticate_and_revalidate_cached_download,
    authenticate_frozen_download,
    inspect_cached_download,
)
from .contracts import (
    CSV_COLUMN_ALPACA_FIELD_PAIRS,
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
    MANIFEST_SCHEMA_VERSION,
    PIPELINE_RECEIPT_MANIFEST_FILENAME,
    PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
)
from .daily_bar_validation import (
    EndOfDayValidationResult,
    read_frozen_alpaca_daily_bars,
    validate_end_of_day_bars,
)
from .download_publication import validate_and_publish_alpaca_download
from .errors import AlpacaDownloadIntegrityError, EndOfDayPipelineError, PricingDataIntegrityError
from .pipeline_version import PIPELINE_CHANGE_LOG_PATH, PIPELINE_VERSION, validate_pipeline_change_log

__all__ = [
    "AlpacaDownloadIntegrityError",
    "CSV_COLUMN_ALPACA_FIELD_PAIRS",
    "CachedDownloadInspection",
    "DOWNLOAD_REQUEST_IDENTITY_FIELDS",
    "EndOfDayPipelineError",
    "EndOfDayValidationResult",
    "INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION",
    "MANIFEST_SCHEMA_VERSION",
    "PIPELINE_CHANGE_LOG_PATH",
    "PIPELINE_RECEIPT_MANIFEST_FILENAME",
    "PIPELINE_VERSION",
    "PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY",
    "PricingDataIntegrityError",
    "RECORDED_PRICING_DATA_FAILURE_ACTION",
    "authenticate_and_revalidate_cached_download",
    "authenticate_frozen_download",
    "download_manifest_path",
    "inspect_cached_download",
    "integrity_failure_manifest_path",
    "pipeline_receipt_manifest_path",
    "read_frozen_alpaca_daily_bars",
    "validate_and_publish_alpaca_download",
    "validate_end_of_day_bars",
    "validate_pipeline_change_log",
]

# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.
