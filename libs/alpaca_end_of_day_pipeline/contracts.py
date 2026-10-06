MANIFEST_SCHEMA_VERSION = 1
PIPELINE_RECEIPT_MANIFEST_FILENAME = "pipeline.manifest.json"
INTEGRITY_FAILURE_MANIFEST_FILENAME = "integrity_failure.manifest.json"
PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY = "verified"

INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION = "invalid_frozen_download_boundary"
RECORDED_PRICING_DATA_FAILURE_ACTION = "recorded_pricing_data_failure"

CSV_COLUMN_ALPACA_FIELD_PAIRS = (
    ("timestamp", "t"),
    ("open", "o"),
    ("high", "h"),
    ("low", "l"),
    ("close", "c"),
    ("volume", "v"),
    ("trade_count", "n"),
    ("vwap", "vw"),
)

DOWNLOAD_REQUEST_IDENTITY_FIELDS = (
    "symbol",
    "adjustment",
    "feed",
    "timeframe",
    "request_from",
    "request_to",
    "asof",
)
