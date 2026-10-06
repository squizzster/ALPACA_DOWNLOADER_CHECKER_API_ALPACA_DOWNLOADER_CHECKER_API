import re

REPORT_VERSION = 1

ASSET_API_BASE_URL = "https://paper-api.alpaca.markets/v2/assets"
CORPORATE_ACTIONS_API_URL = "https://data.alpaca.markets/v1/corporate-actions"
EARLIEST_BAR_DATE = "2016-01-01"

REQUESTS_PER_MINUTE = 180
MIN_REQUEST_INTERVAL_SECONDS = 60 / REQUESTS_PER_MINUTE
MAX_NAME_CHANGE_SYMBOLS = 100

SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]*")
CUSIP_FIELD_PATTERN = re.compile(r"(?:^|_)cusip$")
SYMBOL_FIELD_PATTERN = re.compile(r"(?:^|_)symbol$")

ACTION_TYPE_BY_RESPONSE_KEY = {
    "capital_gains_distributions": "capital_gains_distribution",
    "cash_dividends": "cash_dividend",
    "cash_mergers": "cash_merger",
    "forward_splits": "forward_split",
    "name_changes": "name_change",
    "partial_calls": "partial_call",
    "redemptions": "redemption",
    "reorganizations": "reorganization",
    "reverse_splits": "reverse_split",
    "rights_distributions": "rights_distribution",
    "spin_offs": "spin_off",
    "stock_and_cash_mergers": "stock_and_cash_merger",
    "stock_dividends": "stock_dividend",
    "stock_mergers": "stock_merger",
    "unit_splits": "unit_split",
    "worthless_removals": "worthless_removal",
}
