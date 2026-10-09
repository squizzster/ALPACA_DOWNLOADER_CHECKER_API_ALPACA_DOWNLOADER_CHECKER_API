REQUESTS_PER_MINUTE = 199
MIN_REQUEST_INTERVAL_SECONDS = 60 / REQUESTS_PER_MINUTE
EARLIEST_REQUEST_TIMESTAMP = "2016-01-01T00:00:00Z"
ALPACA_DATA_FEED = "sip"
ALPACA_BAR_TIMEFRAME = "1Day"

ALPACA_ADJUSTMENT_MODES = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)
