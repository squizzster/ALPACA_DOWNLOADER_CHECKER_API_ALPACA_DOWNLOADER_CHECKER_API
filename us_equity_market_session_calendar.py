"""Compatibility facade for the common US-equity market calendar library."""

from libs.common.us_equity_market_session_calendar import (
    US_MARKET_HOURS_FILE_PATH,
    ExpectedMarketSessionCoverage,
    MarketCalendarError,
    compare_observed_to_expected_sessions,
    expected_market_session_coverage,
    market_session_dates_sha256,
    us_market_hours_sha256,
)

__all__ = [
    "ExpectedMarketSessionCoverage",
    "MarketCalendarError",
    "US_MARKET_HOURS_FILE_PATH",
    "compare_observed_to_expected_sessions",
    "expected_market_session_coverage",
    "market_session_dates_sha256",
    "us_market_hours_sha256",
]
