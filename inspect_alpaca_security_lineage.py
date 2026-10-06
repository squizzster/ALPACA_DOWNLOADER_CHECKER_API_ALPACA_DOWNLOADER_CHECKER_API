#!/usr/bin/env python3
"""Stable executable entry point for inspecting Alpaca security lineage."""

import sys

# Keep the read-only command from creating Python bytecode caches.
sys.dont_write_bytecode = True

from libs.alpaca_security_lineage import (  # noqa: E402
    EARLIEST_BAR_DATE,
    MAX_NAME_CHANGE_SYMBOLS,
    MIN_REQUEST_INTERVAL_SECONDS,
    REPORT_VERSION,
    REQUESTS_PER_MINUTE,
    AlpacaIdentityClient,
    AlpacaIdentityRequestRateLimiter,
    build_identity_report,
    inspect_alpaca_security_lineage,
    inspect_local_downloads,
    parse_command_line_arguments,
    rate_limit_delay_seconds,
    run_security_lineage_command,
)

__all__ = [
    "AlpacaIdentityClient",
    "AlpacaIdentityRequestRateLimiter",
    "EARLIEST_BAR_DATE",
    "MAX_NAME_CHANGE_SYMBOLS",
    "MIN_REQUEST_INTERVAL_SECONDS",
    "REPORT_VERSION",
    "REQUESTS_PER_MINUTE",
    "build_identity_report",
    "inspect_alpaca_security_lineage",
    "inspect_local_downloads",
    "main",
    "parse_command_line_arguments",
    "rate_limit_delay_seconds",
]


def main() -> None:
    run_security_lineage_command(parse_command_line_arguments())


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)
