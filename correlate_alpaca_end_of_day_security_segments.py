#!/usr/bin/env python3
"""Stable command for publishing correlated and uncorrelated security segments."""

import sys

sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_pipeline import AlpacaDownloadIntegrityError  # noqa: E402
from libs.alpaca_end_of_day_security_correlation import (  # noqa: E402
    CORRELATION_VERSION,
    SecurityCorrelationError,
    build_security_correlation_report,
    correlate_and_publish_alpaca_end_of_day_security_segments,
    correlated_pipeline_path,
    parse_command_line_arguments,
    run_security_correlation_command,
    security_correlation_manifest_path,
    uncorrelated_pipeline_path,
)

__all__ = [
    "CORRELATION_VERSION",
    "SecurityCorrelationError",
    "build_security_correlation_report",
    "correlate_and_publish_alpaca_end_of_day_security_segments",
    "correlated_pipeline_path",
    "main",
    "parse_command_line_arguments",
    "security_correlation_manifest_path",
    "uncorrelated_pipeline_path",
]


def main() -> None:
    run_security_correlation_command(parse_command_line_arguments())


if __name__ == "__main__":
    try:
        main()
    except AlpacaDownloadIntegrityError as download_integrity_error:
        print(f"error: {download_integrity_error}", file=sys.stderr)
        raise SystemExit(2)
    except (OSError, RuntimeError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)
