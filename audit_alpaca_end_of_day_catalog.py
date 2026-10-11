#!/usr/bin/env python3
"""Stable read-only entry point for auditing the end-of-day catalogue."""

import sys

sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_security_correlation import (  # noqa: E402
    SecurityCorrelationError,
    audit_security_master_catalogue,
    parse_catalog_audit_arguments,
    run_catalog_audit_command,
)

__all__ = [
    "audit_security_master_catalogue",
    "main",
    "parse_catalog_audit_arguments",
    "run_catalog_audit_command",
]


def main() -> None:
    report = run_catalog_audit_command(parse_catalog_audit_arguments())
    if report["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, SecurityCorrelationError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)
