#!/usr/bin/env python3.11
"""Pricing entrypoint, including the complete --update-everything workflow."""

from libs.alpaca_pricing_download.command_line import main

if __name__ == "__main__":
    raise SystemExit(main())
