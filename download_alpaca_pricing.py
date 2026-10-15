#!/usr/bin/env python3.11
"""Standalone ticker-pricing downloader. Never invokes the lineage pipeline."""

from libs.alpaca_pricing_download.command_line import main

if __name__ == "__main__":
    raise SystemExit(main())
