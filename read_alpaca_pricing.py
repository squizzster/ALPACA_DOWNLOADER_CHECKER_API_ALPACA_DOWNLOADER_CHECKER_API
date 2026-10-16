#!/usr/bin/env python3.11
"""Read the standalone downloader/lineage store without acquiring or changing it."""

import sys

sys.dont_write_bytecode = True

from alpaca_end_of_day.standalone_cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
