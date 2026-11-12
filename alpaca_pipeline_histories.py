#!/usr/bin/env python3.11
"""Thin stdin/stdout adapter for the public history-production JSON API."""

import sys

from alpaca_end_of_day import produce_histories_pipeline


def main():
    try:
        output = produce_histories_pipeline(sys.stdin.buffer.read())
        sys.stdout.buffer.write(output)
        sys.stdout.buffer.flush()
    except (RuntimeError, OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
