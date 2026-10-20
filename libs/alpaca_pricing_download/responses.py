"""Completed, normalized bar responses retained only within one operation."""

from dataclasses import dataclass
from datetime import datetime

from .contract import DownloadError, Row, timestamp, transform


@dataclass(frozen=True)
class CompletedBars:
    symbol: str
    adjustment: str
    start: datetime
    end: datetime
    asof: str
    rows: tuple[Row, ...]

    def select(self, symbol, adjustment, start, end, asof):
        if (symbol, adjustment, asof) != (
            self.symbol,
            self.adjustment,
            self.asof,
        ) or not self.start <= start < end <= self.end:
            raise DownloadError("completed bars do not cover the requested identity/range")
        if (start, end) == (self.start, self.end):
            return self
        return CompletedBars(
            symbol,
            adjustment,
            start,
            end,
            asof,
            tuple(row for row in self.rows if start <= timestamp(row[0]) < end),
        )


def completed_bars(value, symbol, adjustment, start, end, asof):
    """Raw standalone client adapters establish the same boundary once on arrival."""
    if isinstance(value, CompletedBars):
        return value.select(symbol, adjustment, start, end, asof)
    return CompletedBars(symbol, adjustment, start, end, asof, transform(value, start, end))


def fetch_rows(client, symbol, adjustment, start, end, *, asof=None):
    value = client.fetch(symbol, adjustment, start, end, asof=asof)
    return completed_bars(value, symbol, adjustment, start, end, asof or end.date().isoformat()).rows
