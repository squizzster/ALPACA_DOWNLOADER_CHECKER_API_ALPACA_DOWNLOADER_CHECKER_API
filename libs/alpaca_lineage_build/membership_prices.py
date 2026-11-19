"""Selected retained observations in complete-source coordinates for membership changes."""

from bisect import bisect_left, bisect_right
from copy import copy
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .source_observations import counterpart_partition


@dataclass
class MembershipRows:
    """Accepted positions and endpoints. Only a real interior query needs its partition."""

    store: str
    symbol: str
    source: dict
    adjustment: str
    partitions: dict
    loaded: dict
    suffix: tuple
    prefix_rows: int
    month_starts: tuple
    month_labels: tuple
    endpoints: dict

    def __len__(self):
        return self.prefix_rows + len(self.suffix)

    def month(self, position):
        return self.month_labels[bisect_right(self.month_starts, position) - 1]

    def timestamp_at(self, position):
        if position in self.endpoints:
            return self.endpoints[position][1]
        return self[position][0]

    def session(self, position):
        if position not in self.endpoints:
            stamp = self.timestamp_at(position)
            self.endpoints[position] = timestamp(stamp).astimezone(NEW_YORK).date().isoformat(), stamp
        return self.endpoints[position][0]

    def __getitem__(self, position):
        if isinstance(position, slice):
            return tuple(self[i] for i in range(*position.indices(len(self))))
        if position < 0:
            position += len(self)
        if not 0 <= position < len(self):
            raise IndexError(position)
        if position >= self.prefix_rows:
            return self.suffix[position - self.prefix_rows]
        month = self.month(position)
        anchor = self.source["anchor_month"]
        if month[:4] < anchor[:4]:
            kind, offset = "completed_years", 0
        else:
            kind = "current_month" if month == anchor else "current_year"
            first = anchor if month == anchor else anchor[:4] + "-01"
            offset = self.month_starts[bisect_left(self.month_labels, first)]
        if kind not in self.loaded:
            self.loaded[kind] = counterpart_partition(
                Path(self.store), SimpleNamespace(symbol=self.symbol, source=self.source), kind, self.partitions
            )
        return self.loaded[kind][self.adjustment][position - offset]


def compose(store, view, retained, partitions):
    """Carry the suffix and the retained monthly assignment coordinates once."""
    segments = {adj: [] for adj in view.views}
    for branch in ("correlated", "uncorrelated"):
        for segment in retained[branch]["segments"]:
            segments[segment["adjustment"]].append(segment)
    result = copy(view)
    result.materialization_scope = "membership_prefix"
    result.views = {}
    accepted = getattr(view, "membership_session_dates", {})
    fresh_sessions, loaded = {}, {}
    for adj, suffix in view.views.items():
        summary = view.previous_assignment_summary[adj]
        limit = summary["rows"]
        starts, endpoints = {}, {}
        for segment in segments[adj]:
            first, last = segment["row_start"] - 1, segment["row_end"] - 1
            if first >= limit:
                continue
            day = segment["first_session_date"]
            starts[day[:7]] = min(starts.get(day[:7], first), first)
            endpoints[first] = day, segment["first_timestamp"]
            if last < limit:
                endpoints[last] = segment["last_session_date"], segment["last_timestamp"]
        if limit:
            endpoints[limit - 1] = summary["last_session"], summary["last_timestamp"]
        for position, row in enumerate(suffix, limit):
            stamp = row[0]
            if stamp not in fresh_sessions:
                fresh_sessions[stamp] = (
                    accepted[stamp] if stamp in accepted else timestamp(stamp).astimezone(NEW_YORK).date()
                ).isoformat()
            day = fresh_sessions[stamp]
            starts.setdefault(day[:7], position)
            endpoints[position] = day, stamp
        months = tuple(sorted(starts))
        result.views[adj] = MembershipRows(
            str(store),
            view.symbol,
            view.source,
            adj,
            partitions,
            loaded,
            suffix,
            limit,
            tuple(starts[m] for m in months),
            months,
            endpoints,
        )
    return result
