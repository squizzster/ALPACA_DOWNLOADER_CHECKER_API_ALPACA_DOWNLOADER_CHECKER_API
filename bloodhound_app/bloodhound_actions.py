"""Build share-unit factors from events with explicitly declared coverage.

This module validates supplied records; it does not independently certify their
completeness or economic correctness. ``stock_dividend`` means an issuance of the
same continuing security. Distributions of another security must not be supplied.

Equivalent same-date, same-ratio records count once, even with different event
IDs. Distinct IDs with different ratios on the same date represent multiple
events and multiply. A reused event ID with a different date or ratio is an error.
"""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np


class ActionEvidenceError(ValueError):
    """Invalid or insufficient supplied corporate-action evidence."""


_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_EVENT_TYPES = {"split", "forward_split", "reverse_split", "stock_dividend"}
_EVENT_COLUMNS = {"public_id", "event_id", "effective_date", "old_shares", "new_shares"}
_COVERAGE_COLUMNS = {"public_id", "start_date", "end_date", "source"}


def _date(value: str, context: str) -> date:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise ActionEvidenceError(f"{context}: expected a strict YYYY-MM-DD date, got {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ActionEvidenceError(f"{context}: invalid calendar date {value!r}") from exc


def _positive_fraction(value: str, context: str) -> Fraction:
    try:
        result = Fraction(str(value))
    except (ValueError, ZeroDivisionError) as exc:
        raise ActionEvidenceError(
            f"{context}: expected a positive finite share quantity, got {value!r}"
        ) from exc
    if result <= 0:
        raise ActionEvidenceError(f"{context}: share quantity must be positive, got {value!r}")
    return result


def _read_csv(path: Path, required: set[str]) -> tuple[list[str], list[tuple[int, dict[str, str]]]]:
    path = Path(path)
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            if reader.fieldnames is None:
                raise ActionEvidenceError(f"{path}: CSV header is required")
            headers = [name.strip() for name in reader.fieldnames]
            if not all(headers) or len(set(headers)) != len(headers):
                raise ActionEvidenceError(f"{path}: empty or duplicate CSV column names")
            missing = required - set(headers)
            if missing:
                raise ActionEvidenceError(f"{path}: missing required columns: {', '.join(sorted(missing))}")
            reader.fieldnames = headers
            rows: list[tuple[int, dict[str, str]]] = []
            for row in reader:
                number = reader.line_num
                if None in row or any(value is None for value in row.values()):
                    raise ActionEvidenceError(
                        f"{path}:{number}: CSV row has a different number of fields than its header"
                    )
                clean = {key: value.strip() for key, value in row.items()}
                for name in required:
                    if not clean[name]:
                        raise ActionEvidenceError(f"{path}:{number}: {name} must not be empty")
                rows.append((number, clean))
            return headers, rows
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ActionEvidenceError(f"Cannot read action evidence CSV {path}: {exc}") from exc


@dataclass(frozen=True)
class _Event:
    effective_date: date
    ratio: Fraction
    event_ids: tuple[str, ...]
    event_types: tuple[str, ...]
    sources: tuple[str, ...]


@dataclass(frozen=True)
class _Coverage:
    start_date: date
    end_date: date
    source: str


@dataclass(frozen=True)
class ActionEvidence:
    """Parsed action evidence, normalized to ``horizon``.

    Call ``factors_for`` with every held source session, before date filtering.
    Coverage is a caller-supplied assertion and is reported as such. One declared
    interval must cover the first held session through the adjustment horizon;
    separate partial intervals are not silently combined.
    """

    horizon: str
    _events: dict[str, tuple[_Event, ...]]
    _coverage: dict[str, tuple[_Coverage, ...]]
    _input_counts: dict[str, int]

    def factors_for(self, public_id: str, sessions: np.ndarray) -> tuple[np.ndarray, dict[str, Any]]:
        """Return exact-ratio backward products as float64, plus provenance.

        Input strings must be strict YYYY-MM-DD session labels. The result
        preserves their input order. For each session t, multiply old/new for
        events with t < effective_date <= horizon. An effective session excludes
        its own event. Factors of one for an event-free history are allowed only
        when that history has the required explicit coverage declaration.
        """
        if not isinstance(public_id, str) or not public_id.strip():
            raise ActionEvidenceError("public_id must be a nonempty string")
        public_id = public_id.strip()
        values = np.asarray(sessions)
        if values.ndim != 1 or values.size == 0:
            raise ActionEvidenceError(f"{public_id}: sessions must be a nonempty one-dimensional array")
        parsed = [_date(value, f"{public_id}: source session") for value in values.tolist()]
        first, last = min(parsed), max(parsed)
        horizon = _date(self.horizon, "adjustment horizon")
        if horizon < last:
            raise ActionEvidenceError(
                f"{public_id}: horizon {self.horizon} precedes last source session {last.isoformat()}"
            )

        covering = [
            interval
            for interval in self._coverage.get(public_id, ())
            if interval.start_date <= first and interval.end_date >= horizon
        ]
        if not covering:
            raise ActionEvidenceError(
                f"{public_id}: no declared coverage interval spans first source session "
                f"{first.isoformat()} through horizon {self.horizon}; missing events do not establish factor 1"
            )
        covering.sort(
            key=lambda item: (item.end_date - item.start_date, item.start_date, item.end_date, item.source)
        )
        selected_coverage = covering[0]
        all_events = self._events.get(public_id, ())
        relevant = sorted(
            (event for event in all_events if first < event.effective_date <= horizon),
            key=lambda event: (event.effective_date, event.ratio),
            reverse=True,
        )

        exact = Fraction(1)
        position = 0
        factors_by_date: dict[date, float] = {}
        for session in sorted(set(parsed), reverse=True):
            while position < len(relevant) and relevant[position].effective_date > session:
                exact *= relevant[position].ratio
                position += 1
            try:
                value = float(exact)
            except OverflowError as exc:
                raise ActionEvidenceError(
                    f"{public_id}: split factor overflows float64 on {session.isoformat()}"
                ) from exc
            if not np.isfinite(value) or value <= 0:
                raise ActionEvidenceError(
                    f"{public_id}: split factor is not representable as positive finite float64 on {session.isoformat()}"
                )
            factors_by_date[session] = value

        input_count = self._input_counts.get(public_id, 0)
        per_date = Counter(event.effective_date for event in relevant)
        diagnostics: dict[str, Any] = {
            "method": "action_records_with_declared_coverage",
            "public_id": public_id,
            "horizon": self.horizon,
            "source_first_session": first.isoformat(),
            "source_last_session": last.isoformat(),
            "coverage": {
                "start_date": selected_coverage.start_date.isoformat(),
                "end_date": selected_coverage.end_date.isoformat(),
                "source": selected_coverage.source,
            },
            "coverage_sources": sorted({interval.source for interval in covering}),
            "coverage_is_declared_not_independently_certified": True,
            "input_event_records": input_count,
            "unique_events": len(all_events),
            "deduplicated_event_records": input_count - len(all_events),
            "events_applied": len(relevant),
            "events_after_horizon": sum(event.effective_date > horizon for event in all_events),
            "events_at_or_before_first_session": sum(event.effective_date <= first for event in all_events),
            "same_date_multiple_distinct_events": sorted(
                day.isoformat() for day, count in per_date.items() if count > 1
            ),
            "event_sources": sorted({source for event in relevant for source in event.sources}),
            "events": [
                {
                    "effective_date": event.effective_date.isoformat(),
                    "old_to_new_price_ratio": str(event.ratio),
                    "event_ids": list(event.event_ids),
                    "event_types": list(event.event_types),
                    "sources": list(event.sources),
                }
                for event in reversed(relevant)
            ],
        }
        return np.asarray([factors_by_date[session] for session in parsed], dtype=np.float64), diagnostics


def load_action_evidence(events_path: Path, coverage_path: Path, horizon: str) -> ActionEvidence:
    """Load strict event and coverage CSV files.

    Events require public_id,event_id,effective_date,old_shares,new_shares. Source
    is optional. If event_type is present, every record must be split,
    forward_split, reverse_split, or stock_dividend (same continuing security).

    Coverage requires public_id,start_date,end_date,source. The nonempty source
    identifies the evidence supporting the caller's completeness assertion.
    An events file with a header and no data is supported; factors_for still
    requires complete declared coverage for each requested series.
    """
    _date(horizon, "adjustment horizon")
    headers, event_rows = _read_csv(Path(events_path), _EVENT_COLUMNS)
    _, coverage_rows = _read_csv(Path(coverage_path), _COVERAGE_COLUMNS)
    counts: Counter[str] = Counter()
    by_id: dict[tuple[str, str], tuple[date, Fraction]] = {}
    grouped: dict[tuple[str, date, Fraction], dict[str, set[str]]] = {}
    for number, row in event_rows:
        context = f"{events_path}:{number}"
        public_id, event_id = row["public_id"], row["event_id"]
        effective = _date(row["effective_date"], f"{context}: effective_date")
        old = _positive_fraction(row["old_shares"], f"{context}: old_shares")
        new = _positive_fraction(row["new_shares"], f"{context}: new_shares")
        ratio = old / new
        event_type = row["event_type"] if "event_type" in headers else "split"
        if event_type not in _EVENT_TYPES:
            raise ActionEvidenceError(
                f"{context}: unsupported event_type {event_type!r}; only continuing-security share-unit changes are supported"
            )
        identifier = (public_id, event_id)
        identity = (effective, ratio)
        if identifier in by_id and by_id[identifier] != identity:
            raise ActionEvidenceError(
                f"{context}: conflicting records for public_id {public_id!r}, event_id {event_id!r}"
            )
        by_id[identifier] = identity
        key = (public_id, effective, ratio)
        group = grouped.setdefault(key, {"event_ids": set(), "event_types": set(), "sources": set()})
        group["event_ids"].add(event_id)
        group["event_types"].add(event_type)
        if row.get("source"):
            group["sources"].add(row["source"])
        counts[public_id] += 1

    events: dict[str, list[_Event]] = defaultdict(list)
    for (public_id, effective, ratio), group in grouped.items():
        events[public_id].append(
            _Event(
                effective,
                ratio,
                tuple(sorted(group["event_ids"])),
                tuple(sorted(group["event_types"])),
                tuple(sorted(group["sources"])),
            )
        )
    coverage: dict[str, set[_Coverage]] = defaultdict(set)
    for number, row in coverage_rows:
        context = f"{coverage_path}:{number}"
        start = _date(row["start_date"], f"{context}: start_date")
        end = _date(row["end_date"], f"{context}: end_date")
        if start > end:
            raise ActionEvidenceError(f"{context}: start_date is after end_date")
        coverage[row["public_id"]].add(_Coverage(start, end, row["source"]))
    return ActionEvidence(
        horizon=horizon,
        _events={
            key: tuple(sorted(value, key=lambda item: (item.effective_date, item.ratio)))
            for key, value in events.items()
        },
        _coverage={
            key: tuple(sorted(value, key=lambda item: (item.start_date, item.end_date, item.source)))
            for key, value in coverage.items()
        },
        _input_counts=dict(counts),
    )
