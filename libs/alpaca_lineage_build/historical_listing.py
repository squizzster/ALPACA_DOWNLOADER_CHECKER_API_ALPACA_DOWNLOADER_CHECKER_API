"""Assess historical listing evidence within one already-owned literal ticker range."""

from array import array
from bisect import bisect_left, bisect_right
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from itertools import groupby

from libs.alpaca_data_api_solver import NEW_YORK, instant, iso
from libs.alpaca_data_api_solver.listing_trade_batch import opening_second
from libs.alpaca_security_lineage.listing_trade_batch import combine_listing_samples, literal_listing_key
from libs.common.us_equity_market_session_calendar import (
    market_session_window,
    next_market_session_after,
    regular_market_hours,
    us_market_hours_sha256,
)

from .exchange_changes import _LISTING_TAPES, _point_sample
from .listing_ranges import COMPETING

_TAPE_EXCHANGES = {tape: exchange for exchange, tape in _LISTING_TAPES.items()}


class ScreenCalendarChanged(ValueError):
    """The owner must rebuild this range's screen from its completed native auction input."""


def _calendar_scope(sessions, candidates):
    # The date axis gives durable integer positions their meaning. Only the
    # selected dates' regular opens enter the point-batch eligibility decision.
    digest = sha256("\n".join(sessions).encode("ascii"))
    for index in candidates:
        if not 0 <= index < len(sessions):
            return None
        day = sessions[index]
        digest.update(f"\n{day}T{regular_market_hours(day)[0]}".encode("ascii"))
    return digest.hexdigest()


def _auction_indices(runs):
    if not runs:
        return range(0)
    if len(runs) == 2:
        start, length = runs
        return range(start, start + length)
    runs = iter(runs)
    end = 0
    indices = array("I")
    for gap, length in zip(runs, runs, strict=True):
        start = end + gap
        end = start + length
        indices.extend(range(start, end))
    return memoryview(indices.tobytes()).cast("I")


@dataclass(frozen=True, slots=True)
class RangeScreen:
    symbol: str
    first_session: str
    last_session: str
    sessions: Sequence[str]
    auction_days: Sequence[int]
    candidates: tuple
    batch_sessions: tuple
    ambiguous: int
    switches: int
    auction_available: bool
    auction_seconds: dict

    @property
    def initial_sessions(self):
        return tuple(self.sessions[i] for i in self.candidates)

    def to_document(self):
        """Represent successive auction dates as integer gap/length pairs."""
        if isinstance(self.auction_days, range):
            runs = [self.auction_days.start, len(self.auction_days)] if self.auction_days else []
        else:
            runs = []
            end = 0
            for _, run in groupby(enumerate(self.auction_days), key=lambda pair: pair[1] - pair[0]):
                start = next(run)[1]
                length = 1 + sum(1 for _ in run)
                runs.extend((start - end, length))
                end = start + length
        eligible = set(self.batch_sessions)
        return {
            "schema_version": 3,
            "calendar_sha256": us_market_hours_sha256(),
            "calendar_scope_sha256": _calendar_scope(self.sessions, self.candidates),
            "symbol": self.symbol,
            "first_session": self.first_session,
            "last_session": self.last_session,
            "auction_runs": runs,
            "candidates": list(self.candidates),
            "batch_indices": [i for i in self.candidates if self.sessions[i] in eligible],
            "auction_seconds": [
                [i, *[int(instant(t).timestamp()) for t in self.auction_seconds[self.sessions[i]]]]
                for i in self.candidates
                if self.sessions[i] in self.auction_seconds
            ],
            "ambiguous": self.ambiguous,
            "switches": self.switches,
            "auction_available": self.auction_available,
        }

    @classmethod
    def from_document(cls, document):
        version = document["schema_version"]
        if version not in (1, 2, 3):
            raise ValueError("unsupported historical listing screen schema")
        first, last = document["first_session"], document["last_session"]
        sessions = market_session_window(first, last)
        if document["calendar_sha256"] != us_market_hours_sha256():
            if (
                version < 3
                or _calendar_scope(sessions, document["candidates"]) != document["calendar_scope_sha256"]
            ):
                raise ScreenCalendarChanged(
                    "historical listing screen calendar changed; rebuild this range from its native auction pages"
                )
        days = _auction_indices(document["auction_runs"])
        batch_sessions = tuple(sessions[i] for i in document["batch_indices"])
        if document["schema_version"] == 1:
            seconds = {s: (opening_second(s)[0],) for s in batch_sessions}
        else:
            seconds = {
                sessions[i]: tuple(iso(datetime.fromtimestamp(stamp, timezone.utc)) for stamp in stamps)
                for i, *stamps in document["auction_seconds"]
            }
        return cls(
            document["symbol"],
            first,
            last,
            sessions,
            days,
            tuple(document["candidates"]),
            batch_sessions,
            document["ambiguous"],
            document["switches"],
            document["auction_available"],
            seconds,
        )


def _auction_screen(rows, positions):
    # These market-centre opening and closing conditions select where to take
    # a trade sample. An auction's execution venue alone cannot identify a listing.
    venues_by_day = {}
    event_times = {}
    for row in rows:
        index = positions.get(row["d"])
        if index is None:
            continue
        for side, condition in (("o", "O"), ("c", "6")):
            for event in row.get(side) or ():
                if event.get("c") == condition and event.get("x"):
                    venue_sets = venues_by_day.setdefault(index, {"o": set(), "c": set()})
                    venue_sets[side].add(event["x"])
                    if isinstance(event.get("t"), str):
                        event_times.setdefault(index, []).append((side, event["t"]))

    candidates = set()
    ambiguous = switches = 0
    previous = None
    for index in sorted(venues_by_day):
        opening, closing = venues_by_day[index].values()
        venues = opening & closing if opening and closing else opening or closing
        if len(venues) != 1:
            ambiguous += 1
            continue
        venue = next(iter(venues))
        if previous and previous[1] != venue:
            candidates.update((previous[0], index))
            switches += 1
        previous = index, venue
    return sorted(venues_by_day), candidates, event_times, ambiguous, switches


def screen_range(symbol, first, last, rows):
    """Select sparse literal queries once. A known opening auction makes its date batch-eligible."""
    if first > last:
        raise ValueError("historical listing range is reversed")
    sessions = market_session_window(first, last)
    if sessions and callable(rows):
        rows = rows()
    positions = {session: index for index, session in enumerate(sessions)}
    auction_days, candidates, event_times, ambiguous, switches = _auction_screen(
        rows if sessions and rows is not None else (), positions
    )
    if sessions:
        candidates.update((0, len(sessions) - 1))
    seconds = {}
    qualified_opens = set()
    for i in sorted(candidates & event_times.keys()):
        day = sessions[i]
        eligible = set()
        for side, raw in event_times[i]:
            if len(raw) <= 10:
                continue
            try:
                stamp = instant(raw)
            except ValueError:
                continue
            if stamp.astimezone(NEW_YORK).date().isoformat() != day:
                continue
            second = iso(stamp.replace(microsecond=0))
            eligible.add(second)
            if side == "o" and second == opening_second(day)[0]:
                qualified_opens.add(i)
        if eligible:
            seconds[day] = tuple(sorted(eligible))
    if auction_days and auction_days[-1] - auction_days[0] + 1 == len(auction_days):
        indices = range(auction_days[0], auction_days[-1] + 1)
    elif auction_days:
        indices = memoryview(array("I", auction_days).tobytes()).cast("I")
    else:
        indices = range(0)
    return RangeScreen(
        symbol,
        first,
        last,
        sessions,
        indices,
        tuple(sorted(candidates)),
        tuple(sessions[i] for i in sorted(qualified_opens)),
        ambiguous,
        switches,
        rows is not None and getattr(rows, "available", True),
        seconds,
    )


def _refine(left, right, auction_days, observe, points, conflicts):
    """Narrow only unlike observed tapes. A missing middle print can leave a bracket."""
    pending = [(left, right)]
    while pending:
        low, high = pending.pop()
        if (
            high - low < 2
            or points[low]["tape"] == points[high]["tape"]
            or any(low < i < high for i in conflicts)
        ):
            continue
        middle = (low + high) // 2
        first = bisect_right(auction_days, low)
        stop = bisect_left(auction_days, high)
        pos = bisect_left(auction_days, middle, first, stop)
        # Prefer a represented session near the midpoint. Keep the midpoint
        # when auctions occur only at a distant edge of the bracket.
        near = auction_days[max(first, pos - 1) : min(stop, pos + 1)]
        selected = min(near, key=lambda i: abs(i - middle)) if near else middle
        if abs(selected - middle) > (high - low) // 4:
            selected = middle
        observe(selected)
        if selected in conflicts:
            continue  # This day has incompatible affirmative prints. Another request cannot settle it.
        inside = []
        if selected in points:
            inside.append(selected)
        else:
            # Thin trading can hide the midpoint. Two distinct nearby auction
            # or calendar dates suffice to try the two sides of that gap.
            pos = bisect_left(auction_days, selected)
            earlier = auction_days[pos - 1] if pos > first else selected - 1
            pos = bisect_right(auction_days, selected)
            later = auction_days[pos] if pos < stop else selected + 1
            for index in (earlier, later):
                if low < index < high:
                    observe(index)
                    if index in conflicts:
                        break
                    if index in points:
                        inside.append(index)
        if inside:
            anchors = [low, *inside, high]
            pending.extend(zip(anchors, anchors[1:]))


def assess_range(symbol, first, last, client, *, screen=None, retained_points: Iterable[dict] = ()):
    """Return dated points and changes for an inclusive, producer-selected ownership interval."""
    if screen is None:
        screen = screen_range(symbol, first, last, lambda: client.auction_days(symbol, first, last))
    elif (screen.symbol, screen.first_session, screen.last_session) != (symbol, first, last):
        raise ValueError("historical listing screen belongs to another selected range")
    sessions, auction_days = screen.sessions, screen.auction_days
    limits = set()
    if sessions and not screen.auction_available:
        limits.add("auction_screen_unavailable")
    if not sessions:
        limits.add("no_calendar_sessions")
    if len(auction_days) < len(sessions):
        limits.add("incomplete_auction_coverage")
    if screen.ambiguous:
        limits.add("ambiguous_auction_venues")

    outcomes = {}
    points = {}
    conflicts = {}
    held = {}
    for sample in retained_points:
        key = literal_listing_key(sample)
        if (
            not key
            or key[0] != symbol
            or sample.get("http_status") != 200
            or sample.get("outcome") not in ("observed", "conflicting_tapes")
        ):
            continue
        index = bisect_left(sessions, key[1])
        if index < len(sessions) and sessions[index] == key[1]:
            held[index] = combine_listing_samples(held.get(index), sample)

    def observe(index):
        if index in outcomes:
            return
        session = sessions[index]
        sample = held[index] if index in held else client.get_trade_tape_sample(symbol, session)
        if (
            sample
            and sample.get("outcome") == "conflicting_tapes"
            and sample.get("http_status") == 200
            and literal_listing_key(sample) == (symbol, session)
        ):
            conflicts[index] = sample["conflicting_trades"]
            outcomes[index] = "conflicting_tapes"
            return
        if sample is None or not _point_sample(sample, symbol) or sample["session"] != session:
            outcomes[index] = "unavailable"
            return
        outcome = sample["outcome"]
        if outcome == "observed":
            trade = sample["trade"]
            tape = trade.get("z")
            if tape in ("A", "B", "C"):
                point = {"session": session, "tape": tape, "trade_at": trade["t"]}
                if tape in _TAPE_EXCHANGES:
                    point["exchange"] = _TAPE_EXCHANGES[tape]
                points[index] = point
            else:
                outcome = "unknown_tape"
        outcomes[index] = outcome

    # The ownership producer has already dated these literal prints. An interior
    # witness may show a transfer and reversion invisible at both selected endpoints.
    for index in held:
        observe(index)
    for index in screen.candidates:
        observe(index)
    if auction_days:
        if 0 not in points and 0 not in conflicts:
            observe(auction_days[0])
        if len(sessions) - 1 not in points and len(sessions) - 1 not in conflicts:
            observe(auction_days[-1])

    anchors = sorted(points)
    for left, right in zip(anchors, anchors[1:]):
        _refine(left, right, auction_days, observe, points, conflicts)

    anchors = sorted(points)
    changes = []
    for left, right in zip(anchors, anchors[1:]):
        old, new = points[left], points[right]
        if old["tape"] == new["tape"]:
            continue
        change = {
            "from_session": old["session"],
            "to_session": new["session"],
            "from_tape": old["tape"],
            "to_tape": new["tape"],
        }
        for side, point in (("from", old), ("to", new)):
            if "exchange" in point:
                change[side + "_exchange"] = point["exchange"]
        if right == left + 1:
            change["effective_session"] = new["session"]
        else:
            limits.add("change_date_bounded")
        changes.append(change)

    if len(points) + len(conflicts) < len(sessions):
        limits.add("sampled_sessions_only")
    if any(point["tape"] == "B" for point in points.values()):
        limits.add("multiple_listings_on_tape_b")
    if conflicts:
        limits.add("conflicting_trade_tapes")
    if sessions and not points and not conflicts:
        limits.add("no_recognized_trade_tape")
    return {
        "symbol": symbol,
        "first_session": first,
        "last_session": last,
        "status": "changes_observed" if changes else "sampled" if points else "unresolved",
        "observations": [points[index] for index in anchors],
        "changes": changes,
        "limits": sorted(limits),
        "diagnostics": {
            "market_sessions": len(sessions),
            "auction_sessions": len(auction_days),
            "ambiguous_auction_sessions": screen.ambiguous,
            "auction_venue_changes": screen.switches,
            "trade_sessions_sampled": len(outcomes),
            "trade_outcomes": dict(sorted(Counter(outcomes.values()).items())),
            **(
                {
                    "conflicting_sessions": [
                        {"session": sessions[i], "conflicting_trades": conflicts[i]} for i in sorted(conflicts)
                    ]
                }
                if conflicts
                else {}
            ),
        },
    }


def assess_label_boundaries(ranges, *, windows=None, include_same_symbol=False):
    """Join directly witnessed endpoints of adjacent ownership ranges for one selected public ID."""
    changes = []
    latest_end = ""
    previous = None
    if windows is not None:
        windows = tuple(windows)
        if not windows:
            return changes
    order = sorted(ranges, key=lambda row: row["first_session"])
    for start, group in groupby(order, key=lambda row: row["first_session"]):
        rows = list(group)
        if (
            previous
            and len(rows) == 1
            and latest_end < start
            and (windows is None or any(first <= start and latest_end <= last for first, last in windows))
            and next_market_session_after(latest_end) == start
        ):
            current = rows[0]
            old_points, new_points = previous["observations"], current["observations"]
            if (
                (include_same_symbol or previous["symbol"] != current["symbol"])
                and old_points
                and new_points
                and COMPETING not in previous.get("limits", ())
                and COMPETING not in current.get("limits", ())
            ):
                old, new = old_points[-1], new_points[0]
                if (
                    old["session"] == latest_end
                    and new["session"] == start
                    and old["tape"] != new["tape"]
                    and old["tape"] in ("A", "B", "C")
                    and new["tape"] in ("A", "B", "C")
                ):
                    changes.append(
                        {
                            "from_symbol": previous["symbol"],
                            "to_symbol": current["symbol"],
                            "from_session": latest_end,
                            "to_session": start,
                            "from_tape": old["tape"],
                            "to_tape": new["tape"],
                            **({"from_exchange": old["exchange"]} if "exchange" in old else {}),
                            **({"to_exchange": new["exchange"]} if "exchange" in new else {}),
                            "effective_session": start,
                        }
                    )
        # A longer-lived earlier alias can overlap several later ranges. Only
        # a unique owner label at the actual ending session can support the join.
        for row in rows:
            if row["last_session"] > latest_end:
                latest_end, previous = row["last_session"], row
            elif row["last_session"] == latest_end:
                previous = None
    return changes
