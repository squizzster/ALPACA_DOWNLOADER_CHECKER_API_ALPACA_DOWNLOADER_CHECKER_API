"""Small publication-owned comparison rows; never a second pricing store."""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from libs.common.debug_trace import event

from .contract import ADJUSTMENTS, COLUMNS, NEW_YORK, SUPPORTED_VERSIONS, VERSION, DownloadError, timestamp

TAIL_SIZE = 10
TAIL_VERSION = 32


class Endpoints(dict):
    """Completed per-view endpoints and the producer's effective all-view tail.

    None identifies a recognized legacy handoff, not an empty history. Consumers
    must select its owning transition before comparing retained history. An empty
    tuple certifies a completed empty all view. Preserve the tail when handing
    endpoints across acquisition, publication and recovery.
    """

    def __init__(self, values=(), *, tail_all=None):
        super().__init__((name, tuple(tuple(row) for row in rows)) for name, rows in dict(values).items())
        self.tail_all = None if tail_all is None else tuple(tuple(row) for row in tail_all)


@dataclass(frozen=True)
class HistoryComparison:
    """One completed first/tail decision, carried with the selected generation."""

    decision: str
    changed_sessions: tuple[str, ...]
    live_all: tuple[tuple[str, ...], ...]
    field_changes: tuple = ()

    @property
    def matched(self):
        return self.decision == "unchanged"

    @property
    def correction_session(self):
        return self.changed_sessions[0] if self.decision == "latest_session" else None


def comparison_rows(first, tail):
    """The first observation plus up to ten final observations, without overlap."""
    if not tail:
        return ()
    return tuple(tail) if timestamp(first[0]) == timestamp(tail[0][0]) else (first, *tail)


def history_windows(endpoints, *, cutoff=None, utc_session_end=False):
    """At most two disjoint windows: first session and the complete stored tail."""
    history_check_endpoints(endpoints)
    tail = getattr(endpoints, "tail_all", None)
    if tail is None:
        raise DownloadError("legacy comparison tail requires its owning upgrade")
    if not tail:
        return ()

    def beginning(row):
        day = timestamp(row[0]).astimezone(NEW_YORK).date()
        return datetime.combine(day, datetime.min.time(), tzinfo=NEW_YORK)

    def ending(row):
        start = beginning(row)
        # Preserve each existing comparison owner's exclusive bound. Daily
        # summary checks end at next UTC midnight; ordinary acquisition uses
        # the existing New York session window. Only the tail's start expands.
        return (
            datetime.combine(start.date() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            if utc_session_end
            else start + timedelta(days=1)
        )

    first = endpoints["all"][0]
    if len(tail) > 1 and timestamp(first[0]) == timestamp(tail[0][0]):
        # Keep the first observation separate even in short histories. Sparse
        # sources can have years between their first and second row; merging
        # those windows would request unselected intervening history. Overlap
        # is removed from the tail request, not compared a second time.
        tail = tail[1:]
    ranges = [(beginning(tail[0]), ending(tail[-1]))]
    if timestamp(first[0]) < timestamp(tail[0][0]):
        ranges.insert(0, (beginning(first), ending(first)))
    return tuple((start.astimezone(timezone.utc), min(end, cutoff) if cutoff else end) for start, end in ranges)


def fetch_history(client, symbol, endpoints, asof, *, cutoff=None):
    """Establish the shared decision from completed live observations once."""
    from .responses import fetch_rows

    live = tuple(
        row
        for start, end in history_windows(endpoints, cutoff=cutoff)
        for row in fetch_rows(client, symbol, "all", start, end, asof=asof)
    )
    return compare_history(endpoints["all"][0], endpoints.tail_all, live)


def compare_history(first, tail, live):
    """Classify exact completed observations once; a missing row is not a change.

    The window is a sampling policy. Matching selected observations approves reuse
    of the unobserved history; it does not establish its equality to the provider.
    A first-session change always selects replacement, including a one-row source.
    """
    expected = comparison_rows(first, tail)
    if not expected:
        raise DownloadError("history comparison requires a nonempty published tail")
    actual = {}
    for row in live:
        instant = timestamp(row[0]).astimezone(NEW_YORK).date()
        if instant in actual:
            raise DownloadError("duplicate live history comparison session")
        actual[instant] = tuple(row)
    sessions = {timestamp(row[0]).astimezone(NEW_YORK).date() for row in expected}
    if sessions - actual.keys():
        raise DownloadError("missing live history comparison session")
    fields = tuple(
        (row[0], different_fields(row, actual[timestamp(row[0]).astimezone(NEW_YORK).date()]))
        for row in expected
    )
    fields = tuple((session, names) for session, names in fields if names)
    changed = tuple(session for session, _ in fields)
    extra = actual.keys() - sessions
    if extra:
        # A new bar inside the selected retained window changes membership. It
        # cannot be represented as a correction of the already stored final row.
        decision = "full_history"
    elif not changed:
        decision = "unchanged"
    elif len(changed) == 1 and changed[0] == tail[-1][0] and timestamp(first[0]) != timestamp(tail[-1][0]):
        decision = "latest_session"
    else:
        decision = "full_history"
    return HistoryComparison(decision, changed, tuple(actual[key] for key in sorted(actual)), fields)


def validate_tail(tail, first, last):
    """Validate the selecting artifact's own tail contract, without upstream I/O."""
    if not isinstance(tail, (tuple, list)) or len(tail) > TAIL_SIZE:
        raise DownloadError("invalid pricing comparison tail")
    if not tail:
        if first is not None or last is not None:
            raise DownloadError("nonempty pricing requires a comparison tail")
        return ()
    result = []
    for row in tail:
        if (
            not isinstance(row, (list, tuple))
            or len(row) != len(COLUMNS)
            or any(type(v) is not str for v in row)
        ):
            raise DownloadError("invalid pricing comparison tail row")
        timestamp(row[0])
        try:
            if any(not Decimal(value).is_finite() for value in row[1:]):
                raise DownloadError("non-finite pricing comparison tail value")
        except InvalidOperation as error:
            raise DownloadError("invalid pricing comparison tail value") from error
        if result and timestamp(result[-1][0]) >= timestamp(row[0]):
            raise DownloadError("pricing comparison tail is not strictly ordered")
        result.append(tuple(row))
    if first is None or last is None or not same_row(result[-1], last):
        raise DownloadError("pricing comparison tail differs from its latest endpoint")
    if (
        timestamp(first[0]) > timestamp(result[0][0])
        or len(result) < TAIL_SIZE
        and not same_row(first, result[0])
    ):
        raise DownloadError("pricing comparison tail does not cover the complete shorter history")
    return tuple(result)


@dataclass(frozen=True)
class EndpointSelection:
    """Acquisition's accepted validator and optional completed comparison.

    The immutable values survive network I/O, bound to one store, source,
    mapping and cutoff. The consumer must recheck that generation under its
    newly acquired lock. Approved appends do not confirm history again; a
    selected full replacement retains its independent producer confirmation.
    """

    store: str
    symbol: str
    generation: str | None
    end: str
    asof: str
    points: tuple | None
    matched: bool | None = None
    tail_all: tuple | None = None
    comparison: HistoryComparison | None = None
    comparison_asof: str | None = None

    def document(self):
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_document(cls, document):
        if not isinstance(document, dict) or document.get("schema_version") != 1:
            raise DownloadError("unsupported durable pricing selection")
        values = {key: value for key, value in document.items() if key != "schema_version"}
        comparison = values.get("comparison")
        if comparison is not None:
            if comparison.get("decision") not in ("unchanged", "latest_session", "full_history"):
                raise DownloadError("invalid durable pricing decision")
            values["comparison"] = HistoryComparison(
                comparison["decision"],
                tuple(comparison["changed_sessions"]),
                tuple(tuple(row) for row in comparison["live_all"]),
                tuple((session, tuple(names)) for session, names in comparison.get("field_changes", ())),
            )
        try:
            return cls(**values)
        except (TypeError, KeyError) as error:
            raise DownloadError("invalid durable pricing selection") from error

    def applies(self, store, symbol, generation, end, asof):
        applicable = (str(store), symbol, generation, end, asof) == (
            self.store,
            self.symbol,
            self.generation,
            self.end,
            self.asof,
        )
        event(
            "endpoint_selection_binding",
            symbol=symbol,
            store=str(store),
            source_generation=generation,
            selected_generation=self.generation,
            selected_store=self.store,
            selected_symbol=self.symbol,
            end_exclusive=end,
            selected_end_exclusive=self.end,
            asof=asof,
            selected_asof=self.asof,
            applicable=applicable,
            comparison_matched=self.matched,
        )
        return applicable

    def consume(self, store, symbol, generation, end, asof):
        if not self.applies(store, symbol, generation, end, asof):
            raise DownloadError("endpoint selection no longer matches the selected acquisition")
        return None if self.points is None else Endpoints(self.points, tail_all=self.tail_all)


def endpoints_of(views: dict) -> dict:
    return Endpoints(
        {
            name: () if not rows else (rows[0],) if len(rows) == 1 else (rows[0], rows[-1])
            for name, rows in views.items()
        },
        tail_all=views["all"][-TAIL_SIZE:],
    )


def history_check_endpoints(endpoints):
    """Require a published all baseline before selecting retained observations."""
    if endpoints is None:
        raise DownloadError("retained pricing requires a published endpoint validator; explicitly rebuild it")
    if not endpoints["all"] and any(endpoints.values()):
        raise DownloadError("retained pricing has no all-adjusted baseline; explicit replacement required")
    return {"all": endpoints["all"]}


def same_row(left: tuple, right: tuple) -> bool:
    return timestamp(left[0]) == timestamp(right[0]) and all(
        Decimal(a) == Decimal(b) for a, b in zip(left[1:], right[1:])
    )


def different_fields(left, right):
    return tuple(
        name
        for index, name in enumerate(COLUMNS)
        if not (
            timestamp(left[index]) == timestamp(right[index])
            if index == 0
            else Decimal(left[index]) == Decimal(right[index])
        )
    )


def validator_document(symbol: str, endpoints: dict) -> dict:
    result = {
        "schema_version": VERSION,
        "boundary": "alpaca_pricing_first_last_sessions",
        "symbol": symbol,
        "adjustments": {
            name: {"first": list(rows[0]) if rows else None, "last": list(rows[-1]) if rows else None}
            for name, rows in endpoints.items()
        },
    }
    if VERSION >= TAIL_VERSION:
        points = endpoints["all"]
        result["tail_all"] = list(
            validate_tail(
                getattr(endpoints, "tail_all", None),
                points[0] if points else None,
                points[-1] if points else None,
            )
        )
    return result


def validator_endpoints(document: dict, symbol: str) -> dict:
    # Check this compact handoff's owner/schema/shape. The pricing publisher
    # already tied it to the selected source; DO NOT open full histories merely
    # to reproduce their first/last rows. Endpoints are not an interior audit.
    if (
        type(document.get("schema_version")) is not int
        or document["schema_version"] not in SUPPORTED_VERSIONS
        or document["schema_version"] < 4
        or document.get("boundary") != "alpaca_pricing_first_last_sessions"
        or document.get("symbol") != symbol
        or not isinstance(document.get("adjustments"), dict)
        or set(document["adjustments"]) != set(ADJUSTMENTS)
    ):
        raise DownloadError("invalid first/last-session validator owner or inventory")
    result = {}
    for name in ADJUSTMENTS:
        pair = document["adjustments"][name]
        if not isinstance(pair, dict) or set(pair) != {"first", "last"}:
            raise DownloadError("invalid first/last-session validator entry")
        first, last = pair["first"], pair["last"]
        if first is None and last is None:
            result[name] = ()
            continue
        for row in (first, last):
            if not isinstance(row, list) or len(row) != len(COLUMNS) or any(type(v) is not str for v in row):
                raise DownloadError("invalid first/last-session validator row")
            timestamp(row[0])
            try:
                if any(not Decimal(value).is_finite() for value in row[1:]):
                    raise DownloadError("non-finite first/last-session value")
            except InvalidOperation as error:
                raise DownloadError("invalid first/last-session value") from error
        if timestamp(first[0]) > timestamp(last[0]) or (
            timestamp(first[0]) == timestamp(last[0]) and first != last
        ):
            raise DownloadError("first/last-session ordering differs")
        result[name] = (tuple(first),) if first == last else (tuple(first), tuple(last))
    tail = None
    if document["schema_version"] >= TAIL_VERSION or "tail_all" in document:
        points = result["all"]
        tail = validate_tail(
            document.get("tail_all"), points[0] if points else None, points[-1] if points else None
        )
    return Endpoints(result, tail_all=tail)


def session_extent(endpoints: dict) -> dict | None:
    days = [
        timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for rows in endpoints.values() for row in rows
    ]
    return {"from": min(days), "to": max(days)} if days else None


def merge_ranges(*ranges: dict | None) -> dict | None:
    selected = [value for value in ranges if value is not None]
    return (
        {"from": min(value["from"] for value in selected), "to": max(value["to"] for value in selected)}
        if selected
        else None
    )
