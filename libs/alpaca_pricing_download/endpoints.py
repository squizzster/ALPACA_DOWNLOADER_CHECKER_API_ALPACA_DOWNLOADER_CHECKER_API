"""Small publication-owned comparison rows; never a second pricing store."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from libs.common.debug_trace import event

from .contract import ADJUSTMENTS, COLUMNS, NEW_YORK, SUPPORTED_VERSIONS, VERSION, DownloadError, timestamp


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
        return None if self.points is None else dict(self.points)


def endpoints_of(views: dict) -> dict:
    return {
        name: () if not rows else (rows[0],) if len(rows) == 1 else (rows[0], rows[-1])
        for name, rows in views.items()
    }


def history_check_endpoints(endpoints):
    """Only first/latest all may select reuse of completed retained pricing."""
    if endpoints is None:
        raise DownloadError("retained pricing requires a published endpoint validator; explicitly rebuild it")
    if not endpoints["all"] and any(endpoints.values()):
        raise DownloadError("retained pricing has no all-adjusted baseline; explicit replacement required")
    return {"all": endpoints["all"]}


def same_row(left: tuple, right: tuple) -> bool:
    return timestamp(left[0]) == timestamp(right[0]) and all(
        Decimal(a) == Decimal(b) for a, b in zip(left[1:], right[1:])
    )


def validator_document(symbol: str, endpoints: dict) -> dict:
    return {
        "schema_version": VERSION,
        "boundary": "alpaca_pricing_first_last_sessions",
        "symbol": symbol,
        "adjustments": {
            name: {"first": list(rows[0]) if rows else None, "last": list(rows[-1]) if rows else None}
            for name, rows in endpoints.items()
        },
    }


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
    return result


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
