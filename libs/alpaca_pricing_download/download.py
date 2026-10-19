"""Ticker-only acquisition and three-boundary consolidation."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    NEW_YORK,
    VALIDATOR_FILE,
    VERSION,
    BarsSymbolUnavailable,
    DownloadError,
    InitialPricingUnavailable,
    build_boundary,
    digest,
    json_bytes,
    month_of,
    symbol_label,
    timestamp,
    transform,
    validate_version,
)
from .endpoints import endpoints_of, merge_ranges, same_row, session_extent, validator_document
from .storage import (
    STORE_DOCUMENT,
    boundary_document,
    boundary_paths,
    initialize_store,
    load_source,
    publish,
    read_boundary,
    read_document,
    read_validator,
    recover,
    regular_path,
    ticker_lock,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_date(value: str) -> date:
    try:
        day = date.fromisoformat(value)
        if day.isoformat() != value:
            raise ValueError("noncanonical date")
        return day
    except (TypeError, ValueError) as error:
        raise DownloadError("date must be YYYY-MM-DD") from error


def request_end(value: str | None, *, now: datetime | None = None) -> datetime:
    """Freeze a daily-bar boundary only after its session's New York date ends.

    Automatic updates select the latest prior exchange session. Explicit ends
    retain their UTC representation, but cannot include today's New York date.
    """
    instant = utc_now() if now is None else now
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise DownloadError("update clock must include a timezone")
    today = instant.astimezone(NEW_YORK).date()
    if value is None:
        try:
            coverage = expected_market_session_coverage(HISTORY_START[:10], today.isoformat())
        except MarketCalendarError as error:
            raise DownloadError(str(error)) from error
        if coverage.last_session_date is None:
            raise DownloadError("no completed exchange session after the historical lower bound")
        day = parse_date(coverage.last_session_date) + timedelta(days=1)
    else:
        day = parse_date(value)
    if day > today:
        raise DownloadError(
            "request end cannot include the current or a future New York date; "
            "--to must be before today in America/New_York"
        )
    end = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    if end <= timestamp(HISTORY_START):
        raise DownloadError("request end must be after the historical lower bound")
    return end


def _select(views: dict, predicate) -> dict:
    return {
        adjustment: tuple(row for row in views[adjustment] if predicate(month_of(row)))
        for adjustment in ADJUSTMENTS
    }


def _join(left: dict, right: dict) -> dict:
    result = {}
    for adjustment in ADJUSTMENTS:
        rows = left[adjustment] + right[adjustment]
        instants = [timestamp(row[0]) for row in rows]
        if any(first >= second for first, second in zip(instants, instants[1:])):
            raise DownloadError("boundary consolidation would duplicate or reorder observations")
        result[adjustment] = rows
    return result


def _stored_endpoints(root: Path, source: dict) -> dict:
    """Read the first/last effective row per view, opening only needed boundaries."""
    cached = {}

    def rows(kind: str, adjustment: str) -> tuple:
        if kind not in cached:
            views = read_boundary(root, source, kind)
            if kind == "current_year":
                views = _select(views, lambda month: month < source["anchor_month"])
            cached[kind] = views
        return cached[kind][adjustment]

    order = ("completed_years", "current_year", "current_month")
    endpoints = {}
    for adjustment in ADJUSTMENTS:
        first = next((values[0] for kind in order if (values := rows(kind, adjustment))), None)
        last = next((values[-1] for kind in reversed(order) if (values := rows(kind, adjustment))), None)
        endpoints[adjustment] = () if first is None else (first,) if first == last else (first, last)
    return endpoints


def _endpoints_match(client, symbol: str, endpoints: dict, asof: str) -> bool:
    matched = True
    for adjustment, stored in endpoints.items():
        for expected in stored:
            day = timestamp(expected[0]).astimezone(NEW_YORK).date()
            start = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
            end = (start + timedelta(days=1)).astimezone(timezone.utc)
            start = start.astimezone(timezone.utc)
            actual = transform(client.fetch(symbol, adjustment, start, end, asof=asof), start, end)
            # Compare exact values, not incidental JSON number or timezone spelling.
            same = len(actual) == 1 and same_row(actual[0], expected)
            matched = matched and same
    return matched


def _manifest_extent(root: Path, source: dict) -> dict | None:
    """Legacy forced replacement needs only trustworthy receipt dates, not old prices."""
    days = []
    for kind, reference in source["boundaries"].items():
        for entry in boundary_document(root, reference, source["symbol"], kind)["adjustments"]:
            if entry["rows"]:
                days.extend(
                    timestamp(entry[key]).astimezone(NEW_YORK).date().isoformat() for key in ("first", "last")
                )
    return {"from": min(days), "to": max(days)} if days else None


def _overlap_matches(endpoints: dict, views: dict, start: datetime) -> bool:
    for name, rows in endpoints.items():
        acquired = {timestamp(row[0]): row for row in views[name]}
        for row in rows:
            instant = timestamp(row[0])
            if instant >= start and (instant not in acquired or not same_row(row, acquired[instant])):
                return False
    return True


def _incremental_endpoints(endpoints: dict, views: dict, start: datetime) -> dict:
    result = {}
    for name in ADJUSTMENTS:
        old, new = endpoints[name], views[name]
        if not new:
            result[name] = old
        else:
            first = old[0] if old and timestamp(old[0][0]) < start else new[0]
            result[name] = (first,) if first == new[-1] else (first, new[-1])
    return result


def _changed_range(views: dict, previous: dict | None = None) -> dict | None:
    changed = set()
    for adjustment in ADJUSTMENTS:
        new = {timestamp(row[0]): row[1:] for row in views[adjustment]}
        old = {timestamp(row[0]): row[1:] for row in previous[adjustment]} if previous is not None else {}
        changed.update(instant for instant in old.keys() | new.keys() if old.get(instant) != new.get(instant))
    if not changed:
        return None
    return {
        "from": min(changed).astimezone(NEW_YORK).date().isoformat(),
        "to": max(changed).astimezone(NEW_YORK).date().isoformat(),
    }


def updated_boundaries(root, previous, anchor, views, *, full_history=False, read=None):
    """Consolidate acquired month coverage; only rollover reads older boundaries."""
    read = read_boundary if read is None else read
    year_start = anchor[:4] + "-01"
    changed_views = {}
    if full_history:
        # Future consideration: retain clipped edges on force when endpoint values match.
        # Deliberately not implemented: gap/retention semantics need separate review.
        # For now every full refresh, including force, is a clean authoritative replacement.
        changed_views["completed_years"] = _select(views, lambda month: month < year_start)
        changed_views["current_year"] = _select(views, lambda month: month >= year_start)
    else:
        if anchor != previous["anchor_month"]:
            # Supplied final month values replace the year snapshot's older overlap.
            year = read(root, previous, "current_year")
            prefix = _select(year, lambda month: month < previous["anchor_month"])
            consolidated = _join(prefix, views)
            changed_views["current_year"] = _select(consolidated, lambda month: month >= year_start)
            if anchor[:4] != previous["anchor_month"][:4]:
                completed = read(root, previous, "completed_years")
                newly_completed = _select(consolidated, lambda month: month < year_start)
                changed_views["completed_years"] = _join(completed, newly_completed)
    changed_views["current_month"] = _select(views, lambda month: month == anchor)
    return changed_views


def publish_acquired(
    root,
    previous,
    symbol,
    anchor,
    request,
    history_start,
    selected_endpoints,
    changed_views,
    update,
    *,
    repair_payloads=False,
    on_check=None,
):
    """Publish through the original recovery, notice and summary transaction."""
    paths = boundary_paths(anchor)
    references = dict(previous["boundaries"]) if previous is not None else {}
    documents = {}
    payloads = {}
    for kind, content in changed_views.items():
        document, files = build_boundary(symbol, kind, anchor, content, request)
        relative = paths[kind]
        documents[relative] = document
        parent = PurePosixPath(relative).parent
        payloads.update((str(parent / filename), data) for filename, data in files.items())
        references[kind] = {"manifest": relative, "sha256": digest(json_bytes(document))}
    validator = validator_document(symbol, selected_endpoints)
    documents[VALIDATOR_FILE] = validator
    source = {
        "schema_version": VERSION,
        "boundary": "alpaca_standalone_pricing_source",
        "symbol": symbol,
        "anchor_month": anchor,
        "latest_request": request,
        "history_start": history_start.isoformat().replace("+00:00", "Z"),
        "validator": {"file": VALIDATOR_FILE, "sha256": digest(json_bytes(validator))},
        "boundaries": references,
    }
    return publish(
        root,
        previous,
        source,
        documents,
        payloads,
        update=update,
        repair_payloads=repair_payloads,
        on_check=on_check,
    )


def download_pricing(
    store: Path | str,
    symbol: str,
    end_date: str | None,
    *,
    client,
    refresh_all: bool = False,
    from_date: str | None = None,
    on_check=None,
    expected_source_sha256: str | None = None,
) -> dict:
    validate_version()
    symbol = symbol_label(symbol)
    end = request_end(end_date)
    requested_start = (
        timestamp(parse_date(from_date).isoformat() + "T00:00:00Z") if from_date is not None else None
    )
    if requested_start is not None and not timestamp(HISTORY_START) <= requested_start < end:
        raise DownloadError("from date must be on/after 2016-01-01 and before the exclusive end")
    end_text = end.isoformat().replace("+00:00", "Z")
    # With an exclusive midnight, the first of a month still completes the prior month.
    anchor = (end - timedelta(days=1)).strftime("%Y-%m")
    store = Path(store).absolute()
    initialize_store(store)
    source_parent = regular_path(store, "end_of_day/pricing")
    source_parent.mkdir(parents=True, exist_ok=True)
    root = regular_path(store, f"end_of_day/pricing/{symbol}")
    root.mkdir(exist_ok=True)
    with ticker_lock(root, exclusive=True):
        recovered = recover(root, on_check=on_check)
        previous = load_source(root, symbol)
        if previous is None and any(path.name != ".download.lock" for path in root.iterdir()):
            raise DownloadError("ticker directory contains unowned files; refusing acquisition")
        if previous is not None:
            previous_end = timestamp(previous["latest_request"]["end_exclusive"])
            if end < previous_end:
                raise DownloadError("request end cannot move backwards, including during full refresh")
        stored_start = timestamp(previous.get("history_start", HISTORY_START)) if previous is not None else None
        history_start = requested_start or stored_start or timestamp(HISTORY_START)
        if expected_source_sha256 is not None:
            if previous is None or digest(json_bytes(previous)) != expected_source_sha256:
                raise DownloadError("pricing source changed after refresh planning; repeat the live check")
            if (
                not refresh_all
                or history_start != stored_start
                or end != previous_end
                or previous["latest_request"]["asof"] != end.date().isoformat()
            ):
                raise DownloadError("planned replacement must preserve the stored start, end and asof")
        if previous is not None and end == previous_end and history_start == stored_start and not refresh_all:
            # The completed request boundary is authoritative, even for an empty
            # response or a weekend. Advance the boundary (or force) to probe again.
            return {
                "symbol": symbol,
                "anchor_month": previous["anchor_month"],
                "request": previous["latest_request"],
                "mode": "unchanged",
                "consistency_check": "not_required",
                "updated_boundaries": [],
                "files_written": 0,
                "recovered": recovered,
                "requests": 0,
                "changed": False,
            }
        endpoints = read_validator(root, previous) if previous is not None else None
        if endpoints is None:
            endpoints = (
                _stored_endpoints(root, previous)
                if previous is not None and not refresh_all
                else {name: () for name in ADJUSTMENTS}
            )
        old_extent = (
            _manifest_extent(root, previous)
            if previous is not None and refresh_all and not previous.get("validator")
            else session_extent(endpoints)
        )
        if requested_start is not None and old_extent and from_date > old_extent["from"]:
            raise DownloadError(
                f"from date {from_date} would exclude previously stored sessions from {old_extent['from']}"
            )
        extending = stored_start is not None and history_start < stored_start
        check = "not_required"
        if previous is not None and not refresh_all:
            if not any(endpoints.values()):
                check = "no_stored_sessions"
            else:
                check = (
                    "matched"
                    if _endpoints_match(client, symbol, endpoints, end.date().isoformat())
                    else "mismatch"
                )
        full_history = (
            previous is None or refresh_all or extending or check in ("mismatch", "no_stored_sessions")
        )
        restarted = False
        for attempt in range(2):
            start = (
                history_start
                if full_history
                else max(history_start, timestamp(previous["anchor_month"] + "-01T00:00:00Z"))
            )
            request = {
                "symbol": symbol,
                "start": start.isoformat().replace("+00:00", "Z"),
                "end_exclusive": end_text,
                "asof": end.date().isoformat(),
                "feed": "sip",
                "currency": "USD",
                "timeframe": "1Day",
            }
            views = {}
            for adjustment in ADJUSTMENTS:
                try:
                    views[adjustment] = transform(client.fetch(symbol, adjustment, start, end), start, end)
                except BarsSymbolUnavailable as error:
                    if previous is None and attempt == 0 and not views and error.symbol == symbol:
                        raise InitialPricingUnavailable(request, error) from error
                    # Held prices or an earlier successful view are contradictory
                    # evidence, even when that earlier view happened to be empty.
                    raise DownloadError(
                        f"symbol rejected during an established pricing acquisition: {symbol}"
                    ) from error
            if any(
                month_of(row) < start.strftime("%Y-%m") or month_of(row) > anchor
                for rows in views.values()
                for row in rows
            ):
                raise DownloadError("provider rows do not fit the requested session-month range")
            overlap_ok = full_history or _overlap_matches(endpoints, views, start)
            selected_endpoints = (
                endpoints_of(views) if full_history else _incremental_endpoints(endpoints, views, start)
            )
            first_only = {name: rows[:1] for name, rows in selected_endpoints.items()}
            first_ok = _endpoints_match(client, symbol, first_only, end.date().isoformat())
            if overlap_ok and first_ok:
                break
            if attempt:
                raise DownloadError("provider pricing changed during acquisition twice; publication unchanged")
            full_history, restarted, check = True, True, "changed_during_acquisition"
        changed_views = updated_boundaries(root, previous, anchor, views, full_history=full_history)
        update = (
            merge_ranges(old_extent, session_extent(selected_endpoints))
            if full_history
            else _changed_range(views, read_boundary(root, previous, "current_month"))
        )
        written = publish_acquired(
            root,
            previous,
            symbol,
            anchor,
            request,
            history_start,
            selected_endpoints,
            changed_views,
            update,
            repair_payloads=refresh_all,
            on_check=on_check,
        )
        return {
            "symbol": symbol,
            "anchor_month": anchor,
            "request": request,
            "mode": "initial" if previous is None else "full_refresh" if full_history else "update",
            "consistency_check": check,
            "updated_boundaries": list(changed_views),
            "files_written": written,
            "recovered": recovered,
            "restarted": restarted,
            "has_data": any(selected_endpoints.values()),
            "changed": update is not None,
        }


def read_pricing_views(store: Path | str, symbol: str) -> dict:
    """Internal handoff reader: current month replaces its overlap, never appends it."""
    symbol = symbol_label(symbol)
    store = Path(store).absolute()
    if read_document(store / "store.manifest.json") != STORE_DOCUMENT:
        raise DownloadError("not a standalone pricing store")
    root = regular_path(store, f"end_of_day/pricing/{symbol}")
    with ticker_lock(root, exclusive=False):
        return _read_locked_views(root, symbol)


def _read_locked_views(root: Path, symbol: str) -> dict:
    if regular_path(root, ".pending.json").exists():
        raise DownloadError("pricing publication is incomplete; resume the downloader")
    source = load_source(root, symbol)
    if source is None:
        raise DownloadError("no published pricing source")
    completed = read_boundary(root, source, "completed_years")
    year = read_boundary(root, source, "current_year")
    month = read_boundary(root, source, "current_month")
    year_prefix = _select(year, lambda value: value < source["anchor_month"])
    return _join(completed, _join(year_prefix, month))
