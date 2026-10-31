"""Ticker-only acquisition and three-boundary consolidation."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path, PurePosixPath

from libs.common.debug_trace import traced
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .context import accept_context
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
)
from .endpoints import endpoints_of, merge_ranges, same_row, session_extent, validator_document
from .responses import fetch_rows
from .storage import (
    STORE_DOCUMENT,
    boundary_document,
    boundary_paths,
    load_source,
    publish,
    read_boundary,
    read_document,
    read_update,
    read_validator,
    recover,
    regular_path,
    ticker_lock,
)

_SOURCE_NOT_SELECTED = object()


def acquisition_asof(source, end):
    """Continue the producer's accepted mapping independently of request coverage."""
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Ordinary current-label requests continue moving with their cutoff. Only a
    # deliberately historical mapping is pinned; its completed receipt owns it.
    if source is not None:
        request = source["latest_request"]
        if request["asof"] != request["end_exclusive"][:10]:
            return request["asof"]
    return end.date().isoformat()


def established_pricing_rejection(symbol, error):
    failure = DownloadError(f"symbol rejected during an established pricing acquisition: {symbol}")
    failure.__cause__ = error
    return failure


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


def _session_end_before(day: date) -> datetime:
    try:
        coverage = expected_market_session_coverage(HISTORY_START[:10], day.isoformat())
    except MarketCalendarError as error:
        raise DownloadError(str(error)) from error
    if coverage.last_session_date is None:
        raise DownloadError("no completed exchange session after the historical lower bound")
    end_day = parse_date(coverage.last_session_date) + timedelta(days=1)
    return datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc)


def request_end(value: str | None, *, now: datetime | None = None) -> datetime:
    """Validate an already selected exclusive boundary without changing accepted work.

    With no boundary, select the latest completed session. New user cutoffs must
    use pricing_session_end; existing intentions and publications retain their
    exact boundaries, including older calendar-range coverage.
    """
    instant = utc_now() if now is None else now
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise DownloadError("update clock must include a timezone")
    today = instant.astimezone(NEW_YORK).date()
    if value is None:
        return _session_end_before(today)
    day = parse_date(value)
    if day > today:
        raise DownloadError(
            "request end cannot include the current or a future New York date; "
            "--through must be before today in America/New_York"
        )
    end = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    if end <= timestamp(HISTORY_START):
        raise DownloadError("request end must be after the historical lower bound")
    return end


def pricing_session_end(through: str | None, *, now: datetime | None = None) -> datetime:
    """Resolve an inclusive user cutoff to its last completed exchange session.

    Resolve before selection, registration or requests. The shared calendar
    covers weekends, holidays and exceptional closures; no weekday guessing.
    The New York completion rule and unknown-calendar rejection still apply.
    """
    if through is None:
        return request_end(None, now=now)
    end = request_end((parse_date(through) + timedelta(days=1)).isoformat(), now=now)
    return _session_end_before(end.date())


def request_start(value: str | None, end: datetime) -> datetime | None:
    """Validate an explicit acquisition start before accepting or executing work."""
    if value is None:
        return None
    start = timestamp(parse_date(value).isoformat() + "T00:00:00Z")
    if not timestamp(HISTORY_START) <= start < end:
        raise DownloadError("from date must be on/after 2016-01-01 and before the exclusive end")
    return start


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


def _endpoint_window(row, asof, *, cutoff=None):
    day = timestamp(row[0]).astimezone(NEW_YORK).date()
    start = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Mapping asof chooses an entity, not the end of its observations. A retired
    # label can map a request containing sessions later than that mapping date.
    end = (start + timedelta(days=1)).astimezone(timezone.utc)
    # The accepted observation cutoff still bounds every request, including a
    # truncated final New York session. It is independent of entity mapping.
    if cutoff is not None:
        end = min(end, cutoff)
    return start.astimezone(timezone.utc), end


def _endpoints_match(client, symbol: str, endpoints: dict, asof: str, *, cutoff=None) -> bool:
    matched = True
    for adjustment, stored in endpoints.items():
        for expected in stored:
            start, end = _endpoint_window(expected, asof, cutoff=cutoff)
            actual = fetch_rows(client, symbol, adjustment, start, end, asof=asof)
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


@traced("pricing_prepare_publication")
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
    acquisition_evidence_sha256=None,
    history_correction_id=None,
    old_documents=None,
):
    """Publish through the original recovery, notice and summary transaction."""
    from .activity import validate_activity
    from .changes import change_history

    paths = boundary_paths(anchor)
    references = dict(previous["boundaries"]) if previous is not None else {}
    documents = {}
    payloads = {}
    # New partitions are validated at this producer. Retained boundary
    # references remain authoritative; DO NOT reopen them to extend this check.
    for kind, content in changed_views.items():
        validate_activity(content)
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
        "history_changes": change_history(previous, update),
        # DO NOT REMOVE WITHOUT USER AUTH!#
        # Changed partitions have just passed the producer's activity contract.
        # An append cannot certify untouched legacy partitions. Carry this
        # guarantee only from a completed guarantee or a complete replacement.
        "activity_consistent": bool((previous or {}).get("activity_consistent"))
        or set(changed_views) == set(references),
    }
    evidence = acquisition_evidence_sha256 or (previous or {}).get("acquisition_evidence_sha256")
    if evidence is not None:
        if len(evidence) != 64 or any(c not in "0123456789abcdef" for c in evidence):
            raise DownloadError("invalid acquisition evidence fingerprint")
        source["acquisition_evidence_sha256"] = evidence
    correction = history_correction_id or (previous or {}).get("history_correction_id")
    if correction is not None:
        source["history_correction_id"] = correction
    if previous is not None and {key: value for key, value in source.items() if key != "history_changes"} == {
        key: value for key, value in previous.items() if key != "history_changes"
    }:
        # Identical selected output is already complete. A repeated acquisition
        # notice must not manufacture a new generation solely for its ancestry.
        source["history_changes"] = previous["history_changes"]
    handoff = {}
    written = publish(
        root,
        previous,
        source,
        documents,
        payloads,
        update=update,
        repair_payloads=repair_payloads,
        on_check=on_check,
        endpoints=selected_endpoints,
        old_documents=old_documents,
        handoff=handoff,
    )
    # Publication has completed before returning these accepted outputs.
    # Consumers must use the source/endpoints/notice handoff, not reload or
    # re-authenticate the files the publisher just selected.
    return {
        "files_written": written,
        "source": source,
        "source_sha256": digest(json_bytes(source)),
        "endpoints": selected_endpoints,
        "pending_update": handoff["pending_update"],
    }


@traced("pricing_download")
def download_pricing(
    store: Path | str,
    symbol: str,
    end_date: str | None,
    *,
    client,
    refresh_all: bool = False,
    repair_payloads: bool = False,
    from_date: str | None = None,
    on_check=None,
    expected_source_sha256: str | None = None,
    selected_source_sha256=_SOURCE_NOT_SELECTED,
    acquisition_evidence_sha256: str | None = None,
    history_correction_id: str | None = None,
    mapping_asof: str | None = None,
    context=None,
) -> dict:
    """Acquire under the ticker lock, optionally bound to a prefetch selection.

    A selected digest binds buffered acquisition to that source; explicit None
    binds it to absence. The separate refresh expectation also preserves the
    complete original request span. History refresh trusts recorded payload
    digests; only explicit payload repair forces identical output to be rewritten.
    """
    symbol = symbol_label(symbol)
    end = request_end(end_date)
    requested_start = request_start(from_date, end)
    end_text = end.isoformat().replace("+00:00", "Z")
    # With an exclusive midnight, the first of a month still completes the prior month.
    anchor = (end - timedelta(days=1)).strftime("%Y-%m")
    store = Path(store).absolute()
    context = accept_context(store, context)
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    with ExitStack() as locks:
        try:
            locks.enter_context(ticker_lock(root, exclusive=True))
        except FileNotFoundError:
            # Only acquisition of a new ticker creates and validates its paths.
            regular_path(store, "end_of_day/pricing").mkdir(parents=True, exist_ok=True)
            regular_path(store, f"end_of_day/pricing/{symbol}").mkdir(exist_ok=True)
            locks.enter_context(ticker_lock(root, exclusive=True, create=True))
        recovered = recover(root, on_check=on_check)
        previous = load_source(root, symbol)
        # DO NOT REMOVE WITHOUT USER AUTH!#
        # Preserve an explicitly established entity mapping across retries and
        # cutoff extensions. Advancing time must not silently turn a retired
        # mapped request back into Alpaca's symbol-only fallback.
        asof = mapping_asof or acquisition_asof(previous, end)
        parse_date(asof)
        if mapping_asof is not None and not refresh_all:
            raise DownloadError("changing a pricing mapping requires complete replacement")
        # Acquisition may have run outside this lock. Its selected generation
        # (including explicit absence) must still be current before publication;
        # matching endpoints alone do not establish that concurrency guarantee.
        if selected_source_sha256 is not _SOURCE_NOT_SELECTED:
            current_sha256 = digest(json_bytes(previous)) if previous is not None else None
            if current_sha256 != selected_source_sha256:
                raise DownloadError(
                    "pricing source changed after acquisition selection; reselect before retrying"
                )
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
                or previous["latest_request"]["asof"] != asof
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
                "source_sha256": digest(json_bytes(previous)),
                "recovered": recovered,
                "requests": 0,
                "changed": False,
                "history_start": previous.get("history_start", HISTORY_START),
                "pending_update": read_update(root, published=True),
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
        prior_nonempty = {name for name, rows in endpoints.items() if rows}
        if not prior_nonempty and old_extent:
            prior_nonempty = set(ADJUSTMENTS)  # Legacy receipts establish data without per-view endpoints.
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
                    "matched" if _endpoints_match(client, symbol, endpoints, asof, cutoff=end) else "mismatch"
                )
        full_history = previous is None or refresh_all or extending or check == "mismatch"
        restarted = False
        for attempt in range(2):
            start = (
                history_start
                if full_history
                else previous_end
                if check == "no_stored_sessions"
                else max(history_start, timestamp(previous["anchor_month"] + "-01T00:00:00Z"))
            )
            request = {
                "symbol": symbol,
                "start": start.isoformat().replace("+00:00", "Z"),
                "end_exclusive": end_text,
                "asof": asof,
                "feed": "sip",
                "currency": "USD",
                "timeframe": "1Day",
            }
            views = {}
            for adjustment in ADJUSTMENTS:
                try:
                    views[adjustment] = fetch_rows(
                        client,
                        symbol,
                        adjustment,
                        start,
                        end,
                        asof=asof if asof != end.date().isoformat() else None,
                        acquisition=True,
                    )
                except BarsSymbolUnavailable as error:
                    if previous is None and attempt == 0 and not views and error.symbol == symbol:
                        raise InitialPricingUnavailable(request, error) from error
                    # Held prices or an earlier successful view are contradictory
                    # evidence, even when that earlier view happened to be empty.
                    raise established_pricing_rejection(symbol, error) from error
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
            first_ok = _endpoints_match(client, symbol, first_only, asof, cutoff=end)
            empties_history = full_history and any(
                name in prior_nonempty and not rows for name, rows in views.items()
            )
            # Removing published history requires a fresh confirmation. The planned
            # response is consumed once, so the bounded retry makes exact requests.
            # Ordinary empty appends must never enter this replacement-only guard.
            if overlap_ok and first_ok and not (empties_history and not attempt):
                break
            if attempt:
                raise DownloadError("provider pricing changed during acquisition twice; publication unchanged")
            full_history, restarted, check = True, True, "changed_during_acquisition"
        old_documents = {}
        read_selected = partial(read_boundary, documents=old_documents)
        changed_views = updated_boundaries(
            root, previous, anchor, views, full_history=full_history, read=read_selected
        )
        update = (
            merge_ranges(old_extent, session_extent(selected_endpoints))
            if full_history
            else _changed_range(views, read_selected(root, previous, "current_month"))
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
            repair_payloads=repair_payloads,
            on_check=on_check,
            acquisition_evidence_sha256=acquisition_evidence_sha256,
            history_correction_id=history_correction_id,
            old_documents=old_documents,
        )
        return {
            "symbol": symbol,
            "anchor_month": anchor,
            "request": request,
            "mode": "initial" if previous is None else "full_refresh" if full_history else "update",
            "consistency_check": check,
            "updated_boundaries": list(changed_views),
            "files_written": written["files_written"],
            "source_sha256": written["source_sha256"],
            "recovered": recovered,
            "restarted": restarted,
            "has_data": any(selected_endpoints.values()),
            "changed": update is not None,
            "history_start": written["source"]["history_start"],
            "pending_update": written["pending_update"],
        }


def read_pricing_views(store: Path | str, symbol: str) -> dict:
    """Internal handoff reader: current month replaces its overlap, never appends it."""
    symbol = symbol_label(symbol)
    store = Path(store).absolute()
    if read_document(store / "store.manifest.json") != STORE_DOCUMENT:
        raise DownloadError("not a standalone pricing store")
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
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
