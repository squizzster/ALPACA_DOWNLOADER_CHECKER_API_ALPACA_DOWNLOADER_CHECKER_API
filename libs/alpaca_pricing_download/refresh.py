"""Batched acquisition and guarded replacement of existing pricing spans."""

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from libs.alpaca_data_api_solver import build_plan
from libs.common.debug_trace import event, traced
from libs.common.provider_http import balanced_batches

from .adaptive import AdjustmentAcquisition, acquire_adjustments
from .context import accept_context
from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    NEW_YORK,
    BarsSymbolUnavailable,
    DownloadError,
    digest,
    failure_outcome,
    json_bytes,
    symbol_label,
    timestamp,
)
from .correction import ProviderObservationConflict, accept_observations, applicable_contract
from .correction import completed as completed_correction
from .correction import observations as correction_observations
from .download import (
    _endpoint_window,
    acquisition_asof,
    download_pricing,
    established_pricing_rejection,
    publish_acquired,
    publish_selected_session_correction,
    request_end,
)
from .endpoints import EndpointSelection, comparison_rows, fetch_history, history_windows
from .failures import AcquisitionFailure
from .history_tail import publish_legacy_tail
from .late_bars import session_ranges
from .responses import CompletedBars, completed_bars
from .storage import load_source, read_validator, regular_path, ticker_lock, trace_source_selection


@traced("retained_history_check")
def check_retained_source(store, symbol, end_date, *, client, pending=None):
    """Check a selected source independently of active-summary eligibility."""
    store = Path(store).absolute()
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    pending = pending or {}
    with ticker_lock(root, exclusive=True):
        if regular_path(root, ".pending.json").exists():
            raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
        source = load_source(root, symbol)
        if source is None:
            raise DownloadError(f"{symbol}: no existing pricing to check")
        request = _refresh_request(symbol, source)
        if request["end_exclusive"][:10] > end_date:
            raise DownloadError(f"{symbol}: required history check exceeds requested cutoff")
        correction_id = pending.get("correction_id")
        contract = pending.get("pricing_correction")
        if correction_id is not None and source.get("history_correction_id") == correction_id:
            if source.get("history_correction_scope") != "selected_sessions":
                return {**request, "matched": True, "completed_scope": "full_history"}
            # Only the missing-session insertion is complete. It established no
            # retained-history comparison and cannot lend that broader approval.
            # The completed insertion itself must not be acquired or recovered again.
            correction_id = None
        completion = pending.get("correction_completion")
        if completion is not None and (completion.get("correction_id"), completion.get("source_sha256")) != (
            correction_id,
            request["source_sha256"],
        ):
            completion = None
        if completion is None:
            completion = completed_correction(root, correction_id, request["source_sha256"])
        if completion is not None:
            return {
                **request,
                "matched": True,
                "completed_correction": True,
                "required_correction_id": correction_id,
                "correction_completion": completion,
            }
        if correction_id is not None:
            if contract is not None and (
                contract.get("schema_version") not in (1, 2, 3)
                or contract.get("scope") not in ("latest_session", "full_history")
            ):
                raise DownloadError("unsupported pending pricing correction; owning recovery required")
            applicable = applicable_contract(
                contract,
                store=store,
                symbol=symbol,
                generation=request["source_sha256"],
                start=request["start"],
                end=request["end_exclusive"],
                asof=request["asof"],
            )
            if contract is None or (applicable and contract["scope"] == "full_history"):
                # The old generic correction ID does not establish a narrower
                # cause. Its existing replacement obligation remains authoritative.
                return {
                    **request,
                    "matched": False,
                    "required_correction_id": correction_id,
                    "correction_scope": "full_history",
                    "correction_cause": (contract or {}).get("cause", "legacy_unspecified_replacement"),
                    "correction_observations": (contract or {}).get("observations", {}),
                }
            selection = EndpointSelection.from_document(contract["selection"]) if applicable else None
            if selection is not None and selection.applies(
                store, symbol, request["source_sha256"], contract["end_exclusive"], contract["asof"]
            ):
                if contract["end_exclusive"][:10] > end_date:
                    raise DownloadError("pending correction exceeds the requested cutoff")
                return {
                    **request,
                    "asof": contract["asof"],
                    "end_exclusive": contract["end_exclusive"],
                    "comparison_end_exclusive": contract["comparison_end_exclusive"],
                    "comparison_asof": selection.comparison_asof or selection.asof,
                    "matched": False,
                    "comparison": selection.comparison,
                    "correction_scope": selection.comparison.decision,
                    "correction_cause": contract["cause"],
                    "endpoint_selection": selection,
                }
        endpoints = read_validator(root, source, published=True)
        if endpoints is None:
            raise DownloadError(f"{symbol}: retained-history checking requires published endpoints")
        if endpoints.tail_all is None:
            publication, endpoints = publish_legacy_tail(
                root, source, endpoints, source_sha256=request["source_sha256"]
            )
            source = publication["source"]
            request = _refresh_request(symbol, source, source_sha256=publication["source_sha256"])
        comparison = fetch_history(
            client,
            symbol,
            endpoints,
            request["asof"],
            cutoff=timestamp(request["end_exclusive"]),
        )
        if comparison.matched and correction_id is not None:
            # A changed generation invalidated a scoped pending decision. Its
            # fresh completed comparison now establishes unchanged current rows;
            # publish only the obligation acknowledgement, never a history fetch.
            publication = publish_acquired(
                root,
                source,
                symbol,
                source["anchor_month"],
                source["latest_request"],
                timestamp(source.get("history_start", HISTORY_START)),
                endpoints,
                {},
                None,
                history_correction_id=correction_id,
                previous_source_sha256=request["source_sha256"],
            )
            request = _refresh_request(symbol, publication["source"], source_sha256=publication["source_sha256"])
            return {
                **request,
                "matched": True,
                "completed_correction": True,
                "required_correction_id": correction_id,
                "correction_completion": publication.get("correction_completion"),
            }
        result = {
            **request,
            "matched": comparison.matched,
            "comparison": comparison,
            "correction_scope": comparison.decision,
            "correction_cause": "selected_all_rows_changed",
        }
        if comparison.decision in ("latest_session", "full_history"):
            result.update(
                comparison_asof=request["asof"],
                comparison_end_exclusive=request["end_exclusive"],
                endpoint_selection=EndpointSelection(
                    str(store),
                    symbol,
                    request["source_sha256"],
                    request["end_exclusive"],
                    request["asof"],
                    tuple(endpoints.items()),
                    matched=False,
                    tail_all=endpoints.tail_all,
                    comparison=comparison,
                    comparison_asof=request["asof"],
                ),
            )
        return result


def _refresh_request(symbol, source, *, source_sha256=None):
    start = timestamp(source.get("history_start", HISTORY_START))
    end = timestamp(source["latest_request"]["end_exclusive"])
    asof = source["latest_request"]["asof"]
    if start != datetime.combine(
        start.date(), datetime.min.time(), tzinfo=timezone.utc
    ) or end != datetime.combine(end.date(), datetime.min.time(), tzinfo=timezone.utc):
        raise DownloadError(f"{symbol}: stored range/asof is incompatible with exact-span replacement")
    return {
        "symbol": symbol,
        "source_sha256": source_sha256 or digest(json_bytes(source)),
        "history_correction_id": source.get("history_correction_id"),
        "history_correction_scope": source.get("history_correction_scope"),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end_exclusive": end.isoformat().replace("+00:00", "Z"),
        "asof": asof,
    }


def prepare_refresh(
    store,
    symbols,
    *,
    comparisons=None,
    approved_generations=None,
    selections=None,
    end_date=None,
    correction_ids=None,
):
    """Read only selected receipts; retain the complete original request coverage."""
    # Selection is already owned by the live-check/correction caller. Read only
    # those ticker receipts and carry their generation/scope into replacement;
    # DO NOT widen an explicit correction into a retained-store inventory.
    store = Path(store).absolute()
    prepared = []
    for symbol in sorted({symbol_label(value) for value in symbols}):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        with ticker_lock(root, exclusive=False):
            if regular_path(root, ".pending.json").exists():
                raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
            source = load_source(root, symbol)
            decision = (comparisons or {}).get(symbol)
            selection = (selections or {}).get(symbol)
            endpoints = (
                read_validator(root, source, published=True)
                if source
                and decision is not None
                and decision.decision in ("latest_session", "unchanged")
                and selection is None
                else None
            )
        if source is None:
            raise DownloadError(f"{symbol}: no existing pricing to replace")
        request = _refresh_request(symbol, source)
        correction_id = (correction_ids or {}).get(symbol)
        if correction_id and source.get("history_correction_id") == correction_id:
            request["completed_correction"] = True
            request["required_correction_id"] = correction_id
        approved = (approved_generations or {}).get(symbol)
        if approved is not None and approved != request["source_sha256"]:
            raise DownloadError(f"{symbol}: pricing changed after completed comparison")
        if decision is not None:
            request["comparison"] = decision
            request["correction_scope"] = decision.decision
            request["correction_cause"] = (
                "selected_all_rows_unchanged" if decision.matched else "selected_all_rows_changed"
            )
        else:
            request["correction_scope"] = "full_history"
            request["correction_cause"] = "retained_replacement_required"
        if request["correction_scope"] == "latest_session":
            request["comparison_end_exclusive"] = request["end_exclusive"]
            request["comparison_asof"] = request["asof"]
            target = request_end(end_date) if end_date else timestamp(request["end_exclusive"])
            if selection is not None:
                target = timestamp(selection.end)
            if target < timestamp(request["end_exclusive"]):
                raise DownloadError("correction target cannot precede selected coverage")
            request["end_exclusive"] = target.isoformat().replace("+00:00", "Z")
            request["asof"] = acquisition_asof(source, target)
        if endpoints is not None:
            request["endpoint_selection"] = EndpointSelection(
                str(store),
                symbol,
                request["source_sha256"],
                request["end_exclusive"],
                request["asof"],
                tuple(endpoints.items()),
                matched=False,
                tail_all=endpoints.tail_all,
                comparison=decision,
                comparison_asof=request.get("comparison_asof", request["asof"]),
            )
        elif selection is not None:
            selection.consume(store, symbol, request["source_sha256"], request["end_exclusive"], request["asof"])
            request["endpoint_selection"] = selection
        trace_source_selection(source, request["source_sha256"], symbol=symbol, reason="replacement_selection")
        prepared.append(request)
    return prepared


@traced("provider_late_bar_check")
def probe_late_bar_repairs(
    store,
    selection,
    *,
    client,
    end_date=None,
    progress=None,
    context=None,
    on_positive=None,
    correction_requirements=None,
):
    """Probe only exact sessions named by a completed serving handoff.

    A positive raw-bar response selects the ordinary generation-bound full-span
    replacement. Empty responses leave the completed stored decision untouched.
    """
    context = accept_context(store, context)
    sources = selection["sources"]
    report = {
        "catalogue_generation": selection["catalogue_generation"],
        "inspected_groups": selection["inspected_groups"],
        "selected_groups": selection["selected_groups"],
        "selected_identities": selection["selected_identities"],
        "selected_sources": len(sources),
        "requested_sources": 0,
        "requested_sessions": 0,
        "returned_sources": 0,
        "returned_sessions": 0,
        "stale_sources": [],
        "beyond_target": [],
        "outside_retained_span": 0,
        "failures": [],
        "reused_ranges": 0,
        "requests": {},
    }
    if not sources:
        return [], report
    prepared = {item["symbol"]: item for item in prepare_refresh(store, sources)}
    needs, selected_sessions = [], {}
    for symbol, selected in sorted(sources.items()):
        request = prepared[symbol]
        if request["source_sha256"] != selected["source_sha256"]:
            report["stale_sources"].append(symbol)
            continue
        if end_date is not None and request["end_exclusive"][:10] > end_date:
            report["beyond_target"].append(symbol)
            continue
        sessions = [
            value
            for value in selected["sessions"]
            if request["start"][:10] <= value < request["end_exclusive"][:10]
        ]
        report["outside_retained_span"] += len(selected["sessions"]) - len(sessions)
        report["requested_sessions"] += len(sessions)
        selected_sessions[symbol] = set(sessions)
        planned = selected.get("planned")
        if planned is not None and (
            planned["asof"],
            timestamp(planned["start"]),
            timestamp(planned["end_exclusive"]),
            planned["sessions"],
        ) == (request["asof"], timestamp(request["start"]), timestamp(request["end_exclusive"]), sessions):
            ranges = planned["ranges"]
        else:
            ranges = session_ranges(
                sessions,
                coalesce=selection.get("window_policy") != "exact_sessions",
                cutoff=request["end_exclusive"],
            )
        if ranges:
            needs.append(
                {
                    "kind": "bars",
                    "why": "provider_late_bar_check",
                    "symbols": [symbol],
                    "asof": request["asof"],
                    "adjustments": ["raw"],
                    "ranges": ranges,
                }
            )
    report["requested_sources"] = len(needs)
    if not needs:
        return [], report

    def key(symbol, window, asof):
        return (False, symbol, "raw", timestamp(window["start"]), timestamp(window["end_exclusive"]), asof)

    def binding(symbol, window, asof):
        return (prepared[symbol]["source_sha256"], *key(symbol, window, asof))

    pending = []
    for need in needs:
        symbol = need["symbols"][0]
        ranges = [
            window
            for window in need["ranges"]
            if binding(symbol, window, need["asof"]) not in context.late_bar_observations
        ]
        report["reused_ranges"] += len(need["ranges"]) - len(ranges)
        if ranges:
            pending.append({**need, "ranges": ranges})

    def failed(_step, action, error):
        event(
            "late_bar_action_failed",
            symbols=action["request"]["params"]["symbols"],
            asof=action["request"]["params"]["asof"],
            start=action["request"]["params"]["start"],
            end=action["request"]["params"]["end"],
            outcome=failure_outcome(error),
        )
        report["failures"].append(
            {
                "symbols": sorted({s for target in action["deliver"] for s in target["symbols"]}),
                "error": str(error),
                "outcome": failure_outcome(error),
            }
        )

    from .correction import merge_observations

    returned, positive_dates = {}, {}

    def requirement_binding(symbol):
        request = prepared[symbol]
        return (
            symbol,
            request["source_sha256"],
            request["start"],
            request["end_exclusive"],
            request["asof"],
            tuple(sorted(selected_sessions[symbol])),
        )

    def completed(delta):
        new = {}
        for cache_key, value in delta.items():
            symbol = cache_key[1]
            rows = [
                row
                for row in value.rows
                if timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() in selected_sessions[symbol]
            ]
            if rows:
                new.setdefault(symbol, []).extend(rows)
        requests = [
            {
                **prepared[symbol],
                "correction_cause": "provider_late_observations",
                "correction_observations": {"raw": rows},
            }
            for symbol, rows in sorted(new.items())
        ]
        if requests and on_positive is not None:
            requests = on_positive(requests)
        for symbol, rows in new.items():
            event(
                "late_bar_action_positive",
                symbol=symbol,
                rows=len(rows),
                source_generation=prepared[symbol]["source_sha256"],
                durable=on_positive is not None,
            )
        for request in requests:
            symbol = request["symbol"]
            if on_positive is None:
                request["correction_observations"] = merge_observations(
                    returned.get(symbol, {}).get("correction_observations", {}),
                    request["correction_observations"],
                )
            returned[symbol] = request
        for symbol, rows in new.items():
            positive_dates.setdefault(symbol, set()).update(
                timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows
            )
            context.late_bar_requirements[requirement_binding(symbol)] = (
                returned[symbol],
                frozenset(positive_dates[symbol]),
            )
        # Commit the positive obligation before exposing completed action reuse.
        for cache_key, value in delta.items():
            context.late_bar_observations[(prepared[cache_key[1]]["source_sha256"], *cache_key)] = value

    reused = {}
    for need in needs:
        symbol = need["symbols"][0]
        for window in need["ranges"]:
            value = context.late_bar_observations.get(binding(symbol, window, need["asof"]))
            if value is not None:
                reused[key(symbol, window, need["asof"])] = value
    # The response and the successful durable handoff are separate completed
    # facts. Retrying a failed peer consumes both without invoking the owner or
    # merging its already accepted evidence again. A renewed requirement needs
    # one owning merge, using those same accepted responses rather than HTTP.
    reuse_pending = {}
    for need in needs:
        symbol = need["symbols"][0]
        accepted = context.late_bar_requirements.get(requirement_binding(symbol))
        if accepted is not None and (
            correction_requirements is None
            or accepted[0].get("required_correction_id")
            == correction_requirements.get(symbol, {}).get("correction_id")
        ):
            returned[symbol], dates = accepted
            positive_dates[symbol] = set(dates)
        else:
            reuse_pending.update((key, value) for key, value in reused.items() if key[1] == symbol)
    if reuse_pending:
        completed(reuse_pending)
    if pending:
        _prefetch(
            pending,
            client,
            check=False,
            metrics=report["requests"],
            progress=progress,
            merge_ranges=False,
            on_failure=failed,
            on_completed=completed,
        )
    report["returned_sources"] = len(returned)
    report["returned_sessions"] = sum(map(len, positive_dates.values()))
    return [
        {**request, "provider_late_sessions": sorted(positive_dates[symbol])}
        for symbol, request in sorted(returned.items())
    ], report


class PlannedClient:
    """Replay each buffered request once; retries use fresh provider responses."""

    def __init__(
        self, client, cache, adjustments=None, *, metrics=None, progress=None, correction_approvals=None
    ):
        self.client, self.cache = client, cache
        self.adjustments = adjustments or {}
        self.metrics, self.progress = metrics, progress
        self.correction_approvals = correction_approvals or {}

    def confirmed_observations(self, acquired, selected):
        key = (acquired.symbol, acquired.start, acquired.end, acquired.asof)
        approval = self.correction_approvals.pop(key, None)
        if (
            approval is not None
            and approval[0] is acquired
            and approval[1].keys() == selected.keys()
            and all(approval[1][name] is rows for name, rows in selected.items())
        ):
            if isinstance(approval[2], ProviderObservationConflict):
                raise approval[2]
            return approval[2]
        inherited = getattr(self.client, "confirmed_observations", None)
        return inherited(acquired, selected) if inherited else None

    def take_adjustments(self, symbol, start, end, asof):
        key = symbol, start, end, asof
        if key in self.adjustments:
            return self.adjustments.pop(key)
        take = getattr(self.client, "take_adjustments", None)
        return take(symbol, start, end, asof) if take else AdjustmentAcquisition(*key)

    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        key = (asof is not None, symbol, adjustment, start, end, asof or end.date().isoformat())
        if key in self.cache:
            return self.cache.pop(key)
        if self.metrics is not None:
            # Deferred producer confirmations still use the common executor and
            # report their pages. Trigger evidence is accepted before these run.
            cached, _ = _prefetch(
                [
                    {
                        "kind": "bars",
                        "why": "acquisition_check",
                        "symbols": [symbol],
                        "adjustments": [adjustment],
                        "asof": key[-1],
                        "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
                    }
                ],
                self.client,
                check=key[0],
                metrics=self.metrics,
                progress=self.progress,
                merge_ranges=False,
            )
            return cached[key]
        return self.client.fetch(symbol, adjustment, start, end, asof=asof)

    def fetch_acquired(self, symbol, adjustment, start, end, *, asof=None):
        # DO NOT REMOVE WITHOUT USER AUTH!#
        # Consume the completed acquisition exactly once. Endpoint probes retain
        # their separate cache entry even for a one-session acquisition.
        key = (False, symbol, adjustment, start, end, asof or end.date().isoformat())
        if key in self.cache:
            return self.cache.pop(key)
        fetch = getattr(self.client, "fetch_acquired", self.client.fetch)
        return fetch(symbol, adjustment, start, end, asof=asof)


def _endpoint_needs(symbol, endpoints, asof, why, *, cutoff=None):
    return [
        {
            "kind": "bars",
            "why": why,
            "symbols": [symbol],
            "asof": asof,
            "adjustments": [adjustment],
            "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
        }
        for adjustment, rows in endpoints.items()
        for row in rows
        for start, end in [_endpoint_window(row, asof, cutoff=cutoff)]
    ]


def _prefetch(
    needs, client, *, check, metrics, progress=None, on_failure=None, merge_ranges=True, on_completed=None
):
    started = monotonic()
    pages_before = (metrics or {}).get("page_requests", 0)
    # Endpoint checks select exact sessions. Saving a page must not expand two
    # points into years of data that the executor normalizes and then discards.
    # Compatible identical windows still batch symbols and share pagination.
    plan = build_plan({"steps": [{"needs": needs}]}, merge_ranges=merge_ranges and not check)
    stage = needs[0]["why"] if needs else "acquisition_check"
    actions = plan["steps"][0]["actions"]
    if progress:
        progress(
            stage,
            {
                "state": "started",
                "symbols": len({need["symbols"][0] for need in needs}),
                "actions": len(actions),
                "minimum_pages": sum(
                    len(balanced_batches(action["request"]["params"]["symbols"].split(",")))
                    for action in actions
                ),
            },
        )
    options = {"on_failure": on_failure} if on_failure is not None else {}
    completed_cache = {}
    if on_completed is not None:

        def deliver(_step, _action, values):
            delta = {}
            for (_, index, offset, symbol, adjustment), value in values.items():
                need = needs[index]
                window = need["ranges"][offset]
                key = (
                    check,
                    symbol,
                    adjustment,
                    timestamp(window["start"]),
                    timestamp(window["end_exclusive"]),
                    need["asof"],
                )
                delta[key] = value
            on_completed(delta)
            completed_cache.update(delta)

        options["on_completed"] = deliver
    try:
        acquired = client.execute_plan(plan, metrics=metrics, **options)
    except (RuntimeError, OSError, ValueError):
        if progress:
            progress(
                stage,
                {
                    "state": "failed",
                    "seconds": monotonic() - started,
                    "page_requests": (metrics or {}).get("page_requests", 0) - pages_before,
                },
            )
        raise
    if progress:
        progress(
            stage,
            {
                "state": "completed",
                "seconds": monotonic() - started,
                "page_requests": (metrics or {}).get("page_requests", 0) - pages_before,
            },
        )
    if on_completed is not None:
        return completed_cache, plan
    cache = {}
    for index, need in enumerate(needs):
        symbol = need["symbols"][0]
        for adjustment in need["adjustments"]:
            for offset, window in enumerate(need["ranges"]):
                key = (0, index, offset, symbol, adjustment)
                if on_failure is not None and key not in acquired:
                    continue  # The caller retains the failure and excludes this ticker.
                cache[
                    (
                        check,
                        symbol,
                        adjustment,
                        timestamp(window["start"]),
                        timestamp(window["end_exclusive"]),
                        need["asof"],
                    )
                ] = completed_bars(
                    acquired[key],
                    symbol,
                    adjustment,
                    timestamp(window["start"]),
                    timestamp(window["end_exclusive"]),
                    need["asof"],
                )
    return cache, plan


@traced("pricing_full_history")
def acquire_full(
    needs,
    client,
    *,
    metrics=None,
    retained_endpoints=None,
    progress=None,
    on_failure=None,
    acquired=None,
    observations=None,
    required_observations=None,
):
    """Acquire shared ranges; only new/replaced histories need producer confirmation.

    The returned client feeds the normal downloader; its existing bounded retry
    requests a fresh plan if the provider changed during acquisition.
    """
    states, acquisition = acquire_adjustments(
        needs,
        client,
        metrics=metrics,
        progress=progress,
        on_failure=on_failure,
        acquired=acquired,
        observations=observations,
    )
    probes, correction_approvals = [], {}
    for need, state in zip(needs, states, strict=True):
        symbol, end = state.symbol, state.end
        if state.completed is None:
            continue
        if symbol in (retained_endpoints or {}):
            # A completed first/latest all comparison already approved this
            # generation. These are new-session rows only. No historical probes.
            continue
        required = (required_observations or {}).get(symbol, {})
        confirmed = {}
        if required:
            if any(not rows for rows in state.completed.views.values()):
                continue  # The locked downloader owns the bounded empty-view fallback.
            try:
                confirmed = accept_observations(
                    symbol, state.completed.views, required, allow_all_context_conflicts=True
                )
            except ProviderObservationConflict as error:
                # Carry the completed contradiction to the locked publisher.
                # Only that owner can determine, from accepted logical digests,
                # whether this exact full-range output reproduced the retained
                # generation. Do not repeat either provider observation.
                confirmed = getattr(error, "confirmed_observations", None)
                correction_approvals[state.key] = (state.completed, required, error)
                if confirmed is None:
                    # This completed failure cannot be resolved by retained
                    # equality. Carry it to the selected publisher without
                    # spending requests on irrelevant first-row confirmations.
                    continue
            else:
                correction_approvals[state.key] = (state.completed, required, confirmed)
        endpoints = state.completed.endpoints
        # These are fresh producer checks for newly acquired output, not a
        # revalidation of completed stored history. Their independent responses
        # must not be replaced with the acquisition body being checked.
        probes.extend(
            _endpoint_needs(
                symbol,
                {
                    name: tuple(
                        row for row in endpoints[name][:1] if timestamp(row[0]) not in confirmed.get(name, ())
                    )
                    for name in state.observed
                },
                need["asof"],
                "acquisition_check",
                cutoff=end,
            )
        )
    checked, checks = (
        _prefetch(probes, client, check=True, metrics=metrics, progress=progress, on_failure=on_failure)
        if probes
        else ({}, {"steps": []})
    )
    return PlannedClient(
        client,
        checked,
        {state.key: state for state in states},
        metrics=metrics,
        progress=progress,
        correction_approvals=correction_approvals,
    ), {
        "schema_version": acquisition["schema_version"],
        "steps": acquisition["steps"] + checks["steps"],
    }


def acquire_retained(
    selected,
    end,
    *,
    client,
    metrics,
    progress=None,
    on_checked=None,
    approved=None,
    acquired=None,
):
    """First/latest all approval -> missing sessions, or selected full replacement."""
    failures, probes = {}, []
    approved = approved or {}

    def failed(_step, action, error):
        outcome = failure_outcome(error)
        exact_fallback = (
            len(action["request"]["params"]["symbols"].split(",")) > 1 and outcome["http_status"] == 400
        )
        for target in action["deliver"]:
            for symbol in target["symbols"]:
                prior = failures.get(symbol)
                if prior is None or prior[1] and not exact_fallback or outcome["outcome"] == "access_denied":
                    failure = (
                        established_pricing_rejection(symbol, error)
                        if action["why"] == "retained_history_acquisition"
                        and isinstance(error, BarsSymbolUnavailable)
                        else error
                    )
                    failures[symbol] = (AcquisitionFailure.from_error(failure), exact_fallback)

    for symbol, (source, endpoints) in selected.items():
        if symbol in approved:
            continue
        asof = acquisition_asof(source, end)
        try:
            windows = history_windows(endpoints, cutoff=end)
        except DownloadError as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
            continue
        probes.extend(
            {
                "kind": "bars",
                "why": "boundary_check",
                "symbols": [symbol],
                "asof": asof,
                "adjustments": ["all"],
                "ranges": [{"start": start.isoformat(), "end_exclusive": stop.isoformat()}],
            }
            for start, stop in windows
        )
    cache, _ = (
        _prefetch(probes, client, check=True, metrics=metrics, progress=progress, on_failure=failed)
        if probes
        else ({}, None)
    )
    # A completed batch omission cannot erase a known retained observation.
    # Confirm only omitted endpoints, once singly. Physical singleton responses
    # (including 414 splits) already establish their result and must not repeat.
    for key, value in list(cache.items()):
        _, symbol, adjustment, start, stop, asof = key
        if symbol in failures or value.singleton:
            continue
        endpoints = selected[symbol][1]
        days = {timestamp(row[0]).astimezone(NEW_YORK).date() for row in value.rows}
        missing = [
            row
            for row in comparison_rows(endpoints["all"][0], endpoints.tail_all)
            if start <= timestamp(row[0]) < stop and timestamp(row[0]).astimezone(NEW_YORK).date() not in days
        ]
        windows = (
            [(start, stop)] if not value.rows else [_endpoint_window(row, asof, cutoff=stop) for row in missing]
        )
        if not missing:
            continue
        try:
            rows = list(value.rows)
            for probe_start, probe_stop in windows:
                metrics["omission_probes"] = metrics.get("omission_probes", 0) + 1
                confirmed = completed_bars(
                    client.fetch(symbol, adjustment, probe_start, probe_stop, asof=asof),
                    symbol,
                    adjustment,
                    probe_start,
                    probe_stop,
                    asof,
                )
                rows.extend(confirmed.rows)
                if confirmed.rows:
                    metrics["omission_recovered"] = metrics.get("omission_recovered", 0) + 1
            cache[key] = replace(value, rows=tuple(sorted(rows, key=lambda row: timestamp(row[0]))))
        except (RuntimeError, OSError, ValueError) as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
    comparison = PlannedClient(client, dict(cache))
    needs, retained, replacements = [], {}, {}
    for symbol, (source, endpoints) in selected.items():
        if symbol in failures:
            continue
        start = timestamp(source.get("history_start", HISTORY_START))
        asof = acquisition_asof(source, end)
        try:
            decision = (
                approved[symbol]
                if symbol in approved
                else fetch_history(
                    comparison,
                    symbol,
                    endpoints,
                    asof,
                    cutoff=end,
                )
            )
        except (RuntimeError, OSError, ValueError) as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
            continue
        if on_checked is not None:
            on_checked(symbol, decision)
        if decision.decision != "full_history":
            start = timestamp(source["latest_request"]["end_exclusive"])
            retained[symbol] = endpoints
        else:
            replacements[symbol] = {"all": decision.live_all}
        needs.append(
            {
                "kind": "bars",
                "why": "retained_history_acquisition",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": asof,
                "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
            }
        )

    def batches():
        from .append import _cohorts

        if failures:
            yield list(failures), client, failures
        # Endpoint results select the actual ranges. Bound those acquisitions,
        # releasing their buffered histories before acquiring the next cohort.
        for cohort in _cohorts(needs):
            symbols = {need["symbols"][0] for need in cohort}
            consumer, _ = acquire_full(
                cohort,
                client,
                metrics=metrics,
                retained_endpoints=retained,
                progress=progress,
                on_failure=failed,
                acquired=acquired,
                required_observations=replacements,
            )
            yield (
                sorted(symbols),
                consumer
                if on_checked is not None
                else PlannedClient(consumer, {key: value for key, value in cache.items() if key[1] in symbols}),
                failures,
            )
            del consumer

    return batches()


@traced("pricing_correction")
def replace_prepared(
    store, prepared, *, client, cohort_size=32, progress=None, before_publish=None, context=None
):
    """Bound memory by cohort; the solver alone chooses concrete API requests."""
    if type(cohort_size) is not int or cohort_size < 1:
        raise DownloadError("refresh cohort size must be a positive integer")
    context = accept_context(store, context)
    results, metrics = [], {}
    from .acquire import MeasuredRequests

    client = MeasuredRequests(client, metrics)
    checked_baseline = False
    for offset in range(0, len(prepared), cohort_size):
        cohort = prepared[offset : offset + cohort_size]
        for item in cohort:
            if item.get("recover_correction") and "correction_completion" not in item:
                root = regular_path(
                    Path(store).absolute(), f"end_of_day/pricing/{item['symbol']}", published=True
                )
                with ticker_lock(root, exclusive=False):
                    item["correction_completion"] = (
                        completed_correction(root, item["required_correction_id"], item["source_sha256"])
                        or False
                    )
            if item.get("correction_completion"):
                item["completed_correction"] = True
                event(
                    "correction_completion_consumed",
                    symbol=item["symbol"],
                    correction_id=item["required_correction_id"],
                    source_generation=item["source_sha256"],
                )
        required = {item["symbol"]: correction_observations(item) for item in cohort}
        failures = {}

        def failed(_step, action, error):
            outcome = failure_outcome(error)
            if outcome.get("http_status") == 400 and len(action["request"]["params"]["symbols"].split(",")) > 1:
                # The adaptive state retains successful stages and owns exact
                # singleton fallback. A rejected batch is not a terminal result
                # for its individually valid symbols.
                return
            for target in action["deliver"]:
                for symbol in target["symbols"]:
                    failures[symbol] = {
                        "symbol": symbol,
                        "status": "failed",
                        "error": str(error),
                        "outcome": failure_outcome(error),
                    }

        if progress:
            progress("acquiring", {"done": offset, "total": len(prepared), "cohort": len(cohort)})
        late_symbols = {
            item["symbol"]
            for item in cohort
            if item.get("correction_scope") == "full_history"
            and item.get("correction_cause") == "provider_late_observations"
            and not item.get("completed_correction")
        }
        needs = [
            {
                "kind": "bars",
                "why": "changed_data",
                "symbols": [item["symbol"]],
                "asof": item["asof"],
                "adjustments": list(ADJUSTMENTS),
                "ranges": [{"start": item["start"], "end_exclusive": item["end_exclusive"]}],
            }
            for item in cohort
            if item.get("correction_scope") not in ("latest_session", "unchanged")
            and item["symbol"] not in late_symbols
            and not item.get("completed_correction")
        ]
        retained, observations = {}, {}
        for item in cohort:
            if item.get("correction_scope") != "latest_session" or item.get("completed_correction"):
                continue
            symbol = item["symbol"]
            decision = item["comparison"]
            day = timestamp(decision.correction_session).astimezone(NEW_YORK).date()
            row = next(row for row in decision.live_all if timestamp(row[0]).astimezone(NEW_YORK).date() == day)
            covered = timestamp(item["comparison_end_exclusive"])
            asof = item["comparison_asof"]
            start, stop = _endpoint_window(row, asof, cutoff=covered)
            observations[symbol, start, stop, asof] = (CompletedBars(symbol, "all", start, stop, asof, (row,)),)
            scopes = [(start, stop, asof)]
            if covered < timestamp(item["end_exclusive"]):
                scopes.append((covered, timestamp(item["end_exclusive"]), item["asof"]))
            needs.extend(
                {
                    "kind": "bars",
                    "why": "latest_session_correction",
                    "symbols": [symbol],
                    "asof": mapping,
                    "adjustments": list(ADJUSTMENTS),
                    "ranges": [{"start": lower.isoformat(), "end_exclusive": upper.isoformat()}],
                }
                for lower, upper, mapping in scopes
            )
            retained[symbol] = item["endpoint_selection"]
        selected_needs, selected_observations = [], {}
        for item in cohort:
            symbol = item["symbol"]
            if symbol not in late_symbols:
                continue
            raw = required[symbol].get("raw", ())
            days = [timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in raw]
            for window in session_ranges(days, cutoff=item["end_exclusive"]):
                start = timestamp(window["start"])
                stop = timestamp(window["end_exclusive"])
                rows = tuple(tuple(row) for row in raw if start <= timestamp(row[0]) < stop)
                key = (symbol, start, stop, item["asof"])
                selected_observations[key] = (CompletedBars(symbol, "raw", start, stop, item["asof"], rows),)
                selected_needs.append(
                    {
                        "kind": "bars",
                        "why": "selected_session_correction",
                        "symbols": [symbol],
                        "asof": item["asof"],
                        "adjustments": list(ADJUSTMENTS),
                        "ranges": [window],
                    }
                )
        try:
            consumer, _ = (
                acquire_full(
                    needs,
                    client,
                    metrics=metrics,
                    progress=progress,
                    retained_endpoints=retained,
                    observations=observations,
                    required_observations=required,
                    on_failure=failed,
                )
                if needs
                else (client, None)
            )
            selected_states, _ = (
                acquire_adjustments(
                    selected_needs,
                    client,
                    metrics=metrics,
                    progress=progress,
                    on_failure=failed,
                    observations=selected_observations,
                )
                if selected_needs
                else ([], None)
            )
        except (RuntimeError, OSError, ValueError) as error:
            results.extend(
                {
                    "symbol": item["symbol"],
                    "status": "failed",
                    "error": str(error),
                    "outcome": failure_outcome(error),
                }
                for item in cohort
            )
            if progress:
                progress("pricing", {"done": len(results), "total": len(prepared), "failed": len(cohort)})
            continue
        selected_by_symbol = {}
        for state in selected_states:
            if state.failure is not None and state.exact_fallback:
                try:
                    state.finish(
                        lambda name: client.fetch(state.symbol, name, state.start, state.end, asof=state.asof)
                    )
                except (RuntimeError, OSError, ValueError) as error:
                    failures[state.symbol] = {
                        "symbol": state.symbol,
                        "status": "failed",
                        "error": str(error),
                        "outcome": failure_outcome(error),
                    }
            if (state.failure is None or state.exact_fallback) and state.completed is not None:
                selected_by_symbol.setdefault(state.symbol, []).append(state.completed)
        if not checked_baseline:
            if before_publish:
                before_publish()
            checked_baseline = True
        for item in cohort:
            if item["symbol"] in failures:
                results.append(failures[item["symbol"]])
                continue
            try:
                if item["symbol"] in late_symbols:
                    acquisitions = selected_by_symbol.get(item["symbol"], ())
                    if not acquisitions:
                        raise DownloadError("selected-session acquisition did not complete")
                    result = publish_selected_session_correction(
                        store,
                        item["symbol"],
                        acquisitions,
                        required[item["symbol"]],
                        expected_source_sha256=item["source_sha256"],
                        history_correction_id=item.get("required_correction_id"),
                    )
                else:
                    local = item.get("correction_scope") in ("latest_session", "unchanged")
                    result = download_pricing(
                        store,
                        item["symbol"],
                        item["end_exclusive"][:10],
                        client=consumer,
                        refresh_all=not local,
                        from_date=item["start"][:10],
                        # Preserve the selected generation across network I/O.
                        expected_source_sha256=None if local else item["source_sha256"],
                        selected_source_sha256=item["source_sha256"],
                        endpoint_selection=item.get("endpoint_selection"),
                        history_correction_id=item.get("required_correction_id"),
                        correction_observations=required[item["symbol"]],
                        correction_evidence_complete=True,
                        correction_cause=item.get("correction_cause"),
                        correction_completion=item.get("correction_completion", False),
                        context=context,
                    )
                result["status"] = "replaced"
            except (RuntimeError, OSError, ValueError) as error:
                result = {
                    "symbol": item["symbol"],
                    "status": "failed",
                    "error": str(error),
                    "outcome": failure_outcome(error),
                }
            results.append(result)
            if progress:
                progress(
                    "pricing",
                    {
                        "done": len(results),
                        "total": len(prepared),
                        "symbol": item["symbol"],
                        "status": result["status"],
                    },
                )
        del consumer  # Release this cohort before acquiring another full history set.
    return {
        "results": results,
        "requests": metrics,
        "failed": sum(item["status"] == "failed" for item in results),
    }
