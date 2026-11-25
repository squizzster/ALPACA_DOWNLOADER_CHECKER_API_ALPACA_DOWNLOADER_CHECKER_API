"""Select a dated SIP observation from a named native event or terminal pricing route."""

from datetime import date, time, timedelta

from libs.alpaca_data_api_solver import NEW_YORK, instant
from libs.alpaca_end_of_day_security_correlation.former_ticker import native_action_anchors
from libs.alpaca_end_of_day_security_correlation.stock_merger_occupation import DEPARTURE_NAME
from libs.alpaca_end_of_day_security_correlation.ticker_reuse_gap import TERMINAL_OWNERSHIP_INTERVAL_RULE_NAMES
from libs.alpaca_security_lineage.cusip_discovery import missing_intervals
from libs.common.us_equity_market_session_calendar import market_sessions_between, next_market_session_after


def _last_complete_day(end_exclusive):
    return instant(end_exclusive).astimezone(NEW_YORK).date() - timedelta(days=1)


def _first_complete_day(history_start):
    start = instant(history_start).astimezone(NEW_YORK)
    return start.date() if start.time() == time.min else start.date() + timedelta(days=1)


def sample_session(view, evidence):
    """Select the native dividend ex-date or a paying acquirer's merger effective date."""
    symbol = view.symbol
    anchors = native_action_anchors(evidence, symbol)
    if not anchors:
        return None
    first = _first_complete_day(view.source["history_start"])
    last = _last_complete_day(view.source["latest_request"]["end_exclusive"])
    raw = next((item for item in view.local["downloads"] if item.get("adjustment") == "raw"), {})
    if raw.get("rows") == 0:
        return None
    if raw.get("from") and raw.get("to"):
        first = max(first, instant(raw["from"]).astimezone(NEW_YORK).date())
        last = min(last, instant(raw["to"]).astimezone(NEW_YORK).date())
    return min((anchor.day.isoformat() for anchor in anchors if first <= anchor.day <= last), default=None)


def _otc_asset_id(lookup, symbol):
    asset = (lookup or {}).get("asset") or {}
    if (asset.get("class"), asset.get("symbol"), asset.get("exchange")) == ("us_equity", symbol, "OTC"):
        return asset.get("id")
    return None


def mapped_sample_route(view, lookup):
    """The labelled mapped response establishes a tape observation on its request route."""
    asset_id = _otc_asset_id(lookup, view.symbol)
    if not asset_id:
        return None
    request = view.source["latest_request"]
    if request.get("feed") != "sip" or request.get("timeframe") != "1Day" or request.get("asof") in (None, "-"):
        return None
    raw = next((item for item in view.local["downloads"] if item.get("adjustment") == "raw"), {})
    if not raw.get("to"):
        return None
    return (
        instant(raw["to"]).astimezone(NEW_YORK).date().isoformat(),
        request["asof"],
        {
            "observed_symbol_asset_id": asset_id,
            "end_exclusive": request["end_exclusive"],
            "current_label": request["asof"] == request["end_exclusive"][:10],
        },
        raw,
    )


def _historical_gap(session, end_exclusive):
    last = date.fromisoformat(session)
    # The sample covers a complete New York date. A partial current session
    # cannot establish that the previous session was a historical endpoint.
    through = _last_complete_day(end_exclusive)
    if through <= last:
        return False
    # A full week of elapsed coverage selects a point observation independently
    # of the installed calendar horizon. For a shorter gap require a known
    # later market session, so weekends and holidays alone do not select work.
    return (through - last).days >= 7 or bool(
        market_sessions_between((last + timedelta(days=1)).isoformat(), through.isoformat())
    )


def _same_pricing_mapping(sample, request, observed_id):
    old = sample["pricing_route_binding"]
    end, asof = request.get("end_exclusive"), request.get("asof")
    return (
        old["observed_symbol_asset_id"] == observed_id
        and (request.get("feed"), request.get("timeframe")) == ("sip", "1Day")
        and (
            sample["asof"] == asof
            or (end and old["current_label"] and asof == end[:10] and old["end_exclusive"] <= end)
        )
    )


def _same_generation(view, previous, through):
    return previous == view.source_sha256 or any(
        change["source_sha256"] == previous
        and (change["changed_from"] is None or change["changed_from"] > through)
        for change in view.source.get("history_changes", ())
    )


def _offset(day, days):
    return (date.fromisoformat(day) + timedelta(days=days)).isoformat()


def _last_observation(search):
    request = search.get("last_request") or {}
    return request if request.get("outcome") == "observed" else search.get("observation")


def listed_sip_trade(mapped):
    """Return the actual dated listed-tape print selected on this mapped route."""
    sample = mapped
    if (mapped.get("http_status"), mapped.get("outcome")) == (200, "no_trades"):
        search = mapped.get("last_trade_search") or {}
        if search.get("outcome") != "observed":
            return None
        sample = _last_observation(search)
    tape = (sample.get("trade") or {}).get("z")
    if (sample.get("http_status"), sample.get("outcome"), sample.get("feed")) == (
        200,
        "observed",
        "sip",
    ) and tape in {"A", "B", "C"}:
        return sample.get("observed_session", sample["session"]), tape
    return None


def _empty_intervals(ranges):
    """Combine only contiguous dates where a completed response establishes absence."""
    joined = []
    for first, last in sorted(ranges):
        if first > last:
            continue
        if joined and first <= _offset(joined[-1][1], 1):
            joined[-1][1] = max(joined[-1][1], last)
        else:
            joined.append([first, last])
    return joined


def _last_trade_search(view, raw, mapped, previous, client, *, refresh):
    generation = getattr(view, "source_sha256", None)
    last = mapped["session"]
    retry_unavailable = refresh and previous and previous["outcome"] == "unavailable"
    if (
        previous
        and previous["source_generation"] == generation
        and previous["raw_last_session"] == last
        and not retry_unavailable
    ):
        return previous
    if (
        not generation
        or raw.get("rows") == 0
        or last > _last_complete_day(view.source["latest_request"]["end_exclusive"]).isoformat()
    ):
        return None
    if previous and not _same_generation(view, previous["source_generation"], previous["raw_last_session"]):
        previous = None
    if getattr(view, "materialization_scope", "complete_history") != "complete_history":
        # A suffix does not describe the first raw session. Its applicable
        # completed selection can carry that endpoint through an append.
        if not previous:
            return None
        raw_first = previous["raw_first_session"]
    elif raw.get("from"):
        raw_first = instant(raw["from"]).astimezone(NEW_YORK).date().isoformat()
    else:
        return None
    first = max(raw_first, _first_complete_day(view.source["history_start"]).isoformat())
    through = _offset(last, -1)
    if first > through:
        return None
    if (
        previous
        and (previous["raw_first_session"], previous["from_session"], previous["raw_last_session"])
        == (raw_first, first, last)
        and not retry_unavailable
    ):
        return {**previous, "source_generation": generation}
    absence = list(previous["empty_intervals"]) if previous else []
    absence.append([last, last])  # The separately retained endpoint day already answered.
    observation = _last_observation(previous) if previous else None
    if observation and not first <= observation["observed_session"] <= through:
        observation = None
    lower = _offset(observation["observed_session"], 1) if observation else first
    last_request = previous.get("last_request") if previous else None
    outcome = "observed" if observation else "no_trades"
    for a, b in reversed(list(missing_intervals(lower, through, absence))):
        last_request = client.get_trade_tape_sample(
            view.symbol,
            a,
            through_session=b,
            asof=mapped["asof"],
            mapping_asset_id=mapped["pricing_route_binding"]["observed_symbol_asset_id"],
            feed="sip",
            sort="desc",
        )
        if last_request is None:
            return None
        if last_request["outcome"] == "unavailable":
            outcome = "unavailable"
            break
        if last_request["outcome"] == "observed":
            observation, outcome = last_request, "observed"
            absence.append([_offset(observation["observed_session"], 1), b])
            break
        absence.append([a, b])
    return {
        "source_generation": generation,
        "observed_symbol_asset_id": mapped["pricing_route_binding"]["observed_symbol_asset_id"],
        "pricing_asof": mapped["asof"],
        "raw_first_session": raw_first,
        "raw_last_session": last,
        "from_session": first,
        "through_session": through,
        "outcome": outcome,
        "empty_intervals": _empty_intervals(absence),
        "last_request": last_request,
        **({"observation": observation} if observation and observation is not last_request else {}),
    }


def record_samples(view, evidence, window, *, refresh):
    from .terminal_cash_trade import record_terminal_cash_trade

    record_terminal_cash_trade(view, evidence, window)
    if window.trade_tape_inputs_changed or refresh and "historical_trade_tape" not in evidence:
        prior = evidence.pop("historical_trade_tape", None)
        session = sample_session(view, evidence)
        if session:
            observed = (
                prior
                if prior and prior["session"] == session
                else window.client.get_trade_tape_sample(view.symbol, session)
            )
            if observed is not None:
                evidence["historical_trade_tape"] = observed
    if window.early_mapped_samples is not None:
        evidence.update(window.early_mapped_samples)
        selected = "mapped_historical_trade_tape" in window.early_mapped_samples
    else:
        selected = record_mapped_samples(view, evidence, window, refresh=refresh)
    if selected:
        _record_following_otc_trade(view, evidence, window, refresh=refresh)


def record_mapped_samples(view, evidence, window, *, refresh):
    prior = evidence.get("mapped_historical_trade_tape")
    lookup = window.consumed_symbols.get(view.symbol) or {}
    route = mapped_sample_route(view, lookup)
    if not route:
        evidence.pop("mapped_historical_trade_tape", None)
        evidence.pop("otc_historical_trade_tape", None)
        return
    session, asof, binding, raw = route
    same_mapping = prior and _same_pricing_mapping(
        prior, view.source["latest_request"], binding["observed_symbol_asset_id"]
    )
    same_point = same_mapping and prior["session"] == session
    if not same_point and not _historical_gap(session, binding["end_exclusive"]):
        evidence.pop("mapped_historical_trade_tape", None)
        evidence.pop("otc_historical_trade_tape", None)
        return
    if window.previous and not prior and not refresh:
        # A newly selected source/date or a newly observed retirement can supply
        # evidence. An unchanged legacy publication does not select a backfill.
        old_lookup = next(
            (
                item
                for item in window.previous["evidence"]["asset_lookups"]
                if item["lookup_kind"] == "symbol" and item["lookup"] == view.symbol
            ),
            None,
        )
        last = window.previous.get("assignment_summary", {}).get("raw", {}).get("last_session")
        if _otc_asset_id(old_lookup, view.symbol) == binding["observed_symbol_asset_id"] and last in (
            None,
            session,
        ):
            old_end = window.previous.get("pricing_request", {}).get("end_exclusive")
            if not old_end or _historical_gap(session, old_end):
                return
    if same_point:
        mapped = prior
    else:
        sample = window.client.get_trade_tape_sample(
            view.symbol, session, asof=asof, mapping_asset_id=binding["observed_symbol_asset_id"]
        )
        if sample is None:
            evidence.pop("mapped_historical_trade_tape", None)
            evidence.pop("otc_historical_trade_tape", None)
            return
        mapped = evidence["mapped_historical_trade_tape"] = {**sample, "pricing_route_binding": binding}
    previous_end = prior["pricing_route_binding"]["end_exclusive"] if same_point and not refresh else None
    if (mapped.get("http_status"), mapped.get("outcome"), mapped.get("feed")) == (200, "no_trades", "sip"):
        previous_search = prior.get("last_trade_search") if same_mapping else None
        if same_point and not previous_search and not refresh:
            # This legacy observation already completed its selected scope.
            return True
        search = _last_trade_search(view, raw, mapped, previous_search, window.client, refresh=refresh)
        if search is not mapped.get("last_trade_search"):
            mapped = evidence["mapped_historical_trade_tape"] = {
                k: v for k, v in mapped.items() if k != "last_trade_search"
            }
            if search:
                mapped["last_trade_search"] = search
        listed = listed_sip_trade(mapped)
        if not listed:
            old_print = _last_observation(search) if search and search["outcome"] == "unavailable" else None
            old_otc = evidence.get("otc_historical_trade_tape")
            if not (old_print and old_otc and old_otc.get("after_sip_session") == old_print["observed_session"]):
                evidence.pop("otc_historical_trade_tape", None)
            # A denied new interval can preserve an independently dated literal
            # response for an older observed print. It cannot select a new one.
            return False
        session = listed[0]
        if previous_end and search is not previous_search and listed_sip_trade(prior) != listed:
            previous_end = None
    day = evidence.get("otc_historical_trade_tape")
    if day and (
        day.get("after_sip_session") != session or (not same_mapping and day.get("outcome") == "unavailable")
    ):
        evidence.pop("otc_historical_trade_tape")
    elif day and not same_mapping and "following_trade_search" in day:
        # The literal day still serves this ticker/date. Its former mapped-issue
        # association cannot carry the forward selection to a new owner.
        evidence["otc_historical_trade_tape"] = {k: v for k, v in day.items() if k != "following_trade_search"}
    _record_otc_sample(view, evidence, window.client, session, previous_end=previous_end)
    return True


def _record_otc_sample(view, evidence, client, session, *, previous_end=None):
    prior = evidence.get("otc_historical_trade_tape")
    if prior and date.fromisoformat(prior["session"]) > _last_complete_day(
        view.source["latest_request"]["end_exclusive"]
    ):
        evidence.pop("otc_historical_trade_tape")
        return
    if (
        prior
        and prior.get("after_sip_session") == session
        and (previous_end or prior["outcome"] in ("observed", "no_trades"))
    ):
        return
    day = next_market_session_after(session)
    if not day:
        return
    selected_day = date.fromisoformat(day)
    if selected_day > _last_complete_day(view.source["latest_request"]["end_exclusive"]):
        return
    if previous_end and selected_day <= _last_complete_day(previous_end):
        # This route's earlier metadata operation already selected the date.
        # An unavailable account or a peer may have consumed the only request.
        # A changed binding or explicit metadata refresh permits a new attempt.
        return
    observed = client.get_trade_tape_sample(view.symbol, day, feed="otc")
    if observed is not None:
        evidence["otc_historical_trade_tape"] = {**observed, "after_sip_session": session}


def _otc_action_limit(symbol, owner, last_sip, evidence):
    """Earliest native departure or different dated occupant limiting this literal search."""
    limits = []
    rivals = {
        item["action_id"]
        for occupant in evidence.get("input_symbol_occupants", ())
        if occupant.get("symbol") == symbol and occupant.get("asset_id") not in (None, owner)
        for item in occupant.get("evidence", ())
        if item.get("action_id")
    }
    for action in evidence.get("corporate_actions", ()):
        kind = action.get("action_type")
        departure = (
            (
                kind in {"name_change", "unit_split"}
                and action.get("old_symbol") == symbol
                and action.get("new_symbol") not in (None, "", symbol)
            )
            or (
                kind in {"cash_merger", "stock_merger", "stock_and_cash_merger"}
                and action.get("acquiree_symbol") == symbol
            )
            or (kind == "worthless_removal" and action.get("symbol") == symbol)
        )
        if departure or action.get("id") in rivals:
            day = action.get("effective_date") or action.get("ex_date") or action.get("process_date")
            if isinstance(day, str) and day >= last_sip:
                try:
                    limits.append(date.fromisoformat(day) - timedelta(days=1))
                except ValueError:
                    continue
    return min(limits).isoformat() if limits else None


def _record_following_otc_trade(view, evidence, window, *, refresh):
    day = evidence.get("otc_historical_trade_tape")
    mapped = evidence.get("mapped_historical_trade_tape") or {}
    generation = getattr(view, "source_sha256", None)
    if not day or (day.get("http_status"), day.get("outcome")) != (200, "no_trades"):
        return
    previous = day.get("following_trade_search")
    owner = (mapped.get("pricing_route_binding") or {}).get("observed_symbol_asset_id")
    last_sip = day.get("after_sip_session")
    listed = listed_sip_trade(mapped)
    if not generation or not owner or not listed or listed[0] != last_sip:
        if previous:
            evidence["otc_historical_trade_tape"] = {
                k: v for k, v in day.items() if k != "following_trade_search"
            }
        return
    request = view.source["latest_request"]
    first = (date.fromisoformat(day["session"]) + timedelta(days=1)).isoformat()
    binding = {"observed_symbol_asset_id": owner, "pricing_asof": mapped["asof"], "after_sip_session": last_sip}
    # record_mapped_samples already discarded an association on any real
    # mapping change. A rolling current-label date can carry this literal scope.
    if previous and not (
        previous["observed_symbol_asset_id"] == owner
        and previous["after_sip_session"] == last_sip
        and _same_generation(view, previous["source_generation"], last_sip)
    ):
        previous = None
    action_limit = (
        previous["action_limit"]
        if previous and not window.otc_limits_changed
        else _otc_action_limit(view.symbol, owner, last_sip, evidence)
    )
    through = _last_complete_day(request["end_exclusive"]).isoformat()
    if action_limit:
        through = min(through, action_limit)
    for _, interval in (window.previous or {}).get("observation_routes", {}).values():
        if (
            interval.get("historical_symbol") == view.symbol
            and interval.get("asset_id") == owner
            and any(
                rule.get("supporting_corporate_action_ids")
                and (
                    rule.get("name") == DEPARTURE_NAME
                    or rule.get("name") in TERMINAL_OWNERSHIP_INTERVAL_RULE_NAMES
                )
                for rule in interval.get("ownership_interval_rules", ())
            )
        ):
            end = interval.get("end_date_exclusive")
            if end and end >= last_sip:
                through = min(through, (date.fromisoformat(end) - timedelta(days=1)).isoformat())
    retry_unavailable = refresh and previous and previous["outcome"] == "unavailable"
    if (
        previous
        and not retry_unavailable
        and previous["source_generation"] == generation
        and previous["through_session"] == through
        and previous["action_limit"] == action_limit
    ):
        return
    # Earlier searches established a continuous empty prefix or returned the
    # first chronological print. Both are usable when a selected bound shrinks.
    last = previous.get("last_request") if previous else None
    empty = previous["empty_through_session"] if previous else day["session"]
    if empty < through and (
        retry_unavailable
        or (last and last["outcome"] == "no_trades")
        or (last is None and (previous is None or previous["outcome"] == "outside_search_bounds"))
    ):
        start = (date.fromisoformat(empty) + timedelta(days=1)).isoformat()
        last = window.client.get_trade_tape_sample(view.symbol, start, feed="otc", through_session=through)
        if last and last["outcome"] == "no_trades":
            empty = through
        elif last and last["outcome"] == "observed":
            empty = (date.fromisoformat(last["observed_session"]) - timedelta(days=1)).isoformat()
    outcome = last["outcome"] if last else "unavailable"
    if through < first:
        outcome = "outside_search_bounds"
    elif through <= empty:
        outcome = "no_trades"
    search = {
        **binding,
        "source_generation": generation,
        "from_session": first,
        "through_session": through,
        "action_limit": action_limit,
        "empty_through_session": empty,
        "outcome": outcome,
        **({"first_observed_session": last["observed_session"]} if outcome == "observed" else {}),
        "last_request": last,
    }
    evidence["otc_historical_trade_tape"] = {**day, "following_trade_search": search}
