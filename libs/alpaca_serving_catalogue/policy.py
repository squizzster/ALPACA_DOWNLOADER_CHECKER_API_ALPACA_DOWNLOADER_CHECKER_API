"""Exact serving checks and public metadata, consuming published ownership only."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import policy
from .reuse import extend_ticker_ranges, select_prefixes


def _bar_reasons(bar, adjustment):
    # Completed pricing publications already guarantee exact, finite numbers.
    values = {
        name: Decimal(bar[name]) for name in ("open", "high", "low", "close", "volume", "trade_count", "vwap")
    }
    o, h, lo, c, volume, count, vwap = (values[n] for n in values)
    reasons = set()
    instant = timestamp(bar["timestamp"]).astimezone(NEW_YORK)
    if (instant.hour, instant.minute, instant.second, instant.microsecond) != (0, 0, 0, 0):
        reasons.add("not_session_midnight")
    zero = volume == count == vwap == 0 and o == h == lo == c and o > 0
    if min(o, h, lo, c) <= 0 or (not zero and vwap <= 0):
        reasons.add("nonpositive_price")
    if not lo <= o <= h or not lo <= c <= h or (not zero and not lo <= vwap <= h):
        reasons.add("price_bounds")
    # Adjusted volumes may be fractional or round to zero. Never cast through float.
    if volume < 0 or (
        not zero and volume == 0 and adjustment not in {"split", "split,dividend", "split,spin-off", "all"}
    ):
        reasons.add("volume_domain")
    if count != count.to_integral_value() or count < 0 or (not zero and count == 0):
        reasons.add("trade_count_domain")
    if volume == count == vwap == 0 and not zero:
        reasons.add("zero_activity_shape")
    return reasons


def _segments(group, public_id):
    return [
        segment
        for items in group.assignments.values()
        for _, _, segment in items
        if segment["branch"] == "correlated" and segment["security_identity"]["security_id"] == public_id
    ]


def _metadata(group, public_id):
    segments = _segments(group, public_id)
    members = {i for s in segments for i in s["security_identity"]["member_alpaca_asset_ids"]}
    observations = {}
    for source in group.evidence["sources"].values():
        evidence = source["evidence"]
        for lookup in evidence["asset_lookups"]:
            asset = lookup.get("asset") or {}
            if asset.get("id") not in members:
                continue
            observed = lookup.get("observed_at_utc")
            if observed is None and lookup.get("source") == "asset_catalogue":
                observed = evidence.get("catalogue_observed_at_utc")
            # A retired CUSIP lookup can carry today's reused-ticker description.
            # Only a positive own-label observation supplies descriptive metadata;
            # its observation time is never promoted to a historical effective date.
            fields = (
                {
                    "name": asset.get("name"),
                    "exchange": asset.get("exchange"),
                    "asset_class": asset.get("class"),
                }
                if lookup.get("lookup_kind") == "symbol" and lookup.get("lookup") == asset.get("symbol")
                else {}
            )
            if lookup.get("lookup_kind") == "cusip":
                fields["cusip"] = lookup.get("lookup")
            for field, value in fields.items():
                if isinstance(value, str) and value:
                    key = field, value, observed or ""
                    observations[key] = {
                        "field": field,
                        "value": value,
                        "observed_at": observed,
                        "effective_from": None,
                        "effective_through": None,
                    }
    # A published canonical CUSIP establishes known identity metadata, not an
    # issuer-wide historical name or exchange timeline.
    for segment in segments:
        value = (segment.get("canonical_security") or {}).get("cusip")
        if value:
            observations[("cusip", value, "")] = {
                "field": "cusip",
                "value": value,
                "observed_at": None,
                "effective_from": None,
                "effective_through": None,
            }
        interval = segment.get("ticker_ownership_interval") or {}
        if value and interval.get("cusip") == value:
            first = max(segment["from"], interval.get("start_date_inclusive") or segment["from"])
            end_exclusive = interval.get("end_date_exclusive")
            last = min(
                segment["to"],
                (date.fromisoformat(end_exclusive) - timedelta(days=1)).isoformat()
                if end_exclusive
                else segment["to"],
            )
            if first <= last:
                observations[("cusip", value, "", first, last)] = {
                    "field": "cusip",
                    "value": value,
                    "observed_at": None,
                    "effective_from": first,
                    "effective_through": last,
                }
    return members, [observations[key] for key in sorted(observations)]


def _ticker_ranges(bars):
    ranges, active = [], {}
    for bar in bars:
        labels = set(bar["tickers"])
        for ticker in set(active) - labels:
            ranges.append(active.pop(ticker))
        for ticker in labels:
            if ticker not in active:
                active[ticker] = {
                    "ticker": ticker,
                    "first_session": bar["session"],
                    "last_session": bar["session"],
                }
            active[ticker]["last_session"] = bar["session"]
    return sorted([*ranges, *active.values()], key=lambda r: (r["ticker"], r["first_session"]))


def _ownership_intervals(group, public_id):
    intervals = set()
    for segment in _segments(group, public_id):
        interval = segment.get("ticker_ownership_interval")
        if interval:
            intervals.add(
                (
                    interval["historical_symbol"],
                    interval.get("start_date_inclusive"),
                    interval.get("end_date_exclusive"),
                    interval.get("cusip"),
                )
            )
    return [
        dict(zip(("ticker", "start_inclusive", "end_exclusive", "cusip"), item))
        for item in sorted(intervals, key=lambda i: tuple(v or "" for v in i))
    ]


def _rename_exclusions(group, members, observations, bars, missing, *, source_sessions):
    """A bounded, same-UUID/CUSIP rename may explain a short no-source gap.

    This checks continuity only. Published assignments already chose the identity.
    No rule can assign any missing or unresolved observation here.
    """
    cusips = {o["value"] for o in observations if o["field"] == "cusip"}
    exchanges = {o["value"] for o in observations if o["field"] == "exchange"}
    if len(members) != 1 or len(cusips) != 1 or len(exchanges) != 1 or not missing:
        return set()
    sources = group.evidence["sources"]
    evidence = [s["evidence"] for s in sources.values()]
    if any(
        e.get("unresolved_cusips") or e.get("query", {}).get("corporate_actions_data_quality") != "complete"
        for e in evidence
    ):
        return set()
    actions = {}
    for e in evidence:
        for action in e["corporate_actions"]:
            key = action.get("id")
            if not key or (key in actions and actions[key] != action):
                return set()
            actions[key] = action
    accepted = set()
    for left, right in zip(bars, bars[1:]):
        gap = {day for day in missing if left["session"] < day < right["session"]}
        if (
            not 1 <= len(gap) <= 5
            or gap & source_sessions
            or len(left["tickers"]) != 1
            or len(right["tickers"]) != 1
        ):
            continue
        old, new = left["tickers"][0], right["tickers"][0]
        if any(
            not e["query"].get("corporate_actions_start")
            or not e["query"].get("corporate_actions_end")
            or e["query"]["corporate_actions_start"] > left["session"]
            or e["query"]["corporate_actions_end"] < right["session"]
            for e in evidence
        ):
            continue
        matches = [
            a
            for a in actions.values()
            if a.get("action_type") == "name_change"
            and a.get("old_symbol") == old
            and a.get("new_symbol") == new
        ]
        if old == new or len(matches) != 1:
            continue
        action = matches[0]
        if (
            action.get("old_cusip") not in cusips
            or action.get("new_cusip") not in cusips
            or action.get("process_date") not in gap | {right["session"]}
        ):
            continue
        if any(
            a != action
            and a.get("process_date") in gap | {left["session"], right["session"]}
            and a.get("action_type") != "cash_dividend"
            for a in actions.values()
        ):
            continue
        lookups = [lookup for e in evidence for lookup in e["asset_lookups"]]

        def ids(kind, label):
            matching = [v for v in lookups if v.get("lookup_kind") == kind and v.get("lookup") == label]
            if not matching or any(v.get("outcome") not in {"found", "not_found"} for v in matching):
                return None
            return {v["asset"]["id"] for v in matching if v.get("asset")}

        if (
            ids("cusip", next(iter(cusips))) != members
            or ids("symbol", new) != members
            or ids("symbol", old) not in (set(), members)
        ):
            continue
        if any(
            i.get("symbol") in {old, new} and i.get("id") not in members
            for e in evidence
            for i in e["identities"]
        ):
            continue
        accepted.update(gap)
    return accepted


def evaluate(group, previous=()):
    from alpaca_end_of_day.standalone import HistoryPrefixChanged, UnservableHistoryError

    decisions, securities = [], []
    inputs, offsets = group.serving_inputs()
    prefixes = select_prefixes(group, previous, inputs, offsets)
    validation = {"inputs": inputs, "identities": {}, "prefix_records": []}
    work = {"reused_history_prefixes": 0, "full_history_evaluations": 0, "checked_bars": 0}
    for security in group.summary["securities"]:
        public_id = security["public_id"]
        reasons = set()
        if set(security["adjustments"]) != set(ADJUSTMENTS):
            decisions.append({"public_id": public_id, "eligible": False, "reasons": ["missing_views"]})
            continue
        prefix = prefixes.get(public_id)
        if prefix is None:
            work["full_history_evaluations"] += 1
        try:
            try:
                views, coverage = group.serving_histories(public_id, prefix=prefix)
            except HistoryPrefixChanged:
                prefix = None
                work["full_history_evaluations"] += 1
                views, coverage = group.serving_histories(public_id)
        except UnservableHistoryError:
            if prefix is not None:
                work["reused_history_prefixes"] += 1
            decisions.append(
                {"public_id": public_id, "eligible": False, "reasons": ["conflicting_daily_history"]}
            )
            continue
        prior = prefix["record"] if prefix else None
        price_reasons = set(prior["price_reasons"] if prior else ())
        if prior:
            work["reused_history_prefixes"] += 1
        validation["identities"][public_id] = coverage
        raw = views["raw"]["bars"]
        sessions = [b["session"] for b in raw]
        if not sessions and prior is None:
            reasons.add("empty_history")
        for adjustment, view in views.items():
            if [b["session"] for b in view["bars"]] != sessions:
                price_reasons.add("incompatible_sessions")
            for bar in view["bars"]:
                price_reasons.update(_bar_reasons(bar, adjustment))
                work["checked_bars"] += 1
        reasons.update(price_reasons)
        members, observations = _metadata(group, public_id)
        missing = set(prior["missing_sessions"] if prior else ())
        gap_bars = list(prior["gap_bars"] if prior else ())
        source_sessions = set(prior["source_sessions_in_gaps"] if prior else ())
        if prefix and prefix["refresh_gap_sources"]:
            source_sessions = group.source_sessions(start=min(missing), end=max(missing)) & missing
        first_session = prior["first_session"] if prior else (sessions[0] if sessions else None)
        last_session = sessions[-1] if sessions else (prior["last_session"] if prior else None)
        if sessions:
            end = (date.fromisoformat(sessions[-1]) + timedelta(days=1)).isoformat()
            start = (
                (date.fromisoformat(prior["last_session"]) + timedelta(days=1)).isoformat()
                if prior
                else sessions[0]
            )
            expected = set(expected_market_session_coverage(start, end).session_dates)
            new_missing = expected - set(sessions)
            missing.update(new_missing)
            if new_missing:
                source_sessions.update(
                    group.source_sessions(start=min(new_missing), end=max(new_missing)) & new_missing
                )
            seam = [prior["last_raw_bar"]] if prior else []
            span = [*seam, *raw]
            for left, right in zip(span, span[1:]):
                if left["session"] < right["session"] and any(
                    left["session"] < day < right["session"] for day in new_missing
                ):
                    gap_bars.extend(
                        {"session": bar["session"], "tickers": bar["tickers"]} for bar in (left, right)
                    )
            if set(sessions) - expected:
                reasons.add("nonmarket_sessions")
                price_reasons.add("nonmarket_sessions")
        gap_bars = sorted({bar["session"]: bar for bar in gap_bars}.values(), key=lambda bar: bar["session"])
        explained = _rename_exclusions(
            group, members, observations, gap_bars, missing, source_sessions=source_sessions
        )
        if missing - explained:
            reasons.add("unexplained_missing_sessions")
        if first_session is not None:
            for source, assignments in group.assignments.items():
                for _, _, segment in assignments:
                    if (
                        segment["branch"] != "uncorrelated"
                        or segment["to"] < first_session
                        or segment["from"] > last_session
                    ):
                        continue
                    candidates = segment.get("candidate_securities") or []
                    candidates_ids = {c.get("asset_id") for c in candidates if isinstance(c, dict)} - {None}
                    if candidates_ids & members or (not candidates_ids and source in security["tickers"]):
                        reasons.add("unresolved_coverage")
        decisions.append(
            {
                "public_id": public_id,
                "eligible": not reasons,
                "reasons": sorted(reasons),
                "explained_missing_sessions": sorted(explained),
            }
        )
        if not first_session:
            continue

        # Preserve every label selected by any advertised view. Public resolution
        # and flat exports must not silently prefer raw's label under ambiguity.
        dated_labels = {day: set() for day in sessions}
        for view in views.values():
            for bar in view["bars"]:
                if bar["session"] in dated_labels:
                    dated_labels[bar["session"]].update(bar["tickers"])
        dated_bars = [{"session": day, "tickers": sorted(labels)} for day, labels in dated_labels.items()]
        ticker_ranges = _ticker_ranges(dated_bars)
        if prior:
            ticker_ranges = extend_ticker_ranges(prior, ticker_ranges)
        history_summary = {
            "public_id": public_id,
            "first_session": first_session,
            "last_session": last_session,
            "ticker_ranges": ticker_ranges,
            "coverage": {
                "meaning": "approved_stored_history",
                "session_count": len(sessions) + (prior["coverage"]["session_count"] if prior else 0),
                "explained_missing_sessions": sorted(explained),
            },
        }
        validation["prefix_records"].append(
            {
                **history_summary,
                "validated_through": max(
                    ([prior["validated_through"]] if prior else [])
                    + [view["bars"][-1]["session"] for view in views.values() if view["bars"]]
                ),
                "last_raw_bar": (
                    {"session": raw[-1]["session"], "tickers": raw[-1]["tickers"]}
                    if raw
                    else prior["last_raw_bar"]
                ),
                "price_reasons": sorted(price_reasons),
                "missing_sessions": sorted(missing),
                "gap_bars": gap_bars,
                "source_sessions_in_gaps": sorted(source_sessions),
            }
        )
        if reasons:
            continue

        def values(field):
            return sorted({o["value"] for o in observations if o["field"] == field})

        securities.append(
            {
                **history_summary,
                "tickers": sorted({item["ticker"] for item in ticker_ranges}),
                "names": values("name"),
                "cusips": values("cusip"),
                "exchanges": values("exchange"),
                "asset_classes": values("asset_class"),
                "external_identifiers": [],
                "adjustments": list(ADJUSTMENTS),
                "observations": observations,
                "ticker_ownership_intervals": _ownership_intervals(group, public_id),
            }
        )
    return {
        "locator": min(group.references["pricing"]),
        "references": group.references,
        "policy": policy(),
        "decision_generation": uuid4().hex,
        "securities": securities,
        "decisions": decisions,
        "history_validation": validation,
        "validation_work": work,
    }
