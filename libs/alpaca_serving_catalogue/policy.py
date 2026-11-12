"""Exact serving checks and public metadata, consuming published ownership only."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.debug_trace import traced
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import CatalogueError, policy
from .offline import available_checkpoints, previous_checkpoints, select_offline
from .reuse import extend_ticker_ranges, select_prefixes

_VACANT_TICKER_BRIDGE_REASON = "certified_vacant_ticker_bridge_non_observation"
_PRE_ENTRY_CARRY_FORWARD_REASON = "certified_pre_entry_carry_forward_non_observation"
_TRUSTED_NON_OBSERVATION_REASONS = frozenset((_VACANT_TICKER_BRIDGE_REASON, _PRE_ENTRY_CARRY_FORWARD_REASON))


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


def _unresolved_ends(group, security, members, first, last):
    """Relevant unresolved observations constrain their dates, not every date."""
    if first is None:
        return []
    ownership = _ownership_intervals(group, security["public_id"])
    ends = []
    for source, assignments in group.assignments.items():
        for _, _, segment in assignments:
            if segment["branch"] != "uncorrelated" or segment["to"] < first or segment["from"] > last:
                continue
            reasons = tuple(segment.get("reason_codes") or ())
            if len(reasons) == 1 and reasons[0] in _TRUSTED_NON_OBSERVATION_REASONS:
                continue
            candidates = segment.get("candidate_securities") or []
            ids = {c.get("asset_id") for c in candidates if isinstance(c, dict)} - {None}
            label_match = not ids and source in security["tickers"]
            intervals = [item for item in ownership if item["ticker"] == source]
            if label_match and intervals:
                # The lineage producer already bounded this identity's use of
                # the label. An ownerless observation outside every such bound
                # is not a conflict with this identity merely because the label
                # once belonged to it. Explicit UUID conflicts still apply.
                for item in intervals:
                    lower = max(first, segment["from"], item["start_inclusive"] or first)
                    upper = min(
                        last,
                        segment["to"],
                        (date.fromisoformat(item["end_exclusive"]) - timedelta(days=1)).isoformat()
                        if item["end_exclusive"]
                        else last,
                    )
                    if lower <= upper:
                        ends.append(upper)
                continue
            if ids & members or label_match:
                ends.append(segment["to"])
    return ends


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
        description = interval.get("security_description")
        if description is not None:
            if (
                not isinstance(description, dict)
                or type(description.get("version")) is not int
                or description["version"] != 1
                or set(description)
                != {"version", "name", "exchange", "asset_class", "instrument_kind", "registrant_cik"}
            ):
                raise CatalogueError("unsupported dated security description; finish owning lineage recovery")
            # Lineage owns this dated description and its primary evidence.
            # In particular, today's reused-ticker CUSIP description cannot
            # substitute for an earlier listed security's completed metadata.
            for field in ("name", "exchange", "asset_class"):
                text = description[field]
                if text is not None:
                    observations[(field, text, "", segment["from"], segment["to"])] = {
                        "field": field,
                        "value": text,
                        "observed_at": None,
                        "effective_from": segment["from"],
                        "effective_through": segment["to"],
                    }
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


@traced("serving_evaluation")
def evaluate(group, previous=(), *, rebuild_projection=False):
    from alpaca_end_of_day.standalone import HistoryPrefixChanged, UnservableHistoryError

    # The locked group owns identity assignments and numerical source choices;
    # this producer owns serving eligibility. Consume those decisions as given,
    # then validate only history work not covered by a reusable serving prefix.
    # DO NOT acquire provider evidence or replay lineage rules to certify input.
    from . import preparation

    decisions, securities = [], []
    previous = tuple(previous)
    binding = preparation.scope(group, previous, rebuild_projection)
    prepared = preparation.selected(binding)
    if prepared is None:
        inputs, offsets = group.serving_inputs()
        previous = previous_checkpoints(previous)
    else:
        inputs, offsets, previous = prepared["inputs"], prepared["offsets"], prepared["previous"]
    from .source_equivalence import accepted_dates

    selected_securities = prepared["securities"] if prepared else group.summary["securities"]
    offline_ids, offline = (
        prepared["offline"]
        if prepared
        else select_offline(group, previous, inputs, offsets, securities=selected_securities)
    )
    prefixes = (
        prepared["prefixes"]
        if prepared
        else (
            select_prefixes(
                group,
                available_checkpoints(previous, offline_ids),
                inputs,
                offsets,
                **({"rebuild_projection": True} if rebuild_projection else {}),
            )
            if any(item["public_id"] not in offline_ids for item in selected_securities)
            else {}
        )
    )
    if prepared is None:
        preparation.completed(
            binding,
            {
                "inputs": inputs,
                "offsets": offsets,
                "previous": previous,
                "securities": selected_securities,
                "offline": (offline_ids, offline),
                "prefixes": prefixes,
            },
        )
    validation = {"inputs": inputs, "identities": {}, "prefix_records": [], "revision_prefix_version": 1}
    if hasattr(group, "observation_choice_handoffs"):
        validation["observation_choice_handoffs"] = group.observation_choice_handoffs()
    work = {"reused_history_prefixes": 0, "full_history_evaluations": 0, "checked_bars": 0}
    for security in selected_securities:
        public_id = security["public_id"]
        if public_id in offline_ids:
            decisions.append({"public_id": public_id, "eligible": False, "reasons": [offline["reason"]]})
            continue
        reasons = set()
        if set(security["adjustments"]) != set(ADJUSTMENTS):
            decisions.append({"public_id": public_id, "eligible": False, "reasons": ["missing_views"]})
            continue
        prefix = prefixes.get(public_id)
        if prefix is not None and "source_revision" in prefix:
            prefix = preparation.revised(binding, group, public_id, prefix, work)
        members, observations = _metadata(group, public_id)
        if prefix is None:
            work["full_history_evaluations"] += 1
        try:
            try:
                views, coverage = preparation.histories(binding, group, public_id, prefix)
            except HistoryPrefixChanged:
                if prefix is not None and "accepted_span" in prefix:
                    raise ValueError(
                        "outside-span extension overlaps accepted history; bounded serving recovery required"
                    )
                if prefix is not None and "source_revision" in prefix:
                    raise ValueError("source revision prefix changed; owning bounded reconciliation required")
                prefix = None
                work["full_history_evaluations"] += 1
                views, coverage = preparation.histories(binding, group, public_id, None)
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
        equal_source_dates = (
            accepted_dates(group, previous, inputs, public_id=public_id, sessions=set(sessions))
            if sessions
            else {}
        )
        outside = bool(prefix and "accepted_span" in prefix)
        before = [bar for bar in raw if outside and bar["session"] < prior["first_session"]]
        after = [bar for bar in raw if not outside or bar["session"] > prior["last_session"]]
        if outside and len(before) + len(after) != len(raw):
            raise ValueError("serving extension reopened its accepted numerical span")
        final_session = after[-1]["session"] if after else prior["last_session"] if prior else None
        if (
            not after
            and prior
            and prior["coverage"]["session_count"] >= 2
            and prior.get("before_last_raw_bar") is None
        ):
            # Metadata-only reuse preserves a legacy checkpoint's capabilities.
            # It cannot claim a preceding-row boundary that was never published.
            # A later append supplies that boundary from the accepted last row.
            validation.pop("revision_prefix_version", None)
        before_last_reasons = set(
            (prior.get("before_last_price_reasons") or ())
            if outside and not after
            else prior["price_reasons"]
            if prior
            else ()
        )
        if not sessions and prior is None:
            reasons.add("empty_history")
        for adjustment, view in views.items():
            if [b["session"] for b in view["bars"]] != sessions:
                price_reasons.add("incompatible_sessions")
                if sessions and [b["session"] for b in view["bars"] if b["session"] < final_session] != [
                    day for day in sessions if day < final_session
                ]:
                    before_last_reasons.add("incompatible_sessions")
            for bar in view["bars"]:
                if bar["session"] in equal_source_dates.get(public_id, ()):
                    continue
                bar_reasons = _bar_reasons(bar, adjustment)
                price_reasons.update(bar_reasons)
                if final_session and bar["session"] < final_session:
                    before_last_reasons.update(bar_reasons)
                work["checked_bars"] += 1
        reasons.update(price_reasons)
        missing = set(prior["missing_sessions"] if prior else ())
        gap_bars = list(prior["gap_bars"] if prior else ())
        source_sessions = set(prior["source_sessions_in_gaps"] if prior else ())
        if prefix and prefix["refresh_gap_sources"]:
            source_sessions = group.source_sessions(start=min(missing), end=max(missing)) & missing
        first_session = (
            before[0]["session"]
            if before
            else prior["first_session"]
            if prior
            else (sessions[0] if sessions else None)
        )
        last_session = final_session
        scopes = []
        if before:
            labels = sorted(
                {
                    run["ticker"]
                    for run in prior["ticker_ranges"]
                    if run["first_session"] == prior["first_session"]
                }
            )
            scopes.append(
                (
                    before[0]["session"],
                    prior["first_session"],
                    before,
                    [*before, {"session": prior["first_session"], "tickers": labels}],
                )
            )
        if after:
            end = (date.fromisoformat(after[-1]["session"]) + timedelta(days=1)).isoformat()
            start = (
                (date.fromisoformat(prior["last_session"]) + timedelta(days=1)).isoformat()
                if prior
                else after[0]["session"]
            )
            scopes.append((start, end, after, [*([prior["last_raw_bar"]] if prior else []), *after]))
        for start, end, fresh, span in scopes:
            fresh_sessions = [bar["session"] for bar in fresh]
            expected = set(expected_market_session_coverage(start, end).session_dates)
            new_missing = expected - set(fresh_sessions)
            missing.update(new_missing)
            if new_missing:
                source_sessions.update(
                    group.source_sessions(start=min(new_missing), end=max(new_missing)) & new_missing
                )
            for left, right in zip(span, span[1:]):
                if left["session"] < right["session"] and any(
                    left["session"] < day < right["session"] for day in new_missing
                ):
                    gap_bars.extend(
                        {"session": bar["session"], "tickers": bar["tickers"]} for bar in (left, right)
                    )
            if set(fresh_sessions) - expected:
                reasons.add("nonmarket_sessions")
                price_reasons.add("nonmarket_sessions")
                if {day for day in fresh_sessions if day < final_session} - expected:
                    before_last_reasons.add("nonmarket_sessions")
        gap_bars = sorted({bar["session"]: bar for bar in gap_bars}.values(), key=lambda bar: bar["session"])
        explained = _rename_exclusions(
            group, members, observations, gap_bars, missing, source_sessions=source_sessions
        )
        if missing - explained:
            reasons.add("unexplained_missing_sessions")
        unresolved_ends = _unresolved_ends(group, security, members, first_session, last_session)
        cutoff = max(unresolved_ends) if unresolved_ends else None
        if cutoff is not None and cutoff >= last_session:
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
            if outside:
                old_ranges = prior["ticker_ranges"]
                if before:
                    old_ranges = extend_ticker_ranges(
                        {
                            "ticker_ranges": _ticker_ranges(
                                [bar for bar in dated_bars if bar["session"] < prior["first_session"]]
                            ),
                            "last_session": before[-1]["session"],
                        },
                        old_ranges,
                    )
                ticker_ranges = extend_ticker_ranges(
                    {**prior, "ticker_ranges": old_ranges},
                    _ticker_ranges([bar for bar in dated_bars if bar["session"] > prior["last_session"]]),
                )
            else:
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
        # Dates/labels are a separate projection of the accepted history. Range
        # changes consume it without reopening or numerically checking old bars.
        session_dates = (
            sorted([*prior["session_dates"], *sessions])
            if prior and "session_dates" in prior
            else sessions
            if prior is None
            else None
        )
        accepted_range = (
            next(
                (
                    item
                    for old in previous
                    if any(
                        d["public_id"] == public_id and d.get("publication_start_after_unresolved") == cutoff
                        for d in old["decisions"]
                    )
                    for item in old["securities"]
                    if item["public_id"] == public_id
                ),
                None,
            )
            if cutoff is not None and session_dates is None
            else None
        )
        if session_dates is None and (
            rebuild_projection
            and (
                cutoff is not None
                and accepted_range is None
                or prior
                and prior.get("session_projection_version") == 1
            )
            or prior
            and prior.get("session_projection_version") is None
            and not reasons
            and cutoff is not None
            and accepted_range is None
        ):
            # An applicable completed numerical prefix already approves the
            # whole selected history. A new publication range needs only its
            # dates. Lift an unmarked legacy projection once, retaining all
            # numerical/calendar outcomes and publishing the dates below.
            session_dates = group.serving_session_dates(public_id)
        validation["prefix_records"].append(
            {
                **history_summary,
                **(
                    {"session_dates": session_dates, "session_projection_version": 1}
                    if session_dates is not None
                    else {}
                ),
                "validated_through": max(
                    ([prior["validated_through"]] if prior else [])
                    + [view["bars"][-1]["session"] for view in views.values() if view["bars"]]
                ),
                "last_raw_bar": (
                    {"session": after[-1]["session"], "tickers": after[-1]["tickers"]}
                    if after
                    else prior["last_raw_bar"]
                ),
                "price_reasons": sorted(price_reasons),
                "before_last_raw_bar": (
                    {"session": after[-2]["session"], "tickers": after[-2]["tickers"]}
                    if len(after) >= 2
                    else prior["last_raw_bar"]
                    if after and prior
                    else {"session": before[-1]["session"], "tickers": before[-1]["tickers"]}
                    if before and prior["coverage"]["session_count"] == 1
                    else prior.get("before_last_raw_bar")
                    if prior
                    else None
                ),
                "before_last_price_reasons": sorted(before_last_reasons)
                if sessions and not (outside and not after and prior.get("before_last_price_reasons") is None)
                else prior.get("before_last_price_reasons"),
                "missing_sessions": sorted(missing),
                "gap_bars": gap_bars,
                "source_sessions_in_gaps": sorted(source_sessions),
            }
        )
        if reasons:
            continue

        if cutoff is not None:
            # Every selected view passed its full numerical/session checks above.
            # Publish only assigned observations strictly after the last relevant
            # uncertainty. This never assigns a row or merges an identity.
            if session_dates is not None:
                supported_dates = [day for day in session_dates if day > cutoff]
                first_supported = supported_dates[0]
                supported_count = len(supported_dates)
                supported_ranges = [
                    {**item, "first_session": max(item["first_session"], first_supported)}
                    for item in ticker_ranges
                    if item["last_session"] >= first_supported
                ]
            else:
                # Legacy partial publications already selected this exact range.
                # Carry it through metadata changes and suffix extensions; new
                # applicable ranges acquire their bounded date projection above.
                accepted = accepted_range
                if accepted is None:
                    raise ValueError(
                        "legacy partial history lacks dated projection; explicit catalogue rebuild required"
                    )
                supported = [bar for bar in dated_bars if bar["session"] > cutoff]
                through = prior["last_session"]
                if accepted["first_session"] > through:
                    first_supported = supported[0]["session"]
                    supported_count = len(supported)
                    supported_ranges = _ticker_ranges(supported)
                else:
                    first_supported = accepted["first_session"]
                    supported_count = (
                        accepted["coverage"]["session_count"]
                        + len(supported)
                        - int(accepted["last_session"] > through)
                    )
                    retained = {
                        **accepted,
                        "last_session": through,
                        "ticker_ranges": [
                            {**item, "last_session": min(item["last_session"], through)}
                            for item in accepted["ticker_ranges"]
                            if item["first_session"] <= through
                        ],
                    }
                    supported_ranges = extend_ticker_ranges(retained, _ticker_ranges(supported))
            history_summary = {
                **history_summary,
                "first_session": first_supported,
                "ticker_ranges": supported_ranges,
                "coverage": {
                    **history_summary["coverage"],
                    "session_count": supported_count,
                    "explained_missing_sessions": sorted(
                        day for day in explained if first_supported < day < last_session
                    ),
                },
            }
            decisions[-1]["publication_start_after_unresolved"] = cutoff
            ticker_ranges = history_summary["ticker_ranges"]

        # Metadata can come from a superseded contributing source whose dated
        # assignments extend beyond the selected numerical history. Project its
        # known dates onto every publication range, including full histories.
        observations = [
            {
                **item,
                "effective_from": max(item["effective_from"], history_summary["first_session"]),
                "effective_through": min(item["effective_through"], last_session),
            }
            if item["effective_from"] is not None
            else item
            for item in observations
            if item["effective_from"] is None
            or (
                item["effective_through"] >= history_summary["first_session"]
                and item["effective_from"] <= last_session
            )
        ]

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
    completed = {
        "locator": min(group.references["pricing"]),
        "references": group.references,
        "policy": policy(),
        "decision_generation": uuid4().hex,
        "securities": securities,
        "decisions": decisions,
        "history_validation": validation,
        "validation_work": work,
    }
    if offline is not None:
        completed["offline_ownership"] = offline
    from .gaps import complete_presence

    complete_presence(group, completed, previous)
    return completed
