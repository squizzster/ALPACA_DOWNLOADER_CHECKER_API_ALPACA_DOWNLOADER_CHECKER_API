"""Dated trading labels from native security events and two observed price routes."""

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from functools import cache
from itertools import combinations

from libs.alpaca_data_api_solver import NEW_YORK, instant
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence
from libs.common.us_equity_market_session_calendar import market_sessions_between

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .name_change import (
    _complete_action_query_covers,
    _non_observation_conflict,
    _positive_decimal,
    name_change_date,
)
from .occupancy_anchor import positive_occupancy_action_date

VERSION = 104
NAME = "native_action_anchored_former_ticker_occupation"


def complete_source_scope(inputs):
    """Both observation and inference require the same unassigned, fully materialized inputs."""
    return len(inputs) >= 2 and all(
        getattr(view, "materialization_scope", None) == "complete_history"
        and getattr(view, "accepted_report", None) is None
        and getattr(view, "retained_split", None) is None
        for view in inputs.values()
    )


@dataclass(frozen=True)
class NativeTickerAnchor:
    day: date
    action_id: str
    action_type: str
    cusip: str
    terminal: str
    asset_id: str


def anchor_cusip(action, symbol):
    kind = action.get("action_type")
    if kind == "cash_dividend" and action.get("symbol") == symbol:
        return action.get("cusip")
    if (
        kind in ("stock_merger", "stock_and_cash_merger")
        and action.get("acquirer_symbol") == symbol
        and action.get("acquiree_symbol") not in (None, "", symbol)
        and action.get("acquiree_cusip") not in (None, "", action.get("acquirer_cusip"))
    ):
        return action.get("acquirer_cusip")
    return None


def native_action_anchors(lineage, symbol):
    """Select a dividend recipient or a stock-paying acquirer under its own exact CUSIP."""
    if isinstance(lineage, ProviderEvidence):
        # Fresh metadata selects the sampling date from this exact provider body.
        # Its downstream correlation consumes the same positive or empty selection.
        # The memo is local to the immutable action/lookup inputs and never serialized.
        memo = getattr(lineage, "_former_ticker_anchors", None)
        if memo is None:
            memo = lineage._former_ticker_anchors = {}
        if symbol not in memo:
            memo[symbol] = tuple(_select_anchors(lineage, symbol))
        return memo[symbol]
    return tuple(_select_anchors(lineage, symbol))


def _archive_asset(asset):
    return asset and (asset.get("status") == "inactive" or asset.get("exchange") == "OTC")


def _select_anchors(lineage, symbol):
    @cache
    def security(cusip):
        return exact_cusip_lookup_asset(lineage, cusip)

    for action in lineage.get("corporate_actions", ()):
        cusip = anchor_cusip(action, symbol)
        if not isinstance(cusip, str) or not cusip or not isinstance(action.get("id"), str) or not action["id"]:
            continue
        asset = security(cusip)
        if not _archive_asset(asset) or asset.get("symbol") in (None, "", symbol):
            continue
        try:
            if action["action_type"] == "cash_dividend":
                day = positive_occupancy_action_date(action, symbol, cusip)
            else:
                # An explicit effective date and positive stock consideration name
                # the paying security. The acquired issue keeps its own history.
                if not all(_positive_decimal(action.get(key)) for key in ("acquirer_rate", "acquiree_rate")):
                    continue
                if action["action_type"] == "stock_and_cash_merger" and not _positive_decimal(
                    action.get("cash_rate")
                ):
                    continue
                day = date.fromisoformat(action["effective_date"])
                other = security(action["acquiree_cusip"])
                if other is not None and other["id"] == asset["id"]:
                    continue
        except (KeyError, ValueError, TypeError, InvalidOperation):
            continue
        if day:
            yield NativeTickerAnchor(
                day, action["id"], action["action_type"], cusip, asset["symbol"], asset["id"]
            )


def _raw_source(sources, symbol):
    return next((item for item in sources.get(symbol, ()) if item[1]["adjustment"] == "raw"), None)


def _missing_lookup(facts, symbol):
    lookups = [
        item
        for item in facts.get("asset_lookups", ())
        if item.get("lookup_kind") == "symbol" and item.get("lookup") == symbol
    ]
    if len(lookups) == 1 and (
        lookups[0].get("found"),
        lookups[0].get("outcome"),
        lookups[0].get("http_status"),
        lookups[0].get("asset"),
    ) == (False, "not_found", 404, None):
        return lookups[0]
    return None


def _later_occupation(facts, symbol, owner, arrival, asof):
    """The later prints must start under another observed CUSIP's rename into this ticker."""
    actions = facts.get("corporate_actions", ())
    arrivals = [
        action
        for action in actions
        if action.get("action_type") == "name_change"
        and action.get("new_symbol") == symbol
        and action.get("old_symbol") not in (None, "", symbol)
        and name_change_date(action, actions)[0] == arrival
    ]
    if len(arrivals) != 1:
        return None
    entry = arrivals[0]
    cusip = entry.get("new_cusip")
    later = exact_cusip_lookup_asset(facts, cusip)
    if not entry.get("id") or not cusip or not later or later["id"] == owner:
        return None
    if asof == "-":
        return (entry["id"],)
    # A missing symbol today cannot describe a pinned historical mapping. The
    # named later occupant must also have left before this request's asof.
    exits = [
        (a, day)
        for a in actions
        if a.get("action_type") == "name_change"
        and a.get("old_symbol") == symbol
        and a.get("old_cusip") == cusip
        and a.get("new_symbol") not in (None, "", symbol)
        and (day := name_change_date(a, actions)[0])
        and arrival < day
        and day.isoformat() < asof
    ]
    if len(exits) == 1:
        action, departure = exits[0]
        through = date.fromisoformat(asof)
        if (
            action.get("id")
            and _complete_action_query_covers(facts, symbol, through)
            and not _non_observation_conflict(
                facts, symbol, departure + timedelta(days=1), through, set(), set()
            )
        ):
            return entry["id"], action["id"]
    return None


def _label_mapping(facts, symbol, owner, request, last, arrival):
    """Identify requests that had literal semantics; all other mappings need dated evidence."""
    if not isinstance(facts, ProviderEvidence):
        return _select_label_mapping(facts, symbol, owner, request, last, arrival)
    memo = getattr(facts, "_former_ticker_mappings", None)
    if memo is None:
        memo = facts._former_ticker_mappings = {}
    key = symbol, owner, request["asof"], request["request_to"], last, arrival
    if key not in memo:
        memo[key] = _select_label_mapping(facts, symbol, owner, request, last, arrival)
    return memo[key]


def _select_label_mapping(facts, symbol, owner, request, last, arrival):
    asof = request["asof"]
    later = _later_occupation(facts, symbol, owner, arrival, asof) if arrival else None
    if asof == "-":
        return True, later
    missing = _missing_lookup(facts, symbol)
    if missing is None or asof != request["request_to"][:10] or asof <= last.isoformat():
        return False, later
    observed = missing.get("observed_at_utc")
    observed_date = instant(observed).astimezone(NEW_YORK).date().isoformat() if observed else None
    return observed_date == asof or bool(later), later


def _candidates(symbols, evidence):
    candidates = defaultdict(set)
    for symbol in symbols:
        for a in native_action_anchors(evidence[symbol], symbol):
            if a.terminal in symbols:
                candidates[symbol, a.terminal, a.cusip, a.asset_id].add(a)
    return candidates


def literal_edge_requests(inputs, evidence):
    """Select up to two literal daily bars for a genuinely ambiguous historical mapping.

    Use the accepted raw bounds and already materialized former date grid. Ordinary
    metadata discovery and completed price selection remain the upstream authorities.
    """
    routes = {}
    if len(inputs) < 2:
        return routes
    for (symbol, terminal, _cusip, owner), anchors in _candidates(inputs, evidence).items():
        source, archive = inputs[symbol], inputs[terminal]
        direct = next((d for d in source.local["downloads"] if d["adjustment"] == "raw"), {})
        mapped = next((d for d in archive.local["downloads"] if d["adjustment"] == "raw"), {})
        if any(
            not d.get("from") or not d.get("to") or (d.get("timeframe"), d.get("feed")) != ("1Day", "sip")
            for d in (direct, mapped)
        ):
            continue
        first = max(direct["from"], mapped["from"])[:10]
        last = min(direct["to"], mapped["to"])[:10]
        if (
            mapped["asof"] == "-"
            or mapped["asof"] <= last
            or not any(first <= a.day.isoformat() <= last for a in anchors)
        ):
            continue
        values = source.views["raw"]
        stop = bisect_right(values, last, key=lambda row: row[0][:10])
        arrival = date.fromisoformat(values[stop][0][:10]) if stop < len(values) else None
        literal, _ = _label_mapping(evidence[symbol], symbol, owner, direct, date.fromisoformat(last), arrival)
        if not literal:
            routes[symbol, terminal] = tuple(dict.fromkeys((first, last)))
    return routes


def _conflict(evidence, former, terminal, cusip, owner, first, last, named_actions):
    for facts in evidence.values():
        # A cash dividend naming the historical ticker with no CUSIP supplies
        # no competing identity. A different or unresolved named CUSIP does.
        unattributed = {
            a["id"]
            for a in facts.get("corporate_actions", ())
            if a.get("action_type") == "cash_dividend"
            and a.get("symbol") == former
            and not a.get("cusip")
            and a.get("id")
        }
        if _non_observation_conflict(facts, former, first, last, {(owner, cusip)}, unattributed | named_actions):
            return True
        # Even a same-issuer dated action under the other spelling competes
        # with the proposed trading label. An asset.exchange snapshot does not.
        if _non_observation_conflict(facts, terminal, first, last, set(), set()):
            return True
    return False


def _same_fields(left, right, fields):
    return all(left[k] == right[k] or Decimal(left[k]) == Decimal(right[k]) for k in fields)


def _raw_agreement(direct, mapped):
    """Identify an equal raw bar or a contained, genuinely traded mapped aggregate."""
    volume, trades = Decimal(mapped["v"]), Decimal(mapped["n"])
    if min(volume, trades) <= 0 or not _same_fields(direct, mapped, ("o", "c")):
        return None
    if _same_fields(direct, mapped, ("v", "n", "h", "l", "vw")):
        return "equal"
    if (
        Decimal(direct["v"]) > volume
        and Decimal(direct["n"]) > trades
        and Decimal(direct["h"]) >= Decimal(mapped["h"])
        and Decimal(direct["l"]) <= Decimal(mapped["l"])
        and Decimal(direct["l"]) <= Decimal(mapped["vw"]) <= Decimal(direct["h"])
    ):
        return "partial"
    return None


@dataclass(frozen=True)
class FormerTickerPeriods:
    terminal: str
    intervals: tuple
    partial_dates: frozenset

    def interval_on(self, day):
        return next((item for item in self.intervals if item.contains(day)), None)


def _observed_row(rows, dates, day):
    i = bisect_left(dates, day)
    return rows[i] if i < len(dates) and dates[i] == day else None


def _literal_outcomes(facts, symbol, terminal, generations, asofs):
    """Consume only actual literal SIP observations produced for this ticker."""
    results = {}
    sample = facts.get("historical_trade_tape", {})
    if (sample.get("symbol"), sample.get("asof"), sample.get("feed")) == (symbol, "-", "sip"):
        if sample.get("outcome") == "no_trades":
            results[date.fromisoformat(sample["session"])] = False
    bundle = facts.get("former_ticker_literal_bars", {}).get(terminal, {})
    if (
        not bundle
        or not generations
        or not all(generations.get(label) for label in (symbol, terminal))
        or bundle.get("source_asofs") != asofs
        or bundle.get("source_generations")
        != {symbol: generations.get(symbol), terminal: generations.get(terminal)}
    ):
        return results
    for item in bundle.get("observations", ()):
        if (item.get("symbol"), item.get("asof"), item.get("feed"), item.get("adjustment")) != (
            symbol,
            "-",
            "sip",
            "raw",
        ):
            continue
        day = date.fromisoformat(item["session"])
        if item["outcome"] == "no_bars":
            results[day] = False
        elif item["outcome"] == "observed" and day not in results:
            results[day] = item["bar"]
    return results


def _occupation(symbol, terminal, cusip, owner, anchors, sources, evidence, generations):
    from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval

    facts = evidence[symbol]
    current = exact_symbol_lookup_asset(evidence[terminal], terminal)
    if not _archive_asset(current) or current["id"] != owner:
        return None, "terminal_symbol_lookup_does_not_confirm_cusip_owner"
    direct_input, mapped_input = _raw_source(sources, symbol), _raw_source(sources, terminal)
    if not direct_input or not mapped_input:
        return None, "missing_raw_source"
    direct, request, dates = direct_input
    mapped, other_request, mapped_dates = mapped_input
    if (
        not dates
        or not mapped_dates
        or any((meta.get("timeframe"), meta.get("feed")) != ("1Day", "sip") for meta in (request, other_request))
    ):
        return None, "missing_daily_sip_observations"
    first, last = max(dates[0], mapped_dates[0]), min(dates[-1], mapped_dates[-1])
    if first > last:
        return None, "no_shared_observation_dates"
    if other_request["asof"] == "-" or other_request["asof"] <= last.isoformat():
        return None, "terminal_route_not_mapped_after_observed_episode"
    witnesses = [a for a in anchors if first <= a.day <= last]
    if not witnesses:
        return None, "no_dated_occupancy_action_in_observed_episode"
    named_actions = {a.action_id for a in witnesses}

    @cache
    def direct_row(day):
        return _observed_row(direct.rows, dates, day)

    @cache
    def agreement(day):
        left = direct_row(day)
        right = _observed_row(mapped.rows, mapped_dates, day)
        return _raw_agreement(left, right) if left is not None and right is not None else None

    stop = bisect_right(dates, last)
    arrival = dates[stop] if stop < len(dates) else None
    literal, later = _label_mapping(facts, symbol, owner, request, last, arrival)
    observed = _literal_outcomes(
        facts, symbol, terminal, generations, {symbol: request["asof"], terminal: other_request["asof"]}
    )
    confirmed, contradicted = set(), set()
    for day, result in observed.items():
        if not first <= day <= last:
            continue
        if result is False or not direct_row(day) or _raw_agreement(result, direct_row(day)) != "equal":
            contradicted.add(day)
        else:
            confirmed.add(day)

    @cache
    def action_scope(left, right):
        if not all(
            _complete_action_query_covers(evidence[s], s, d) for s in (symbol, terminal) for d in (left, right)
        ):
            return "corporate_action_coverage_incomplete"
        if _conflict(evidence, symbol, terminal, cusip, owner, left, right, named_actions):
            return "competing_dated_occupancy"
        return None

    def interval(left, right, identifiers):
        rule = AppliedOwnershipIntervalRule(1, NAME, tuple(sorted(identifiers)))
        return TickerOwnershipInterval(symbol, cusip, owner, left, right + timedelta(days=1), (rule,))

    # The dated action names this equity on its ex-date or merger effective date. A current or historical
    # mapping may also echo an older, unreported spelling. Use only literal
    # semantics or observations to extend the spelling beyond the dated anchors.
    plausible = {a.day for a in witnesses if a.day not in contradicted and agreement(a.day)}
    if not plausible:
        return (
            None,
            "literal_observation_contradicts_mapped_trades"
            if contradicted
            else "raw_trading_observations_disagree",
        )
    bounds = confirmed | plausible
    left, right = (first, last) if literal else (min(bounds), max(bounds))
    if arrival and not later and not confirmed and request["asof"] != "-":
        left, right = min(plausible), max(plausible)
    covered = dates[bisect_left(dates, left) : bisect_right(dates, right)]
    other = mapped_dates[bisect_left(mapped_dates, left) : bisect_right(mapped_dates, right)]
    issue = action_scope(left, right)
    if not issue and any(left <= day <= right for day in contradicted):
        issue = "literal_observation_contradicts_mapped_trades"
    if not issue and (
        covered != other
        or tuple(d.isoformat() for d in covered) != market_sessions_between(left.isoformat(), right.isoformat())
    ):
        issue = "different_or_incomplete_observed_session_grids"
    if not issue:
        partial = set()
        full_close = None
        varied = False
        for day in covered:
            match = agreement(day)
            if match == "equal":
                close = Decimal(direct_row(day)["c"])
                if full_close is None:
                    full_close = close
                elif close != full_close:
                    varied = True
            elif match == "partial" and left < day < right:
                partial.add(day)
            else:
                issue = "raw_trading_observations_disagree"
                break
        if not issue and (varied or left == right):
            support = {a.action_id for a in witnesses if left <= a.day <= right}
            if later and literal:
                support.update(later)
            result = FormerTickerPeriods(terminal, (interval(left, right, support),), frozenset(partial))
            return result, "spelling_supported_only_between_dated_witnesses" if (left, right) != (
                first,
                last,
            ) else None
        issue = issue or "no_varying_full_traded_observations"
    # A different date's missing bar, competing action or price disagreement
    # cannot erase a native event date with independently compatible trades.
    isolated = [
        interval(day, day, {a.action_id for a in witnesses if a.day == day})
        for day in sorted(plausible)
        if not action_scope(day, day)
    ]
    if not isolated:
        return None, issue
    partial = frozenset(day for day in plausible if agreement(day) == "partial")
    return FormerTickerPeriods(terminal, tuple(isolated), partial), issue


def select(sources, evidence, *, pricing_generations=None):
    """Consume the selected group's provider observations and already materialized raw routes."""
    if len(sources) < 2 or not sources.keys() <= evidence.keys():
        return {}, {}
    choices, limits = defaultdict(list), defaultdict(list)
    for (symbol, terminal, cusip, owner), anchors in _candidates(sources, evidence).items():
        result, limit = _occupation(
            symbol, terminal, cusip, owner, anchors, sources, evidence, pricing_generations
        )
        if result is not None:
            choices[symbol].append(result)
            choices[terminal].append(result)
        if limit:
            issue = {
                "reason_code": "former_ticker_occupation_evidence_limit",
                "former_symbol": symbol,
                "terminal_symbol": terminal,
                "candidate_asset_id": owner,
                "candidate_cusip": cusip,
                "candidate_actions": [
                    {"date": a.day.isoformat(), "action_type": a.action_type, "action_id": a.action_id}
                    for a in sorted(anchors, key=lambda a: (a.day, a.action_id))
                ],
                "limit": limit,
            }
            limits[symbol].append(issue)
            limits[terminal].append(issue)
    # Two distinct spellings or owners can be equally plausible on the same
    # dates. Remove only their overlap from both sides of those associations.
    blocked = defaultdict(list)
    for items in choices.values():
        for one, two in combinations(items, 2):
            for left in one.intervals:
                for right in two.intervals:
                    start = max(left.start_date_inclusive, right.start_date_inclusive)
                    end = min(left.end_date_exclusive, right.end_date_exclusive)
                    if start >= end:
                        continue
                    blocked[one].append((start, end))
                    blocked[two].append((start, end))
                    keys = sorted({left.identity_key(), right.identity_key()})
                    issue = {
                        "reason_code": "competing_former_ticker_occupations",
                        "from": start.isoformat(),
                        "end_exclusive": end.isoformat(),
                        "candidates": [
                            {"former_symbol": label, "cusip": cusip, "asset_id": asset_id}
                            for label, cusip, asset_id in keys
                        ],
                    }
                    for label in {one.terminal, two.terminal, left.historical_symbol, right.historical_symbol}:
                        if issue not in limits[label]:
                            limits[label].append(issue)
    remaining = {}
    for item, overlaps in blocked.items():
        periods = []
        cuts = sorted(set(overlaps))
        for value in item.intervals:
            start, end = value.start_date_inclusive, value.end_date_exclusive
            for left, right in cuts:
                if right <= start or left >= end:
                    continue
                if start < left:
                    periods.append(replace(value, start_date_inclusive=start, end_date_exclusive=left))
                start = max(start, right)
            if start < end:
                periods.append(replace(value, start_date_inclusive=start, end_date_exclusive=end))
        remaining[item] = replace(item, intervals=tuple(periods))
    return {
        symbol: parts
        for symbol, items in choices.items()
        if (parts := tuple(result for item in items if (result := remaining.get(item, item)).intervals))
    }, limits
