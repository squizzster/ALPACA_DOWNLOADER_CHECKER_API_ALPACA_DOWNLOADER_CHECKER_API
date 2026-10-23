"""A resumed traded episode can be anchored by its own dated departure."""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from itertools import groupby

from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .action_identity import exact_cusip_lookup_asset
from .current_symbol_reuse import _complete_corporate_action_query, _current_identity
from .occupancy_anchor import _action_anchor
from .ownership_interval_rules import TickerOwnershipInterval
from .security_identity import security_identity_for_alpaca_assets
from .ticker_reuse_gap import adjustment_session_consensus, assess_ticker_reuse_gap
from .uuid_lineage import build_uuid_lineage_edges


@dataclass(frozen=True)
class TerminalTradedEpisode:
    first: date
    last: date
    asset_id: str
    cusip: str
    terminated_during_reuse_gap: bool = False
    bounded_by_inert_bridges: bool = False
    initial_before_reuse_gap: bool = False
    preceding_inert_first: date | None = None
    preceding_inert_last: date | None = None


@dataclass(frozen=True)
class DatedDeparturePrefix:
    first: date
    last: date
    interval: TickerOwnershipInterval


def _current_same_uuid_alias(lineage, action, asset):
    if action.get("action_type") != "name_change":
        return False
    successor = exact_cusip_lookup_asset(lineage, action.get("new_cusip"))
    if successor is None or successor["id"] != asset["id"] or successor.get("symbol") != asset.get("symbol"):
        return False
    edges, _ = build_uuid_lineage_edges(lineage)
    current = (action.get("new_symbol"), action.get("new_cusip"))
    before = date.fromisoformat(action["process_date"])
    seen = set()
    while current[0] != asset.get("symbol"):
        if current in seen:
            return False
        seen.add(current)
        outgoing = [
            edge
            for edge in edges
            if (edge.predecessor_symbol, edge.predecessor_cusip) == current
            and edge.predecessor_asset_id == asset["id"]
            and edge.effective_date > before
        ]
        if len(outgoing) != 1:
            return False
        edge = outgoing[0]
        if edge.edge_type != "name_change" or edge.successor_asset_id != asset["id"]:
            return False
        current = (edge.successor_symbol, edge.successor_cusip)
        before = edge.effective_date
    return True


def terminal_episode_candidates(symbol, lineage):
    occupants = {
        item["id"] for item in lineage.get("identities", []) if item.get("symbol") == symbol and item.get("id")
    } | {
        item["asset_id"]
        for item in lineage.get("symbol_occupancy_evidence", [])
        if item.get("symbol") == symbol and item.get("asset_id")
    }
    if len(occupants) < 2:
        return ()
    candidates = []
    for action in lineage.get("corporate_actions", []):
        kind = action.get("action_type")
        role = "old" if kind == "name_change" else "acquiree"
        if kind not in ("name_change", "cash_merger", "stock_merger") or action.get(role + "_symbol") != symbol:
            continue
        cusip = action.get(role + "_cusip")
        asset = exact_cusip_lookup_asset(lineage, cusip)
        if asset is None or not action.get("id"):
            continue
        try:
            when = date.fromisoformat(action["process_date" if kind == "name_change" else "effective_date"])
        except (KeyError, TypeError, ValueError):
            continue
        if asset.get("symbol") != symbol:
            if not _current_same_uuid_alias(lineage, action, asset) or not any(
                (item.get("symbol"), item.get("cusip"), item.get("asset_id")) == (symbol, cusip, asset["id"])
                and any(
                    record.get("action_id") == action["id"]
                    and record.get("role") == "old"
                    and record.get("action_type") == "name_change"
                    for record in item.get("evidence", [])
                )
                for item in lineage.get("symbol_occupancy_evidence", [])
            ):
                continue
        candidates.append((when, asset["id"], cusip))
    return tuple(candidates)


def _inert(row):
    try:
        prices = tuple(Decimal(str(row[key])) for key in ("o", "h", "l", "c"))
        if Decimal(str(row["v"])) == 0 and Decimal(str(row["n"])) == 0 and len(set(prices)) == 1:
            return prices[0]
    except (KeyError, InvalidOperation, ValueError):
        pass
    return None


def _traded_prices(rows):
    return {
        tuple(row[key] for key in ("o", "h", "l", "c"))
        for row in rows
        if Decimal(str(row.get("v", "0"))) > 0 and Decimal(str(row.get("n", "0"))) > 0
    }


def has_prolonged_carry_forward(rows):
    return any(value is not None and sum(1 for _ in group) >= 100 for value, group in groupby(rows, _inert))


def has_informative_trading(rows):
    return len(_traded_prices(rows)) >= 2


def shared_inert_bridges(work_items, *, include_edges=False, before=None):
    """Return only complete, constant inert separators shared by all views."""
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return ()
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return ()
    if before is not None:
        stop = next((i for i, day in enumerate(sessions) if day >= before), len(sessions))
        views = {mode: (values[:stop], grid[:stop]) for mode, (values, grid) in views.items()}
        rows, sessions = views["raw"]
    bridges = []
    for value, group in groupby(enumerate(rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        first, last = positions[0], positions[-1]
        if value is None or len(positions) < 100 or not include_edges and (first == 0 or last + 1 == len(rows)):
            continue
        if all(
            None not in (values := {_inert(row) for row in other[first : last + 1]}) and len(values) == 1
            for other, _ in views.values()
        ):
            bridges.append((first, last))
    return tuple(bridges)


def _positive_competing_owner(symbol, intervals, asset_id, first, last):
    return any(
        item.historical_symbol == symbol
        and item.asset_id != asset_id
        and (
            item.start_date_inclusive is not None
            and item.start_date_inclusive <= last
            and (item.end_date_exclusive is None or item.end_date_exclusive > first)
            or item.end_date_exclusive is not None
            and first < item.end_date_exclusive <= last
        )
        for item in intervals
    )


def _dividend_backed_current_reuse(
    symbol, lineage, intervals, candidates, current_id, cusip, bridge, first, last, end
):
    """A dated dividend anchors the current issue within an observed resumption.

    It does not establish inception. The shared inert separator and unrelated
    former departure distinguish this episode from a continuing stock's halt.
    """
    # Even an uncertified continuation vetoes a claim that two episodes are
    # unrelated. This screen does not publish or accept UUID membership.
    edges, issues = build_uuid_lineage_edges(lineage, candidates_only=True)
    rejected = {issue["rejected_action_id"] for issue in issues if "rejected_action_id" in issue}
    actions = {action.get("id"): action for action in lineage.get("corporate_actions", [])}
    departures = set()
    # Trust the ownership boundary, not merely a raw action candidate. A known
    # departure does not require resolving the former issue's successor UUID.
    for when, asset_id, former_cusip in candidates:
        if not bridge[0] <= when <= bridge[1] or asset_id == current_id:
            continue
        for interval in intervals:
            if (
                interval.identity_key() != (symbol, former_cusip, asset_id)
                or interval.end_date_exclusive != when
            ):
                continue
            for rule in interval.applied_rules:
                for action_id in rule.supporting_action_ids:
                    action = actions.get(action_id, {})
                    role = "old" if action.get("action_type") == "name_change" else "acquiree"
                    effective = (
                        action.get("process_date")
                        if role == "old"
                        else action.get("effective_date") or action.get("process_date")
                    )
                    if (
                        action_id not in rejected
                        and effective == when.isoformat()
                        and (action.get(role + "_symbol"), action.get(role + "_cusip")) == (symbol, former_cusip)
                    ):
                        departures.add((when, asset_id))
    if not departures or not _complete_corporate_action_query(lineage, min(d[0] for d in departures), end):
        return False
    component = security_identity_for_alpaca_assets(
        (current_id,), edges, tickers=(symbol,), basis="current_reuse_screen"
    ).member_alpaca_asset_ids
    if any(asset_id in component for _, asset_id in departures):
        return False
    anchored = False
    for action in lineage.get("corporate_actions", []):
        if not any(value == symbol for key, value in action.items() if key.endswith("symbol")):
            continue
        try:
            when = date.fromisoformat(
                action.get("ex_date") or action.get("effective_date") or action["process_date"]
            )
        except (KeyError, TypeError, ValueError):
            return False
        if not first <= when <= last:
            continue
        if action.get("action_type") != "cash_dividend" or action.get("cusip") != cusip:
            return False  # A return, structural change or another issue needs its own dated allocation.
        try:
            anchored |= _action_anchor(action, symbol, cusip) == when
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return False
    # A conflicting dated occupant may not have produced an ownership interval.
    for occupant in lineage.get("symbol_occupancy_evidence", []):
        if occupant.get("symbol") != symbol or (occupant.get("asset_id"), occupant.get("cusip")) == (
            current_id,
            cusip,
        ):
            continue
        for record in occupant.get("evidence", []):
            try:
                when = date.fromisoformat(record["process_date"])
            except (KeyError, TypeError, ValueError):
                return False
            if first <= when <= last:
                return False
    return anchored


def assess_bridged_traded_episodes(symbol, lineage, intervals, work_items):
    """Anchor bounded resumed runs, without interpreting inert rows as trades."""
    bridges = shared_inert_bridges(work_items)
    if not bridges:
        return ()
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    rows, source, sessions = views["raw"]
    candidates = terminal_episode_candidates(symbol, lineage)
    current = _current_identity(lineage, symbol)
    episodes = []
    for number, (_, left_end) in enumerate(bridges):
        following = bridges[number + 1] if number + 1 < len(bridges) else None
        start = left_end + 1
        stop = following[0] if following else len(rows)
        first, last = sessions[start], sessions[stop - 1]
        expected = expected_market_session_coverage(first.isoformat(), (last + timedelta(days=1)).isoformat())
        if tuple(day.isoformat() for day in sessions[start:stop]) != expected.session_dates or any(
            not has_informative_trading(values[start:stop]) or has_prolonged_carry_forward(values[start:stop])
            for values, _, _ in views.values()
        ):
            continue
        owners = set()
        current_reuse = False
        if following:
            # A named departure near the first inert row anchors the preceding
            # complete episode. An event anywhere in a long bridge is insufficient.
            for when, asset_id, cusip in candidates:
                left, right = sorted((when, sessions[following[0]]))
                if (
                    sessions[following[0]] <= when <= sessions[following[1]]
                    and (
                        left == right
                        or expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count
                        <= 4
                    )
                    and all(
                        Decimal(str(values[stop - 1]["c"])) == _inert(values[stop])
                        for values, _, _ in views.values()
                    )
                ):
                    owners.add((asset_id, cusip))
        elif current is not None:
            asset_id, cusip, _, _ = current
            endpoint = exact_cusip_lookup_asset(lineage, cusip)
            complete = expected_market_session_coverage(first.isoformat(), source["request_to"])
            if (
                endpoint is None
                or endpoint.get("status") != "active"
                or tuple(day.isoformat() for day in sessions[start:]) != complete.session_dates
            ):
                continue
            if _dividend_backed_current_reuse(
                symbol,
                lineage,
                intervals,
                candidates,
                asset_id,
                cusip,
                (sessions[bridges[number][0]], sessions[left_end]),
                first,
                last,
                date.fromisoformat(source["request_to"][:10]),
            ):
                owners.add((asset_id, cusip))
                current_reuse = True
            for action in lineage.get("corporate_actions", []):
                if (
                    action.get("action_type") != "stock_merger"
                    or (action.get("acquirer_symbol"), action.get("acquirer_cusip")) != (symbol, cusip)
                    or not action.get("id")
                ):
                    continue
                try:
                    when = date.fromisoformat(action["effective_date"])
                    rates = [Decimal(str(action[key])) for key in ("acquirer_rate", "acquiree_rate")]
                except (KeyError, ValueError, InvalidOperation):
                    continue
                if (
                    all(rate.is_finite() and rate > 0 for rate in rates)
                    and sessions[bridges[number][0]] <= when <= sessions[left_end]
                    and expected_market_session_coverage(when.isoformat(), first.isoformat()).session_count <= 4
                ):
                    owners.add((asset_id, cusip))
        if len(owners) != 1:
            continue
        asset_id, cusip = next(iter(owners))
        if _positive_competing_owner(symbol, intervals, asset_id, first, last) or any(
            item.historical_symbol == symbol
            and item.asset_id == asset_id
            and item.start_date_inclusive is not None
            and first < item.start_date_inclusive <= last
            for item in intervals
        ):
            continue
        episodes.append(
            TerminalTradedEpisode(
                first,
                last,
                asset_id,
                cusip,
                bounded_by_inert_bridges=True,
                preceding_inert_first=sessions[bridges[number][0]] if current_reuse else None,
                preceding_inert_last=sessions[left_end] if current_reuse else None,
            )
        )
    return tuple(episodes)


def assess_dated_departure_prefix(symbol, lineage, intervals, work_items):
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return None
    first = last = None
    for value, group in groupby(enumerate(rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        if value is not None and len(positions) >= 100:
            first, last = positions[0], positions[-1]
            break
    if first in (None, 0) or last + 1 >= len(rows):
        return None
    if not has_informative_trading(rows[:first]) or not has_informative_trading(rows[last + 1 :]):
        return None
    for values, _ in views.values():
        inert_values = {_inert(row) for row in values[first : last + 1]}
        if None in inert_values or len(inert_values) != 1 or has_prolonged_carry_forward(values[:first]):
            return None
        if Decimal(str(values[first - 1]["c"])) != next(iter(inert_values)):
            return None
    expected = expected_market_session_coverage(sessions[0].isoformat(), sessions[first].isoformat())
    if tuple(session.isoformat() for session in sessions[:first]) != expected.session_dates:
        return None
    start, end, inactive_start = sessions[0], sessions[first - 1], sessions[first]
    actions = {action.get("id"): action for action in lineage.get("corporate_actions", []) if action.get("id")}
    qualified = []
    for interval in intervals:
        when = interval.end_date_exclusive
        if (
            interval.historical_symbol != symbol
            or when is None
            or not interval.contains(start)
            or not interval.contains(end)
        ):
            continue
        left, right = sorted((when, inactive_start))
        if (
            left != right
            and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
        ):
            continue
        endpoint = exact_cusip_lookup_asset(lineage, interval.cusip)
        if endpoint is None or endpoint["id"] != interval.asset_id:
            continue
        action_ids = {action_id for rule in interval.applied_rules for action_id in rule.supporting_action_ids}
        supported = False
        for action_id in action_ids:
            action = actions.get(action_id, {})
            kind = action.get("action_type")
            role = "old" if kind == "name_change" else "acquiree"
            if kind in ("name_change", "cash_merger", "stock_merger") and (
                action.get(role + "_symbol"),
                action.get(role + "_cusip"),
            ) == (symbol, interval.cusip):
                supported = True
        if not supported:
            continue
        if any(
            other.historical_symbol == symbol
            and other.asset_id != interval.asset_id
            and (
                (
                    other.start_date_inclusive is not None
                    and other.start_date_inclusive <= end
                    and (other.end_date_exclusive is None or other.end_date_exclusive > start)
                )
                or (other.end_date_exclusive is not None and start < other.end_date_exclusive <= end)
            )
            for other in intervals
        ):
            continue
        qualified.append(interval)
    if len({item.identity_key() for item in qualified}) != 1:
        return None
    return DatedDeparturePrefix(start, end, qualified[0])


def _same_day_cash_termination(symbol, cusip, when, lineage, intervals):
    """A cash closing date need not exclude that day's departing-issue trades."""
    supporting = {
        action_id
        for interval in intervals
        for rule in interval.applied_rules
        for action_id in rule.supporting_action_ids
    }
    cash = False
    for action in lineage.get("corporate_actions", []):
        kind = action.get("action_type")
        role = "old" if kind == "name_change" else "acquiree"
        if (
            kind not in ("name_change", "cash_merger", "stock_merger")
            or action.get(role + "_symbol") != symbol
            or action.get("process_date" if kind == "name_change" else "effective_date") != when.isoformat()
        ):
            continue
        if kind != "cash_merger" or action.get(role + "_cusip") != cusip:
            return False
        try:
            rate = Decimal(str(action.get("rate")))
        except InvalidOperation:
            return False
        if not rate.is_finite() or rate <= 0:
            return False
        cash |= action.get("id") in supporting
    return cash


def assess_terminal_traded_episode(symbol, lineage, intervals, work_items):
    candidates = terminal_episode_candidates(symbol, lineage)
    if not candidates:
        return None
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in work_items}
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    rows, sessions = views["raw"]
    if not sessions or any(grid != sessions for _, grid in views.values()):
        return None
    raw_source = next(source for _, source, _ in work_items if source["adjustment"] == "raw")
    gap = assess_ticker_reuse_gap(
        sessions,
        raw_source,
        lineage,
        intervals,
        adjustment_session_consensus((adjustment, grid) for adjustment, (_, grid) in views.items()),
    )
    prior_end = len(rows)
    if gap is not None:
        if not gap.complete_current_suffix_eligible or gap.current_asset_status != "active":
            return None
        prior_end = sessions.index(gap.left_observed_session) + 1
    prior_rows = rows[:prior_end]
    separators = []
    for key, group in groupby(enumerate(prior_rows), lambda pair: _inert(pair[1])):
        positions = [position for position, _ in group]
        first, last = positions[0], positions[-1]
        if key is not None and len(positions) >= 100 and first > 0 and last + 1 < len(prior_rows):
            if _traded_prices(prior_rows[:first]) and len(_traded_prices(prior_rows[last + 1 :])) >= 2:
                separators.append((first, last))
    initial = not separators
    if initial:
        if gap is None or any(
            not has_informative_trading(values[:prior_end]) or has_prolonged_carry_forward(values[:prior_end])
            for values, _ in views.values()
        ):
            return None
        episode_start = 0
    else:
        first, last = separators[-1]
        for values, _ in views.values():
            value = _inert(values[last])
            suffix_first = last
            while suffix_first > first and value is not None and _inert(values[suffix_first - 1]) == value:
                suffix_first -= 1
            if (
                value is None
                or last - suffix_first + 1 < 100
                or has_prolonged_carry_forward(values[last + 1 : prior_end])
            ):
                return None
        episode_start = last + 1
    start, end = sessions[episode_start], sessions[prior_end - 1]
    expected = expected_market_session_coverage(start.isoformat(), (end + timedelta(days=1)).isoformat())
    if tuple(session.isoformat() for session in sessions[episode_start:prior_end]) != expected.session_dates:
        return None
    qualified = {}
    for when, asset_id, cusip in candidates:
        episode_end = end
        # Event processing and the last trading session can differ by a few
        # sessions. That tolerance supports a terminal episode, not a new birth.
        left, right = sorted((when, end))
        if gap is None:
            if when < start or (
                left != right
                and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
            ):
                continue
        else:
            endpoint = exact_cusip_lookup_asset(lineage, cusip)
            owner_intervals = [
                interval
                for interval in intervals
                if interval.identity_key() == (symbol, cusip, asset_id)
                and interval.end_date_exclusive == when
                and interval.contains(start)
            ]
            same_day_cash = (
                initial
                and when == end
                and _same_day_cash_termination(symbol, cusip, when, lineage, owner_intervals)
            )
            if initial and (
                left != right
                and expected_market_session_coverage(left.isoformat(), right.isoformat()).session_count > 4
                or not any(interval.contains(end) or same_day_cash for interval in owner_intervals)
            ):
                continue
            if same_day_cash and any(
                not _traded_prices(values[prior_end - 1 : prior_end]) for values, _ in views.values()
            ):
                # An untraded event-day observation does not establish ownership
                # of that row, but must not erase the independently bounded prefix.
                if prior_end - episode_start < 2:
                    continue
                episode_end = sessions[prior_end - 2]
            known = (
                {
                    item.get("id")
                    for item in lineage.get("identities", [])
                    if item.get("symbol") == symbol and item.get("id")
                }
                | {
                    item.get("asset_id")
                    for item in lineage.get("symbol_occupancy_evidence", [])
                    if item.get("symbol") == symbol and item.get("asset_id")
                }
                | {
                    item.get("asset_id")
                    for item in lineage.get("input_symbol_occupants", [])
                    if item.get("symbol") == symbol and item.get("asset_id")
                }
            )
            known.update(
                item["asset"].get("id")
                for item in lineage.get("asset_lookups", [])
                if item.get("found") is True
                and isinstance(item.get("asset"), dict)
                and item["asset"].get("symbol") == symbol
                and item["asset"].get("id")
            )
            if (
                not (end < when or same_day_cash)
                or when >= gap.right_observed_session
                or endpoint.get("status") != "inactive"
                or asset_id == gap.current_asset_id
                or known != {asset_id, gap.current_asset_id}
                or any(
                    interval.asset_id == asset_id
                    and interval.historical_symbol == symbol
                    and interval.start_date_inclusive is not None
                    and interval.start_date_inclusive > start
                    for interval in intervals
                )
            ):
                continue
        if any(
            interval.historical_symbol == symbol
            and interval.asset_id != asset_id
            and (interval.start_date_inclusive is None or interval.start_date_inclusive <= end)
            and (interval.end_date_exclusive is None or interval.end_date_exclusive > start)
            for interval in intervals
        ):
            continue
        if any(
            item.get("asset_id") not in (None, asset_id)
            for item in lineage.get("symbol_occupancy_evidence", [])
            if (item.get("symbol"), item.get("cusip")) == (symbol, cusip)
        ):
            continue
        previous_end, previous_date = qualified.get((asset_id, cusip), (episode_end, when))
        qualified[asset_id, cusip] = (max(previous_end, episode_end), min(previous_date, when))
    if len(qualified) != 1:
        return None
    (asset_id, cusip), (episode_end, departure_date) = next(iter(qualified.items()))
    return TerminalTradedEpisode(
        start,
        episode_end,
        asset_id,
        cusip,
        gap is not None and end < departure_date,
        initial_before_reuse_gap=initial,
    )
