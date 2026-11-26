"""Preserve a same-asset listing change between dated Alpaca asset observations."""

from bisect import bisect_left
from datetime import date, datetime, time, timedelta, timezone

from libs.alpaca_data_api_solver import NEW_YORK, instant
from libs.common.us_equity_market_session_calendar import (
    market_sessions_between,
    next_market_session_after,
    regular_market_hours,
)

from .projection import last_observed_symbol_lookup

FIELD = "listing_exchange_changes"
# Networks A and C uniquely identify these primary listings. Network B alone
# cannot distinguish its several listing exchanges. Execution venues never date a move.
_LISTING_TAPES = {"NYSE": "A", "NASDAQ": "C"}
_MAX_SESSIONS = 15
_SCHEDULE_FIELDS = {"date_session", "date_prior_session", "date_after_utc"}


def next_dating_time(evidence):
    """The lineage selector only carries the next outstanding handoff."""
    return min(
        (row["date_after_utc"] for row in evidence.get(FIELD, ()) if "date_after_utc" in row), default=None
    )


def dating_due(due_stamp, observation_stamp):
    return bool(due_stamp and observation_stamp and instant(observation_stamp) >= instant(due_stamp))


def _available_at(session):
    _, close = regular_market_hours(session)
    local_close = datetime.combine(date.fromisoformat(session), time.fromisoformat(close), NEW_YORK)
    return (local_close + timedelta(minutes=15)).astimezone(timezone.utc)


def _observations(lookups, catalogue_stamp, asset_id):
    for lookup in lookups:
        asset = lookup.get("asset") or {}
        if (
            asset.get("id") != asset_id
            or asset.get("class") != "us_equity"
            or not asset.get("symbol")
            or asset.get("exchange") in (None, "", "UNKNOWN", "UNSPECIFIED")
            or (lookup.get("outcome"), lookup.get("http_status")) != ("found", 200)
            or lookup.get("catalogue_conflict")
            or (lookup.get("lookup_kind"), lookup.get("lookup"))
            not in (("symbol", asset["symbol"]), ("asset_id", asset_id))
        ):
            continue
        stamp = lookup.get(
            "observed_at_utc",
            lookup.get(
                "catalogue_observed_at_utc",
                catalogue_stamp if lookup.get("source") == "asset_catalogue" else None,
            ),
        )
        if stamp:
            yield asset["exchange"], asset["symbol"], stamp


def _latest(observations):
    dated = []
    for exchange, symbol, stamp in observations:
        if not isinstance(stamp, str) or len(stamp) <= 10:
            continue
        try:
            dated.append((instant(stamp), exchange, symbol, stamp))
        except ValueError:
            continue
    if not dated:
        return None
    latest = max(dated)
    if any(row[0] == latest[0] and row[1:3] != latest[1:3] for row in dated):
        return None
    return latest


def _point_sample(sample, symbol):
    return (
        sample
        and sample.get("symbol") == symbol
        and sample.get("returned_symbol", symbol) == symbol
        and sample.get("session")
        and sample.get("asof") == "-"
        and sample.get("feed") == "sip"
        and not sample.get("through_session")
        and (
            sample.get("outcome") in ("observed", "no_trades")
            and sample.get("http_status") == 200
            or sample.get("outcome") == "unavailable"
            and sample.get("http_status") in (403, 404)
        )
    )


def _saved_samples(change, reports):
    symbol = change["from_symbol"]
    saved = {}
    for report in reports:
        for event in report.get(FIELD, ()):
            if event["asset_id"] == change["asset_id"]:
                for sample in event.get("tape_samples", ()):
                    if _point_sample(sample, symbol):
                        saved[sample["session"]] = sample
        sample = report.get("historical_trade_tape")
        if _point_sample(sample, symbol):
            saved[sample["session"]] = sample
    return saved


def _date_change(change, before, after, client, reports):
    old_tape = _LISTING_TAPES.get(change["from_exchange"])
    new_tape = _LISTING_TAPES.get(change["to_exchange"])
    symbol = change["from_symbol"]
    if not old_tape or not new_tape or symbol != change["to_symbol"]:
        return
    first = before.astimezone(NEW_YORK).date()
    last = after.astimezone(NEW_YORK).date()
    start = max(first - timedelta(days=7), last - timedelta(days=30))
    sessions = market_sessions_between(start.isoformat(), last.isoformat())
    saved = _saved_samples(change, reports)
    if sessions and sessions[-1] == last.isoformat() and after < _available_at(sessions[-1]):
        # An already observed print can date today's tape. Never request the
        # same unfinished day before and after enough time has elapsed to trade.
        today = saved.get(sessions[-1]) or {}
        if today.get("outcome") != "observed" or instant(today["trade"]["t"]) > after:
            sessions = sessions[:-1]
    # One preceding session can bracket a same-day observation or a weekend.
    start_index = max(0, bisect_left(sessions, first.isoformat()) - 1, len(sessions) - _MAX_SESSIONS)
    sessions = sessions[start_index:]
    if not sessions:
        return

    samples = {}

    def tape(index):
        session = sessions[index]
        sample = saved.get(session)
        if sample is None:
            sample = client.get_trade_tape_sample(symbol, session)
        if _point_sample(sample, symbol) and sample["session"] == session:
            samples[session] = sample
            if sample["outcome"] == "observed":
                trade = sample["trade"]
                if instant(trade["t"]) <= after:
                    return trade.get("z")
        return None

    # Updates usually straddle consecutive sessions. Probe those first, then
    # bisect the bounded interval only if its endpoints actually have both tapes.
    high = len(sessions) - 1
    upper_tape = tape(high)
    if upper_tape == old_tape:
        next_day = next_market_session_after(sessions[high])
        if next_day and after < (due := _available_at(next_day)):
            change.update(
                date_session=next_day, date_prior_session=sessions[high], date_after_utc=due.isoformat()
            )
    if upper_tape == new_tape:
        low = high - 1
        lower_tape = tape(low) if low >= 0 else None
        if lower_tape == new_tape and low > 0:
            high, low = low, 0
            lower_tape = tape(low)
        if lower_tape == old_tape:
            while high - low > 1:
                middle = (high + low) // 2
                observed = tape(middle)
                if observed == new_tape:
                    high = middle
                elif observed == old_tape:
                    low = middle
                else:
                    break
            if high - low == 1 and instant(samples[sessions[high]]["trade"]["t"]) > before:
                change.update(prior_session=sessions[low], effective_session=sessions[high])
    if samples:
        change["tape_samples"] = [samples[session] for session in sorted(samples)]


def _competing_label(lookups, symbol, owner):
    return any(
        lookup.get("lookup_kind") == "symbol"
        and lookup.get("lookup") == symbol
        and (lookup.get("asset") or {}).get("id") not in (None, owner)
        for lookup in lookups
    )


def _finish_pending(event, evidence, prior, client, observe_new_tapes):
    owner, symbol = event["asset_id"], event["to_symbol"]
    lookups = evidence["asset_lookups"]
    stamp = evidence.get("catalogue_observed_at_utc")
    current = _latest(_observations(lookups, stamp, owner))
    competitor = _competing_label(lookups, symbol, owner)
    applicable = current and current[1:3] == (event["to_exchange"], symbol) and not competitor
    due = dating_due(event["date_after_utc"], stamp) or bool(
        current and dating_due(event["date_after_utc"], current[3])
    )
    if not due and not competitor and (not current or applicable):
        return event
    result = {key: value for key, value in event.items() if key not in _SCHEDULE_FIELDS}
    if applicable and dating_due(event["date_after_utc"], current[3]) and observe_new_tapes:
        session = event["date_session"]
        saved = _saved_samples(event, (prior, evidence))
        sample = saved.get(session)
        if sample is None:
            sample = client.get_trade_tape_sample(symbol, session)
        result["dating_observed_at_utc"] = current[3]
        if _point_sample(sample, symbol) and sample["session"] == session:
            result["tape_samples"] = [*result.get("tape_samples", ()), sample]
            trade = sample.get("trade") or {}
            if (
                sample["outcome"] == "observed"
                and trade.get("z") == _LISTING_TAPES[event["to_exchange"]]
                and instant(event["from_observed_at_utc"]) < instant(trade["t"]) <= current[0]
            ):
                result.update(prior_session=event["date_prior_session"], effective_session=session)
    return result


def record_exchange_changes(evidence, previous, client, *, observe_new_tapes=True):
    """Carry accepted changes. Compare dated positive asset facts only for this source's prior owner."""
    if not previous:
        return
    prior = previous["evidence"]
    history = evidence.get(FIELD, prior.get(FIELD, ()))
    if history:
        if any("date_after_utc" in item for item in history):
            history = [
                _finish_pending(item, evidence, prior, client, observe_new_tapes)
                if "date_after_utc" in item
                else item
                for item in history
            ]
        evidence[FIELD] = history
    symbol = previous.get("symbol") or prior.get("query", {}).get("input_symbol")
    if not symbol or not prior.get("asset_lookups"):
        return
    anchor = last_observed_symbol_lookup(previous if "symbol" in previous else {**previous, "symbol": symbol})
    if not anchor or not (asset_id := (anchor.get("asset") or {}).get("id")):
        return
    old = tuple(_observations(prior["asset_lookups"], prior.get("catalogue_observed_at_utc"), asset_id))
    fallback = tuple(_observations((anchor,), None, asset_id))
    current = tuple(
        _observations(evidence["asset_lookups"], evidence.get("catalogue_observed_at_utc"), asset_id)
    )
    if not current or not (old or fallback):
        return
    if len({row[0] for row in (*current, *old, *fallback)}) == 1:
        return
    before = _latest((*old, *fallback))
    after = _latest(current)
    if not before or not after or before[1] == after[1] or before[0] >= after[0]:
        return
    change = {
        "asset_id": asset_id,
        "from_exchange": before[1],
        "from_symbol": before[2],
        "from_observed_at_utc": before[3],
        "to_exchange": after[1],
        "to_symbol": after[2],
        "to_observed_at_utc": after[3],
    }
    if any(all(item.get(key) == value for key, value in change.items()) for item in history):
        return
    if observe_new_tapes and not _competing_label(evidence["asset_lookups"], after[2], asset_id):
        _date_change(change, before[0], after[0], client, (prior, evidence))
    evidence[FIELD] = [*history, change]
