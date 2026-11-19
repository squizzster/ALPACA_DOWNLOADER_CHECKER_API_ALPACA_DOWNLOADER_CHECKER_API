"""Assess whether a reported share split is compatible with regular-session price units."""

import json
from contextlib import closing, contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from itertools import pairwise
from uuid import uuid4

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import _positive_rate
from libs.alpaca_lineage_build.metadata import action_symbols
from libs.common.sqlite_connections import connect_sqlite
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage, regular_market_hours

from .contract import ADJUSTMENTS, NEW_YORK, DownloadError, symbol_label, timestamp
from .dated_publication import _pack, _unpack
from .storage import regular_path

RESTRICT = "unresolved_pre_event_adjusted_basis"
RESTORED = "price_basis_resolved"
MODES = ADJUSTMENTS


def window(day, first, last):
    pivot = date.fromisoformat(day)
    start = max(first, pivot - timedelta(days=14))
    end = min(last, pivot + timedelta(days=14))
    if not start < pivot < end:
        return ()
    dates = expected_market_session_coverage(
        start.isoformat(), (end + timedelta(days=1)).isoformat()
    ).session_dates
    if day not in dates:
        return ()
    i = dates.index(day)
    return dates[max(0, i - 5) : i + 6]


def _near(ratio, span):
    return 1 / span <= ratio <= span


def plan(inputs, pivot, recheck):
    dates = inputs["dates"]
    if not dates or len(dates) < 4 or pivot not in dates[1:-1]:
        return {"outcome": "unsupported", "reason": "incomplete exchange-session witness window"}
    views = {mode: inputs["views"][mode]["bars"] for mode in MODES}
    i = dates.index(pivot)
    pair = list(dates[i - 1 : i + 1])
    if any(
        [b["session"] for b in rows] != (list(dates) if mode == "raw" else pair) for mode, rows in views.items()
    ):
        return {"outcome": "unsupported", "reason": "missing published price witnesses"}
    raw = views["raw"]
    previous = dates[i - 1]
    labels = raw[i]["tickers"]
    if len(labels) != 1 or any(b["tickers"] != labels for rows in views.values() for b in rows):
        return {"outcome": "unsupported", "reason": "ambiguous dated price owner or instrument"}
    sym = labels[0]
    context = dict(
        symbol=sym, cut=pivot, previous_session=previous, witness_start=dates[0], witness_end=dates[-1]
    )
    before, after = raw[i - 1 : i + 1]
    if any(min(Decimal(b["volume"]), Decimal(b["trade_count"])) <= 0 for b in (before, after)):
        return {**context, "outcome": "unsupported", "reason": "boundary has a carried or missing trade"}

    def step(mode):
        p, n = views[mode]
        return (Decimal(p["close"]) / Decimal(before["close"])) / (Decimal(n["close"]) / Decimal(after["close"]))

    split, full = step("split"), step("all")
    if all(_near(step(mode), Decimal("1.05")) for mode in MODES if mode != "raw"):
        return {**context, "outcome": "restored" if recheck else "unchanged", "reason": RESTORED}
    actions = [
        a
        for a in inputs["actions"]
        if sym in action_symbols(a)
        and (a.get("ex_date") or a.get("effective_date") or a.get("process_date")) == pivot
    ]
    splits = [
        a
        for a in actions
        if a.get("action_type") in ("forward_split", "reverse_split") and a.get("symbol") == sym
    ]
    if len(splits) != 1 or any(
        a.get("action_type") not in ("forward_split", "reverse_split", "cash_dividend") for a in actions
    ):
        return {
            **context,
            "outcome": "unsupported",
            "reason": "missing or competing corporate-action explanation",
        }
    a = splits[0]
    old, new = (_positive_rate(a.get(k)) for k in ("old_rate", "new_rate"))
    if not old or not new or (old > new) != (a["action_type"] == "reverse_split"):
        return {**context, "outcome": "unsupported", "reason": "unusable reported share terms"}
    factor = old / new
    if (
        max(factor, 1 / factor) < 2
        or not _near(split / factor, Decimal("1.05"))
        or not _near(full / split, Decimal("1.05"))
    ):
        return {
            **context,
            "outcome": "unsupported",
            "reason": "published split-containing views do not isolate the reported terms",
        }
    if any(
        not _near(step(mode), Decimal("1.01")) for mode in ("dividend", "spin-off", "dividend,spin-off")
    ) or any(not _near(step(mode) / factor, Decimal("1.05")) for mode in ("split,dividend", "split,spin-off")):
        return {
            **context,
            "outcome": "unsupported",
            "reason": "an independent adjustment view carries other economic terms",
        }
    traded = [b for b in raw if min(Decimal(b["volume"]), Decimal(b["trade_count"])) > 0]
    if len([b for b in traded if b["session"] >= pivot]) < 3:
        return {**context, "outcome": "unsupported", "reason": "insufficient subsequent market activity"}
    for left, right in pairwise(traded):
        move = Decimal(right["open"]) / Decimal(left["close"])
        if _near(move / factor, Decimal("1.35")):
            if right["session"] == pivot and not recheck:
                return {
                    **context,
                    "outcome": "unchanged",
                    "reason": "opening trade supports reported share units",
                }
            if right["session"] != pivot:
                return {
                    **context,
                    "outcome": "unsupported",
                    "reason": "a nearby traded opening supports the reported share step",
                }
            break
    else:
        theoretical = Decimal(before["close"]) * factor
        if any(
            Decimal(b["low"]) / Decimal("1.25") <= theoretical <= Decimal(b["high"]) * Decimal("1.25")
            for b in traded
            if b["session"] >= pivot
        ):
            return {
                **context,
                "outcome": "unsupported",
                "reason": "a subsequent traded price range supports the reported share units",
            }
        observed = Decimal(after["open"]) / Decimal(before["close"])
        if not _near(observed, Decimal("1.4")):
            return {
                **context,
                "outcome": "unsupported",
                "reason": "opening prices do not distinguish the share units",
            }
    return {
        **context,
        "outcome": "check_regular_trades",
        "factor": str(factor),
        "daily_before": before["close"],
        "daily_after": after["open"],
    }


def regular_minutes(client, symbol, day, which, *, remainder=False):
    if symbol != symbol_label(symbol) or which not in ("close", "open"):
        raise DownloadError("invalid selected regular-session request")
    op, cl = regular_market_hours(day)
    clock = cl if which == "close" else op
    boundary = datetime.combine(date.fromisoformat(day), time.fromisoformat(clock), NEW_YORK)
    first, last = (
        (boundary - timedelta(minutes=10), boundary)
        if which == "close"
        else (boundary, boundary + timedelta(minutes=10))
    )
    if remainder:
        day_date = date.fromisoformat(day)
        if which == "close":
            first, last = datetime.combine(day_date, time.fromisoformat(op), NEW_YORK), first
        else:
            first, last = last, datetime.combine(day_date, time.fromisoformat(cl), NEW_YORK)
    doc = client._get(
        dict(
            symbols=symbol,
            timeframe="1Min",
            feed="sip",
            currency="USD",
            asof="-",
            adjustment="raw",
            start=first.isoformat(),
            end=(last - timedelta(microseconds=1)).isoformat(),
            limit=1000,
        )
    )
    if not isinstance(doc.get("bars"), dict) or set(doc["bars"]) - {symbol} or doc.get("next_page_token"):
        raise DownloadError("incomplete or mismatched literal minute response")
    tape = doc["bars"].get(symbol, [])
    previous = None
    trades = 0
    for b in tape:
        instant = timestamp(b["t"])
        if not first <= instant < last or previous is not None and instant <= previous:
            raise DownloadError("minute response outside its exchange session window")
        values = tuple(_positive_rate(b.get(k)) for k in ("o", "h", "l", "c", "v", "n"))
        if None in values:
            raise DownloadError("invalid literal traded minute")
        op, hi, lo, cl, _, number = values
        if number != int(number) or not (lo <= min(op, cl) <= max(op, cl) <= hi):
            raise DownloadError("inconsistent literal traded minute")
        trades += int(number)
        previous = instant
    if not tape:
        return {}
    return dict(
        price=str(tape[-1]["c"] if which == "close" else tape[0]["o"]),
        minutes=len(tape),
        trades=trades,
    )


def finish(selected, before, after, recheck):
    evidence = {"regular_close": before, "regular_open": after, **selected}
    if any(w.get("minutes", 0) < 2 or w.get("trades", 0) < 3 for w in (before, after)):
        return {**evidence, "outcome": "unsupported", "reason": "insufficient regular-session prints"}
    left, right = Decimal(before["price"]), Decimal(after["price"])
    if not _near(left / Decimal(selected["daily_before"]), Decimal("1.1")) or not _near(
        right / Decimal(selected["daily_after"]), Decimal("1.1")
    ):
        return {
            **evidence,
            "outcome": "unsupported",
            "reason": "regular-session trades disagree with daily endpoints",
        }
    move = right / left
    expected = move / Decimal(selected["factor"])
    if _near(expected, Decimal("1.35")):
        return {**evidence, "outcome": "restored" if recheck else "unchanged", "reason": RESTORED}
    if _near(move, Decimal("1.4")) and not _near(expected, Decimal("1.8")):
        return {**evidence, "outcome": "restricted", "reason": RESTRICT}
    return {**evidence, "outcome": "unsupported", "reason": "competing traded share-unit interpretations"}


@contextmanager
def review_steps(store, binding, *, completed_only=False):
    path = regular_path(store, "end_of_day/pricing/price_basis_reviews.sqlite3")
    key = json.dumps(binding, sort_keys=True)
    for suffix in ("-journal", "-wal", "-shm"):
        regular_path(path.parent, path.name + suffix)
    if completed_only and not path.exists():
        yield None
        return
    with closing(connect_sqlite(path, readonly=completed_only, create=not completed_only)) as db:
        if not completed_only:
            with db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS stages(binding TEXT,step TEXT,payload BLOB NOT NULL,PRIMARY KEY(binding,step))"
                )

        def get(k):
            row = db.execute("SELECT payload FROM stages WHERE binding=? AND step=?", (key, k)).fetchone()
            return _unpack(row[0]) if row else None

        def put(k, v):
            with db:
                db.execute("INSERT INTO stages VALUES(?,?,?)", (key, k, _pack(v)))
            return v

        yield get, put


def review(store, binding, load, client, *, completed_only=False, historical_identity=None):
    with review_steps(store, binding, completed_only=completed_only) as steps:
        if steps is None:
            return None
        get, put = steps
        prior = get("decision")
        if prior is not None or completed_only:
            return prior
        selected = get("plan")
        recheck = binding["recheck"]
        if selected is None:
            if binding.get("product_quote") or binding.get("untraded_split"):
                selected = load()
            elif historical_identity is not None:
                from .historical_split_review import select

                selected = select(load, binding["cut"], recheck, client, historical_identity, get, put)
            else:
                selected = plan(load(), binding["cut"], recheck)
            put("plan", selected)
        result = selected
        if selected["outcome"] == "check_recovered_trades":
            from .untraded_split import confirm_recovery

            result = confirm_recovery(selected, client, get, put)
        if selected["outcome"] == "check_untraded_tape":
            from .untraded_split import confirm

            result = confirm(selected, client, get, put)
        if selected["outcome"] == "check_product_tape":
            from .product_quote import confirm

            result = confirm(selected, client, get, put)
        if selected["outcome"] == "check_regular_trades":
            prices = []
            for side, day in (("close", selected["previous_session"]), ("open", selected["cut"])):
                price = get(side)
                if price is None:
                    price = put(side, regular_minutes(client, selected["symbol"], day, side))
                if price.get("minutes", 0) < 2 or price.get("trades", 0) < 3:
                    # A thin instrument can first trade well after the opening bell.
                    # Fetch only the rest of this exchange session and keep the
                    # endpoint nearest the actual open/close over both blocks.
                    more = get(side + "_remainder")
                    if more is None:
                        more = put(
                            side + "_remainder",
                            regular_minutes(client, selected["symbol"], day, side, remainder=True),
                        )
                    price = {
                        "price": (price or more).get("price"),
                        "minutes": price.get("minutes", 0) + more.get("minutes", 0),
                        "trades": price.get("trades", 0) + more.get("trades", 0),
                    }
                prices.append(price)
            result = finish(selected, *prices, recheck)
        return put(
            "decision",
            {
                **result,
                "public_id": binding["public_id"],
                "source_decision": binding["source_decision"],
                "numerical_token": binding["numerical_token"],
                "token": uuid4().hex,
            },
        )


def selected_historical_route(store, selected):
    """Read the original assessment for a compatible early coverage descriptor."""
    if selected.get("published_symbol"):
        return selected["published_symbol"]
    path = regular_path(store, "end_of_day/pricing/price_basis_reviews.sqlite3", published=True)
    with closing(connect_sqlite(path, readonly=True)) as db:
        rows = db.execute(
            """SELECT payload FROM stages WHERE step='decision'
            AND json_extract(binding,'$.public_id')=? AND json_extract(binding,'$.cut')=?
            AND json_extract(binding,'$.store')=? AND json_extract(binding,'$.source_decision')=?
            AND json_extract(binding,'$.historical_ticker')=1""",
            (selected["public_id"], selected["cut"], str(store), selected["source_decision"]),
        )
        for (packed,) in rows:
            decision = _unpack(packed)
            if decision["token"] == selected["token"] and decision["symbol"] == selected["symbol"]:
                return decision.get("published_symbol")
    return None
