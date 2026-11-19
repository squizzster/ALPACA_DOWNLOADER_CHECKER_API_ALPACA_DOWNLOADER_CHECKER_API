"""Select a dated former ticker from an actual displaced literal-price occupation."""

from datetime import date, timedelta
from decimal import Decimal

from .dated_rename import _matches, provider_rows


def _positive(bar):
    return min(Decimal(bar["volume"]), Decimal(bar["trade_count"])) > 0


def assess(ticker, first, pivot, prior_day, end, candidates, load, source, peer_occupied, client, get, put):
    def stage(key, make):
        old = get(key)
        return put(key, make()) if old is None else old

    def unsupported(reason):
        return {"outcome": "unsupported", "reason": reason}

    if not candidates:
        return unsupported("no completed earlier literal occupation ends on the preceding session")

    def market(lo, hi):
        return stage(f"literal:{ticker}:{lo}:{hi}", lambda: provider_rows(client, ticker, "-", "raw", lo, hi))

    def owned(lo, hi):
        return stage(f"owned:{lo}:{hi}", lambda: load(lo, hi))

    def successor():
        held = owned(pivot, end)
        first_two = [b for b in held if _positive(b)][:2]
        if (
            len(first_two) != 2
            or first_two[0]["session"] != pivot
            or any(b["tickers"] != [ticker] for b in held if b["session"] <= first_two[-1]["session"])
        ):
            return unsupported("two traded successor observations are required")
        actual = market(pivot, first_two[-1]["session"])
        if any(b["session"] not in actual or not _matches(b, actual[b["session"]]) for b in first_two):
            return unsupported("literal successor trades do not match the selected owner")
        return {"end": first_two[-1]["session"]}

    later = stage("successor", successor)
    if "outcome" in later:
        return later

    # Keep only a continuous traded/observed overlap with the already selected
    # identity. A donor can describe several older occupations of one spelling.
    def compare(token):
        desc, read = source(token)
        former = desc["symbol"]
        if former == ticker:
            return {}
        lo = max(first, desc["prior_first_trade"])
        if lo > prior_day:
            return {}
        raw = owned(lo, prior_day)
        if not raw or raw[-1]["session"] != prior_day or not _positive(raw[-1]):
            return {}
        actual = market(lo, prior_day)
        prior = read()
        expected = raw
        seen = []
        before = None
        for index in range(len(expected) - 1, -1, -1):
            b = expected[index]
            d = b["session"]
            other = prior.get(d)
            anchored_carry = (
                other is None
                and index > 0
                and not _positive(b)
                and Decimal(b["volume"]) == Decimal(b["trade_count"]) == Decimal(b["vwap"]) == 0
                and all(
                    Decimal(b[k]) == Decimal(expected[index - 1]["close"])
                    for k in ("open", "high", "low", "close")
                )
            )
            if (
                b["tickers"] != [ticker]
                or d in actual
                and _positive(actual[d])
                or not (anchored_carry or other is not None and _matches(b, other))
            ):
                before = d
                break
            seen.append(b)
        if len([b for b in seen if _positive(b)]) < 3:
            return {}
        start = seen[-1]["session"]
        if any(start <= day <= prior_day and _positive(bar) for day, bar in actual.items()):
            return {}
        if {d for d, r in prior.items() if start <= d <= prior_day and _positive(r)} != {
            b["session"] for b in seen if _positive(b)
        }:
            return {}
        if before is None and start > first:
            previous_end = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
            earlier = owned(
                max(first, (date.fromisoformat(start) - timedelta(days=14)).isoformat()), previous_end
            )
            if not earlier:
                return {}
            before = earlier[-1]["session"]
        return {
            "token": token,
            "former": former,
            "locator": desc["locator"],
            "public_id": desc["public_id"],
            "first": start,
            "prior_session": before or (date.fromisoformat(start) - timedelta(days=1)).isoformat(),
        }

    matches = [
        candidate
        for token in sorted(candidates)
        if (candidate := stage("match:" + token, lambda: compare(token)))
    ]
    if len(matches) != 1:
        return unsupported("the recorded occupations do not select a unique matching earlier ticker")
    match = matches[0]
    if peer_occupied(match["former"], match["first"], prior_day):
        return unsupported(
            "an independent online identity occupies the earlier ticker during the proposed interval"
        )
    return dict(
        outcome="projected",
        basis_token=match["token"],
        occupation={"locator": match["locator"], "public_id": match["public_id"]},
        witness_end=later["end"],
        change=dict(
            old=ticker,
            new=match["former"],
            first=match["first"],
            prior_session=match["prior_session"],
            last=prior_day,
            next_session=pivot,
        ),
    )
