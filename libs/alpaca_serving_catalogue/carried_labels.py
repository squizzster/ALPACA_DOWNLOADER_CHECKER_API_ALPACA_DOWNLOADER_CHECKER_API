"""Apply the selected literal ticker handover to published date projections."""


def project_ranges(ranges, changes):
    """Transfer the end of the old dated range to the immediately following label."""
    for c in changes:
        transferred = []
        retained = []
        for r in ranges:
            lo, hi = r["first_session"], r["last_session"]
            if r["ticker"] != c["old"] or hi < c["first"] or lo > c["last"]:
                retained.append(r)
                continue
            if lo < c["first"]:
                retained.append({**r, "last_session": c["prior_session"]})
            transferred.append(
                dict(ticker=c["new"], first_session=max(lo, c["first"]), last_session=min(hi, c["last"]))
            )
            if hi > c["last"]:
                retained.append({**r, "first_session": c["next_session"]})
        if transferred:
            following = next(
                (r for r in retained if r["ticker"] == c["new"] and r["first_session"] == c["next_session"]),
                None,
            )
            if following:
                retained.remove(following)
                transferred[-1]["last_session"] = following["last_session"]
            ranges = sorted(retained + transferred, key=lambda r: (r["ticker"], r["first_session"]))
    return ranges


def project_ownership(intervals, changes, *, carry_cusip=True):
    """Keep each existing CUSIP and its bounds while transferring the observed ticker."""
    for c in changes:
        result = []
        for row in intervals:
            lo, hi = row["start_inclusive"], row["end_exclusive"]
            if row["ticker"] != c["old"] or (hi and hi <= c["first"]) or (lo and lo >= c["next_session"]):
                result.append(row)
                continue
            if lo is None or lo < c["first"]:
                result.append({**row, "end_exclusive": c["first"]})
            result.append(
                {
                    **row,
                    "ticker": c["new"],
                    **({} if carry_cusip else {"cusip": None}),
                    "start_inclusive": max(lo or c["first"], c["first"]),
                    "end_exclusive": min(hi or c["next_session"], c["next_session"]),
                }
            )
            if hi is None or hi > c["next_session"]:
                result.append({**row, "start_inclusive": c["next_session"]})
        intervals = list({tuple(sorted(r.items())): r for r in result}.values())
    return intervals


def add_witnessed_occupation(intervals, change):
    """Fill the former ticker's confirmed dates without extending or inventing a CUSIP."""
    first, last = change["first"], change["next_session"]
    known = sorted(
        (max(first, r["start_inclusive"] or first), min(last, r["end_exclusive"] or last))
        for r in intervals
        if r["ticker"] == change["new"]
        and (not r["end_exclusive"] or r["end_exclusive"] > first)
        and (not r["start_inclusive"] or r["start_inclusive"] < last)
    )
    result = list(intervals)
    cursor = first
    for lo, hi in [*known, (last, last)]:
        if cursor < lo:
            result.append(dict(ticker=change["new"], start_inclusive=cursor, end_exclusive=lo, cusip=None))
        cursor = max(cursor, hi)
    return result
