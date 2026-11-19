"""Assemble only the portions of online history selected by the serving producer."""

from datetime import date, timedelta


def spans(first, last, exclusions):
    begin = first
    for low, high in exclusions:
        lo, hi = date.fromisoformat(low), date.fromisoformat(high)
        if hi < begin:
            continue
        if last < lo:
            break
        if begin < lo:
            yield begin, lo - timedelta(days=1)
        begin = hi + timedelta(days=1)
    if begin <= last:
        yield begin, last


def assemble(modes, ranges, read):
    parts = [read(lo, hi) for lo, hi in ranges]
    if len(parts) == 1:
        return parts[0]
    merged = {}
    result = {}
    for mode in modes:
        segments = [p[mode]["bars"] for p in parts]
        key = tuple(id(rows) for rows in segments)
        if key not in merged:
            merged[key] = [r for rows in segments for r in rows]
        result[mode] = {"bars": merged[key], "bar_count": len(merged[key])}
    return result
