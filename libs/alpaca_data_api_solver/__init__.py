"""Pure Alpaca request planning: JSON download needs -> JSON actions.

No credentials, network, store discovery, or publication. A reason identifies one
acquisition stage; later consistency checks must be submitted after acquisition.
"""

import heapq
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

VERSION = 3
BAR_URL = "https://data.alpaca.markets/v2/stocks/bars"
ASSET_URL = "https://paper-api.alpaca.markets/v2/assets"
ACTION_URL = "https://data.alpaca.markets/v1/corporate-actions"
ADJUSTMENTS = {
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
}
NEW_YORK = ZoneInfo("America/New_York")
PAGE_LIMIT = 10000
CORPORATE_SYMBOL_LIMIT = 1000
ASSET_CATALOGUE_MIN_SYMBOLS = 100


def validate_version():
    """Application startup check; the planning function itself performs no I/O."""
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_data_api_solver_change_log.json"
    entries = json.loads(path.read_bytes())
    if (
        not isinstance(entries, list)
        or len(entries) != VERSION
        or any("version" in entry or not entry.get("modifications") for entry in entries)
    ):
        raise ValueError("API solver changelog/version mismatch")


def instant(value):
    if not isinstance(value, str):
        raise ValueError("range boundaries must be ISO strings")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if len(value) == 10:
        parsed = parsed.replace(tzinfo=NEW_YORK)
    if parsed.tzinfo is None:
        raise ValueError("timestamp boundaries require a timezone")
    return parsed.astimezone(timezone.utc)


def iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _strings(values, name):
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(v, str) or not v.strip() for v in values)
    ):
        raise ValueError(f"{name} must be a nonempty string array")
    return sorted(set(values))


def _params(symbols, start, end, asof, adjustment):
    return {
        "symbols": ",".join(sorted(symbols)),
        "timeframe": "1Day",
        "feed": "sip",
        "currency": "USD",
        "start": iso(start),
        "end": iso(end - timedelta(microseconds=1)),
        "asof": asof,
        "adjustment": adjustment,
        "limit": PAGE_LIMIT,
        "sort": "asc",
    }


def _pages(group):
    # Every calendar day is counted, including holidays: deliberately conservative.
    days = max(
        1, (group["end"].astimezone(NEW_YORK).date() - group["start"].astimezone(NEW_YORK).date()).days + 1
    )
    return max(1, math.ceil(days * len(group["symbols"]) / PAGE_LIMIT))


def _combine(left, right):
    targets = {key: set(symbols) for key, symbols in left["targets"].items()}
    for key, symbols in right["targets"].items():
        targets.setdefault(key, set()).update(symbols)
    return {
        "start": min(left["start"], right["start"]),
        "end": max(left["end"], right["end"]),
        "symbols": left["symbols"] | right["symbols"],
        "targets": targets,
    }


def _merge_nearby(groups, fits):
    """Merge adjacent ranges when the conservative page estimate falls.

    This is a bounded greedy plan, not a promise about unknown provider row counts
    or page filling. Exact compatible requests are already grouped before this.
    """
    groups.sort(key=lambda group: (group["start"], group["end"], sorted(group["symbols"])))
    nodes = {i: group for i, group in enumerate(groups)}
    previous = {i: i - 1 if i else None for i in nodes}
    following = {i: i + 1 if i + 1 < len(nodes) else None for i in nodes}
    candidates = []

    def offer(left, right):
        if left is None or right is None:
            return
        merged = _combine(nodes[left], nodes[right])
        saving = _pages(nodes[left]) + _pages(nodes[right]) - _pages(merged)
        if saving > 0 and fits(merged):
            footprint = (merged["end"] - merged["start"]).total_seconds() * len(merged["symbols"])
            heapq.heappush(candidates, (-saving, footprint, left, right))

    for left, right in following.items():
        offer(left, right)
    index = len(nodes)
    while candidates:
        _, _, left, right = heapq.heappop(candidates)
        if left not in nodes or right not in nodes or following[left] != right:
            continue
        before, after = previous[left], following[right]
        nodes[index] = _combine(nodes.pop(left), nodes.pop(right))
        previous[index], following[index] = before, after
        if before is not None:
            following[before] = index
        if after is not None:
            previous[after] = index
        offer(before, index)
        offer(index, after)
        index += 1
    return sorted(nodes.values(), key=lambda group: (group["start"], group["end"], sorted(group["symbols"])))


def _build_step(document, *, max_symbols, max_url_bytes, merge_ranges):
    """Plan daily bars and exact asset lookups; unknown need kinds fail closed.

    Bars need kind/why/symbols/adjustments/asof/ranges. Each range has inclusive
    start and exclusive end_exclusive; date-only values mean New York midnight.
    Asset needs have kind/why/identifiers. Repeated exact lookups are deduplicated.
    max_symbols is a tested operating cap, not a claimed Alpaca symbol limit.
    """
    if not isinstance(document, dict) or set(document) != {"needs"} or not isinstance(document["needs"], list):
        raise ValueError("expected an object containing a needs array")
    if type(max_symbols) is not int or max_symbols < 1 or type(max_url_bytes) is not int or max_url_bytes < 512:
        raise ValueError("invalid request-size limits")
    buckets, lookups, symbol_lookups, corporate, order = {}, {}, {}, {}, []
    for need_index, need in enumerate(document["needs"]):
        if not isinstance(need, dict) or not isinstance(need.get("why"), str) or not need["why"].strip():
            raise ValueError("each need requires a nonempty why")
        why, kind = need["why"], need.get("kind")
        if kind == "asset_catalogue":
            if set(need) != {"kind", "why"}:
                raise ValueError("asset catalogue need requires only kind and why")
            order.append(("asset_catalogue", (need_index, why)))
            continue
        if kind == "corporate_actions":
            if set(need) != {"kind", "why", "symbols", "start", "end"}:
                raise ValueError("corporate actions require symbols and inclusive start/end dates")
            start, end = need["start"], need["end"]
            if (
                date.fromisoformat(start).isoformat() != start
                or date.fromisoformat(end).isoformat() != end
                or start > end
            ):
                raise ValueError("invalid corporate-action date range")
            key = (why, start, end)
            if key not in corporate:
                corporate[key] = {}
                order.append((kind, key))
            corporate[key][need_index] = set(_strings(need["symbols"], "symbols"))
            continue
        if kind == "asset_lookup":
            if set(need) - {"kind", "why", "identifiers", "lookup_kind"} or not {
                "kind",
                "why",
                "identifiers",
            } <= set(need):
                raise ValueError("invalid asset-lookup need fields")
            if need.get("lookup_kind", "exact") not in {"exact", "symbol", "cusip", "asset_id"}:
                raise ValueError("invalid asset lookup kind")
            if need.get("lookup_kind") == "symbol":
                if why not in symbol_lookups:
                    symbol_lookups[why] = {}
                    order.append(("symbol_lookup", why))
                symbol_lookups[why][need_index] = set(_strings(need["identifiers"], "identifiers"))
                continue
            for identifier in _strings(need["identifiers"], "identifiers"):
                key = (why, identifier)
                if key not in lookups:
                    lookups[key] = []
                    order.append(("asset_lookup", key))
                lookups[key].append(need_index)
            continue
        if kind != "bars" or set(need) != {"kind", "why", "symbols", "adjustments", "asof", "ranges"}:
            raise ValueError("unsupported download need or invalid bar-need fields")
        symbols = _strings(need["symbols"], "symbols")
        if any(
            symbol != symbol.strip().upper()
            or not symbol[0].isalpha()
            or not all(c.isascii() and (c.isalnum() or c in ".-") for c in symbol)
            for symbol in symbols
        ):
            raise ValueError("bars require canonical US-equity ticker labels")
        adjustments = _strings(need["adjustments"], "adjustments")
        if set(adjustments) - ADJUSTMENTS:
            raise ValueError("unsupported adjustment view")
        asof = need["asof"]
        if not isinstance(asof, str) or date.fromisoformat(asof).isoformat() != asof:
            raise ValueError("asof must be YYYY-MM-DD")
        if not isinstance(need["ranges"], list) or not need["ranges"]:
            raise ValueError("ranges must be a nonempty array")
        for range_index, window in enumerate(need["ranges"]):
            if not isinstance(window, dict) or set(window) != {"start", "end_exclusive"}:
                raise ValueError("a range requires start and end_exclusive")
            start, end = instant(window["start"]), instant(window["end_exclusive"])
            if start >= end:
                raise ValueError("range start must precede its exclusive end")
            target = (need_index, range_index, iso(start), iso(end))
            for adjustment in adjustments:
                key = (why, asof, adjustment)
                if key not in buckets:
                    buckets[key] = {}
                    order.append(("bars", key))
                group = buckets[key].setdefault(
                    (start, end), {"start": start, "end": end, "symbols": set(), "targets": {}}
                )
                group["symbols"].update(symbols)
                group["targets"].setdefault(target, set()).update(symbols)

    actions = []
    for kind, key in order:
        if kind == "asset_catalogue":
            need_index, why = key
            actions.append(
                {
                    "strategy": "asset_catalogue",
                    "why": why,
                    "request": {"url": ASSET_URL, "params": {"asset_class": "us_equity"}},
                    "deliver": [{"need": need_index, "all_statuses": True}],
                }
            )
            continue
        if kind == "symbol_lookup":
            targets = symbol_lookups[key]
            identifiers = sorted(set().union(*targets.values()))
            if len(identifiers) >= ASSET_CATALOGUE_MIN_SYMBOLS:
                actions.append(
                    {
                        "strategy": "asset_catalogue",
                        "why": key,
                        "request": {"url": ASSET_URL, "params": {"asset_class": "us_equity"}},
                        "deliver": [
                            {"need": index, "identifiers": sorted(values)} for index, values in targets.items()
                        ],
                    }
                )
            else:
                for identifier in identifiers:
                    actions.append(
                        {
                            "strategy": "asset_lookup",
                            "why": key,
                            "request": {"url": ASSET_URL + "/" + quote(identifier, safe=""), "params": {}},
                            "deliver": [
                                {"need": index, "identifier": identifier}
                                for index, values in targets.items()
                                if identifier in values
                            ],
                        }
                    )
            continue
        if kind == "corporate_actions":
            why, start, end = key
            symbols = sorted(set().union(*corporate[key].values()))
            offset = 0
            while offset < len(symbols):
                chunk = []
                params = {
                    "symbols": "",
                    "start": start,
                    "end": end,
                    "region": "us",
                    "limit": 1000,
                    "data_quality": "complete",
                    "sort": "asc",
                }
                size = len(ACTION_URL) + 1 + len(urlencode(params)) + 2048
                while offset < len(symbols) and len(chunk) < min(max_symbols, CORPORATE_SYMBOL_LIMIT):
                    symbol = symbols[offset]
                    added = len(quote(symbol, safe="")) + (3 if chunk else 0)
                    if size + added > max_url_bytes:
                        break
                    chunk.append(symbol)
                    size += added
                    offset += 1
                if not chunk:
                    raise ValueError("corporate-action symbol exceeds request URL limit")
                params["symbols"] = ",".join(chunk)
                selected = set(chunk)
                actions.append(
                    {
                        "strategy": "corporate_actions_batch",
                        "why": why,
                        "request": {"url": ACTION_URL, "params": params},
                        "pagination": {"parameter": "page_token", "response_field": "next_page_token"},
                        "deliver": [
                            {"need": index, "symbols": sorted(values & selected)}
                            for index, values in corporate[key].items()
                            if values & selected
                        ],
                    }
                )
            continue
        if kind == "asset_lookup":
            why, identifier = key
            actions.append(
                {
                    "strategy": "asset_lookup",
                    "why": why,
                    "request": {"url": ASSET_URL + "/" + quote(identifier, safe=""), "params": {}},
                    "deliver": [{"need": index, "identifier": identifier} for index in lookups[key]],
                }
            )
            continue
        why, asof, adjustment = key

        def fits(group):
            return (
                len(group["symbols"]) <= max_symbols
                and len(BAR_URL)
                + 1
                + len(urlencode(_params(group["symbols"], group["start"], group["end"], asof, adjustment)))
                + 2048
                <= max_url_bytes
            )

        groups = []
        for original in buckets[key].values():
            chunk, size = [], 0
            # Commas are encoded as %2C; reserve space for a continuation token.
            base_size = (
                len(BAR_URL)
                + 1
                + len(urlencode(_params([], original["start"], original["end"], asof, adjustment)))
                + 2048
            )
            for symbol in sorted(original["symbols"]):
                added = len(quote(symbol, safe="")) + (3 if chunk else 0)
                if chunk and (len(chunk) == max_symbols or base_size + size + added > max_url_bytes):
                    subset = set(chunk)
                    groups.append(
                        {
                            **original,
                            "symbols": subset,
                            "targets": {
                                target: symbols & subset
                                for target, symbols in original["targets"].items()
                                if symbols & subset
                            },
                        }
                    )
                    chunk, size, added = [], 0, len(quote(symbol, safe=""))
                if base_size + size + added > max_url_bytes:
                    raise ValueError("one symbol exceeds the configured request URL limit")
                chunk.append(symbol)
                size += added
            if chunk:
                subset = set(chunk)
                groups.append(
                    {
                        **original,
                        "symbols": subset,
                        "targets": {
                            target: symbols & subset
                            for target, symbols in original["targets"].items()
                            if symbols & subset
                        },
                    }
                )
        if merge_ranges:
            groups = _merge_nearby(groups, fits)
        for group in groups:
            deliver = [
                {
                    "need": target[0],
                    "range": target[1],
                    "start": target[2],
                    "end_exclusive": target[3],
                    "symbols": sorted(symbols),
                }
                for target, symbols in sorted(group["targets"].items())
            ]
            actions.append(
                {
                    "strategy": "bars_boundary_batch"
                    if why in ("boundary_check", "acquisition_check")
                    else "bars_single_range"
                    if len(group["symbols"]) == 1
                    else "bars_multi_range",
                    "why": why,
                    "request": {
                        "url": BAR_URL,
                        "params": _params(group["symbols"], group["start"], group["end"], asof, adjustment),
                    },
                    "pagination": {"parameter": "page_token", "response_field": "next_page_token"},
                    "deliver": deliver,
                }
            )
    return {"actions": actions}


def build_plan(document, *, max_symbols=4000, max_url_bytes=60000, merge_ranges=True):
    """Optimize independently within each ordered step; never across steps."""
    if not isinstance(document, dict) or set(document) != {"steps"} or not isinstance(document["steps"], list):
        raise ValueError("expected an ordered steps array")
    return {
        "schema_version": VERSION,
        "steps": [
            _build_step(step, max_symbols=max_symbols, max_url_bytes=max_url_bytes, merge_ranges=merge_ranges)
            for step in document["steps"]
        ],
    }


def split_action(action):
    """Resolve a rejected oversized multi-symbol request into two concrete actions."""
    symbols = action["request"]["params"].get("symbols", "").split(",")
    if len(symbols) < 2:
        raise ValueError("a one-symbol action cannot be split further")
    result = []
    middle = len(symbols) // 2
    for chunk in (symbols[:middle], symbols[middle:]):
        selected = set(chunk)
        targets = [
            {**target, "symbols": sorted(set(target["symbols"]) & selected)}
            for target in action["deliver"]
            if set(target["symbols"]) & selected
        ]
        strategy = action["strategy"]
        if strategy == "bars_multi_range" and len(chunk) == 1:
            strategy = "bars_single_range"
        result.append(
            {
                **action,
                "strategy": strategy,
                "request": {
                    **action["request"],
                    "params": {**action["request"]["params"], "symbols": ",".join(chunk)},
                },
                "deliver": targets,
            }
        )
    return result
