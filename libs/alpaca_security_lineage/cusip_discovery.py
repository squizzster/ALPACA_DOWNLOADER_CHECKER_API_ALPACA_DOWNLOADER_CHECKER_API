"""Discover ticker candidates from native, dated corporate-action endpoints."""

from datetime import date, timedelta

from libs.alpaca_data_api_solver import valid_cusip

from .inference_evidence import reject_assertions
from .settings import SYMBOL_PATTERN

# The CUSIP filter can select either participant. Pair only documented fields
# on the same native action leg. A merger counterparty is not a former ticker.
SINGLE_SECURITY = {
    "forward_split",
    "cash_dividend",
    "stock_dividend",
    "redemption",
    "worthless_removal",
    "partial_call",
    "capital_gains_distribution",
    "reorganization",
}
ENDPOINT_ROLES = {
    "unit_split": ("old", "new", "alternate"),
    "name_change": ("old", "new"),
    "spin_off": ("source", "new"),
    "rights_distribution": ("source", "new"),
    "cash_merger": ("acquirer", "acquiree"),
    "stock_merger": ("acquirer", "acquiree"),
    "stock_and_cash_merger": ("acquirer", "acquiree"),
}


def dated_matches(action, cusip):
    kind, day, identifier = action.get("action_type"), action.get("process_date"), action.get("id")
    if not isinstance(identifier, str) or not identifier or not isinstance(day, str):
        return []
    try:
        if date.fromisoformat(day).isoformat() != day:
            return []
    except ValueError:
        return []
    return [
        {
            "symbol": participant[symbol_field],
            "symbol_field": path + symbol_field,
            "cusip_field": path + cusip_field,
            "action_type": kind,
            "action_id": identifier,
            "process_date": day,
        }
        for participant, symbol_field, cusip_field, path in _endpoints(action)
        if participant.get(cusip_field) == cusip
        and isinstance(participant.get(symbol_field), str)
        and SYMBOL_PATTERN.fullmatch(participant[symbol_field])
    ]


def _endpoints(action):
    kind = action.get("action_type")
    if kind in SINGLE_SECURITY:
        yield action, "symbol", "cusip", ""
    if kind == "reverse_split":
        for field in ("old_cusip", "new_cusip"):
            yield action, "symbol", field, ""
    for role in ENDPOINT_ROLES.get(kind, ()):
        yield action, role + "_symbol", role + "_cusip", ""
    if kind == "reorganization" and isinstance(action.get("stock_movements"), list):
        for index, movement in enumerate(action["stock_movements"]):
            if isinstance(movement, dict):
                yield movement, "symbol", "cusip", f"stock_movements[{index}]."


def accept_window(response, cusip, start, end):
    reject_assertions(response, where="CUSIP-filtered provider actions")
    outcome, status, rows = response.get("outcome"), response.get("http_status"), response.get("actions")
    if (outcome, status) not in {("denied", 403), ("complete", 200)} or not isinstance(rows, list):
        raise ValueError("incomplete CUSIP-filtered action response")
    if outcome == "denied" and rows:
        raise ValueError("denied CUSIP action interval cannot supply observations")
    matches = []
    observed_actions = {}
    identities = {}
    unmatched = 0
    for action in rows:
        if not isinstance(action, dict):
            raise ValueError("malformed CUSIP-filtered action")
        if isinstance(action.get("id"), str) and action["id"]:
            key = action.get("action_type"), action["id"]
            if key in identities and identities[key] != action:
                raise ValueError("conflicting native CUSIP action identity")
            identities[key] = action
        selected = dated_matches(action, cusip)
        if selected and not start <= selected[0]["process_date"] <= end:
            raise ValueError("CUSIP-filtered action outside requested dates")
        if not selected:
            unmatched += 1
        else:
            key = action["action_type"], action["id"]
            if key in observed_actions:
                continue
            observed_actions[key] = action
        matches.extend(selected)
    return {
        "start": start,
        "end": end,
        "outcome": outcome,
        "http_status": status,
        "matches": matches,
        "actions": [
            {"action": action, "ticker": single_ticker_action(action)} for action in observed_actions.values()
        ],
        "unmatched_action_count": unmatched,
    }


def single_ticker_action(action):
    """A full native action can supply ticker evidence without importing another party."""
    endpoints = list(_endpoints(action))
    declared = {field for _, symbol, cusip, path in endpoints if not path for field in (symbol, cusip)}
    if any(
        field.endswith(("_symbol", "_cusip")) or field in {"symbol", "cusip"}
        for field in action.keys() - declared
    ):
        return None
    labels = {row[s] for row, s, _, _ in endpoints if isinstance(row.get(s), str) and row[s]}
    return next(iter(labels)) if len(labels) == 1 else None


def missing_intervals(start, end, covered):
    lower, upper = date.fromisoformat(start), date.fromisoformat(end)
    for a, b in sorted(covered):
        first, last = date.fromisoformat(a), date.fromisoformat(b)
        if last < lower:
            continue
        if upper < first:
            break
        if lower < first:
            yield lower.isoformat(), (first - timedelta(days=1)).isoformat()
        if last >= upper:
            return
        lower = last + timedelta(days=1)
    if lower <= upper:
        yield lower.isoformat(), end


def _action_sources(symbol, asset_id, actions, lookups, start, end, match_action):
    sources = {}
    for (kind, cusip), lookup in lookups.items():
        exact = lookup.get("asset") or {}
        if (
            kind != "cusip"
            or not valid_cusip(cusip)
            or lookup.get("outcome") != "found"
            or lookup.get("http_status") != 200
            or exact.get("id") != asset_id
            or exact.get("class") != "us_equity"
        ):
            continue
        matches = [
            match
            for action in actions
            for match in match_action(action, cusip)
            if match["symbol"] == symbol and start <= match["process_date"] <= end
        ]
        if matches:
            sources[cusip] = {
                "cusip": cusip,
                "latest_process_date": max(m["process_date"] for m in matches),
                "matches": matches,
            }
    return sources


def _most_recent_cusips(sources):
    latest = max(item["latest_process_date"] for item in sources.values())
    candidates = {cusip for cusip, item in sources.items() if item["latest_process_date"] == latest}
    if len(candidates) != 2:
        return candidates
    # Only a real named old/new CUSIP transition can order a same-day tie.
    roles = {}
    for cusip in candidates:
        for match in sources[cusip]["matches"]:
            if match["process_date"] == latest and match["action_type"] in {"reverse_split", "name_change"}:
                roles.setdefault((match["action_type"], match["action_id"]), {})[match["cusip_field"]] = cusip
    successors = {
        legs["new_cusip"]
        for legs in roles.values()
        if {legs.get("old_cusip"), legs.get("new_cusip")} == candidates
    }
    return successors if len(successors) == 1 else candidates


def discover(symbol, lookups, actions, start, end, client):
    own = lookups["symbol", symbol]
    asset = own.get("asset") or {}
    if (
        own.get("outcome") != "found"
        or own.get("http_status") != 200
        or asset.get("symbol") != symbol
        or asset.get("class") != "us_equity"
        or not isinstance(asset.get("id"), str)
        or not asset["id"]
        or (asset.get("status") != "inactive" and asset.get("exchange") != "OTC")
    ):
        return set(), None
    match_action = getattr(client, "dated_cusip_matches", dated_matches)
    cusip = asset.get("cusip")
    if valid_cusip(cusip):
        source = {"kind": "asset_snapshot"}
    else:
        sources = _action_sources(symbol, asset["id"], actions, lookups, start, end, match_action)
        if not sources:
            return set(), None
        selected = _most_recent_cusips(sources)
        # Distinct dated CUSIPs can legitimately resolve to a single terminal
        # UUID. Follow its most recent named observation or explicit successor.
        if len(selected) != 1:
            return set(), {
                "symbol": symbol,
                "asset_id": asset["id"],
                "windows": [],
                "source": {
                    "kind": "ambiguous_dated_actions",
                    "candidates": [sources[k] for k in sorted(selected)],
                },
            }
        cusip = next(iter(selected))
        source = {
            "kind": "dated_action_and_exact_cusip",
            "exact_lookup": cusip,
            "matches": sources[cusip]["matches"],
        }
        if len(sources) > 1:
            source["other_dated_cusips"] = [sources[k] for k in sorted(sources) if k != cusip]
    former = set()
    for action in actions:
        for match in match_action(action, cusip):
            if match["symbol"] != symbol:
                former.add(match["symbol"])
            elif match["action_type"] == "name_change" and match["symbol_field"] == "new_symbol":
                old = action.get("old_symbol")
                if isinstance(old, str) and old != symbol and SYMBOL_PATTERN.fullmatch(old):
                    former.add(old)
    binding = {"symbol": symbol, "asset_id": asset["id"], "cusip": cusip}
    known = getattr(client, "known_cusip_labels", None)
    if known is not None:
        former.update(known(binding, start, end) - {symbol})
    acquire_windows = getattr(client, "cusip_discovery_windows", None)
    if acquire_windows is not None:
        windows = acquire_windows(binding, start, end, covered_by_symbol=bool(former))
    elif not former and callable(getattr(client, "get_cusip_actions", None)):
        windows = [accept_window(client.get_cusip_actions(cusip, start, end), cusip, start, end)]
    else:
        windows = []
    named = sorted(former)
    for window in windows:
        former.update(match["symbol"] for match in window["matches"] if start <= match["process_date"] <= end)
    discovery = {**binding, "source": source, "windows": windows}
    if named and not windows:
        discovery["previously_named_tickers"] = named
    return former - {symbol}, discovery
