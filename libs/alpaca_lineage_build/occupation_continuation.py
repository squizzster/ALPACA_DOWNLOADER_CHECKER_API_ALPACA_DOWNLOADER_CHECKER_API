"""Apply a completed unit retirement to fresh assignment rows from its existing owner."""

from bisect import bisect_right

from libs.alpaca_security_lineage.inference_evidence import provider_actions

from .contract import RecoveryRequired


def terms(evidence, item):
    """Capture only the dated provider inputs which identify this terminal unit."""
    action = item["terminal_action"]
    ids = {action["id"], *item["component_action_ids"]}
    symbols = {action[k] for k in ("old_symbol", "new_symbol", "alternate_symbol")}
    cusips = {action[k] for k in ("old_cusip", "new_cusip", "alternate_cusip")}
    day = action["effective_date"]
    selected = {}
    for event in provider_actions(evidence):
        identifier = event.get("id")
        if identifier in ids or (
            event.get("action_type") in {"unit_split", "stock_merger", "cash_merger"}
            and event.get("effective_date") == day
            and (
                any(event.get(k) in symbols for k in ("old_symbol", "acquiree_symbol"))
                or any(event.get(k) in cusips for k in ("old_cusip", "acquiree_cusip"))
            )
        ):
            selected[identifier] = event
    endpoints = sorted(
        {
            (lookup.get("asset") or {}).get("id") or "not_found"
            for lookup in evidence.get("asset_lookups", ())
            if lookup.get("lookup") == action["old_cusip"]
        }
    )
    return {"actions": selected, "old_cusip_endpoints": endpoints}


def apply(previous, evidence, view, report, indexes, *, continued):
    records = previous.get("dated_occupations", {})
    terminal = {pid: desc for pid, desc in records.items() if desc.get("retained_side") == "prior"}
    if not terminal:
        return report
    for item in terminal.values():
        if (
            evidence is not previous["evidence"]
            and terms(evidence, item) != item["terminal_evidence"][view.symbol]
        ):
            raise RecoveryRequired("unit retirement evidence changed; selected occupation recovery required")
        if view.symbol != item["symbol"]:
            continue
        if continued:
            changed = view.correction_from
        elif view.source_sha256 != previous["source_sha256"]:
            ancestors = {r["source_sha256"]: r["changed_from"] for r in view.source.get("history_changes", ())}
            changed = ancestors.get(previous["source_sha256"], "")
        else:
            changed = None
        if changed is not None and changed <= item["current_asof"]:
            raise RecoveryRequired(
                "unit occupation pricing history changed; selected occupation recovery required"
            )
    selected = []
    rejected = []
    removed = 0
    route_ids = set()
    for seg in report["correlated"]["segments"]:
        pid = seg["security_identity"]["security_id"]
        item = terminal.get(pid)
        if item is None:
            selected.append(seg)
            continue
        dates = indexes[seg["adjustment"]].sessions
        lo, hi = dates[seg["row_start"] - 1], dates[seg["row_end"] - 1]
        if hi <= item["last_prior_trade"]:
            selected.append(seg)
            continue
        if seg["historical_symbol"] != item["symbol"]:
            raise RecoveryRequired("changed dated unit assignment needs selected occupation recovery")
        if lo <= item["last_prior_trade"]:
            pivot = bisect_right(dates, item["last_prior_trade"], seg["row_start"] - 1, seg["row_end"])
            rows = view.views[seg["adjustment"]]
            # A full materialization may include both the accepted unit and its
            # later unowned ticker. Split the already selected rows in memory.
            selected.append(
                {
                    **seg,
                    "row_end": pivot,
                    "row_count": pivot - seg["row_start"] + 1,
                    "last_timestamp": rows[pivot - 1][0],
                    "last_session_date": dates[pivot - 1],
                }
            )
            seg = {
                **seg,
                "row_start": pivot + 1,
                "row_count": seg["row_end"] - pivot,
                "first_timestamp": rows[pivot][0],
                "first_session_date": dates[pivot],
            }
        route_ids.add(pid)
        rejected.append(
            {
                "adjustment": seg["adjustment"],
                "row_start": seg["row_start"],
                "row_end": seg["row_end"],
                "row_count": seg["row_count"],
                "first_timestamp": seg["first_timestamp"],
                "last_timestamp": seg["last_timestamp"],
                "correlation_rule": {"order": 1, "name": "provider_dated_trading_occupation"},
                "reason_codes": ["later_unit_occupation_owner_unresolved"],
                "warnings": [],
                "candidate_securities": [],
                "market_data_series_identity": None,
            }
        )
        removed += seg["row_count"]
    if not removed:
        return report
    old = report["uncorrelated"]
    return {
        **report,
        "correlated": {
            **report["correlated"],
            "segments": selected,
            "row_count": report["correlated"]["row_count"] - removed,
        },
        "uncorrelated": {
            **old,
            "row_count": old["row_count"] + removed,
            "segments": [*old["segments"], *rejected],
        },
        "observation_routes": {
            p: r for p, r in report.get("observation_routes", {}).items() if p not in route_ids
        },
    }
