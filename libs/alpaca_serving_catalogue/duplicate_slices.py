"""Restrict accepted history checkpoints to the selected owner's truthful dates."""

from copy import deepcopy

from alpaca_end_of_day._history_coverage import extend_coverage


def prefix(group, previous, inputs, offsets):
    transfer = getattr(group, "duplicate_source_slices", lambda: {})()
    if not transfer or transfer["source_pricing"] != group.references["pricing"]:
        return {}
    parents = [g for g in previous if g["decision_generation"] == transfer["source_decision"]]
    if not parents:
        return {}
    if len(parents) != 1:
        raise ValueError("ambiguous serving checkpoint for the selected duplicate ownership correction")
    records = {r["public_id"]: r for r in parents[0]["history_validation"]["prefix_records"]}
    rows = {name: {m: v["rows"] for m, v in source["views"].items()} for name, source in inputs.items()}
    result = {}
    for piece in transfer["selection"]:
        pid = piece["source_id"]
        record = deepcopy(records[pid])
        if piece["last"] != record["last_session"]:
            preceding, final = piece["final_raw_sessions"]
            record.update(
                last_session=piece["last"],
                validated_through=piece["last"],
                before_last_raw_bar=preceding,
                before_last_price_reasons=[],
                last_raw_bar=final,
            )
        if piece["first"] != record["first_session"] or "final_raw_sessions" in piece:
            record["first_session"] = piece["first"]
            record["coverage"]["session_count"] = piece["sessions"]
            if "session_dates" in record:
                record["session_dates"] = [
                    d for d in record["session_dates"] if piece["first"] <= d <= piece["last"]
                ]
            record["ticker_ranges"] = [
                {
                    **span,
                    "first_session": max(span["first_session"], piece["first"]),
                    "last_session": min(span["last_session"], piece["last"]),
                }
                for span in record["ticker_ranges"]
                if span["last_session"] >= piece["first"] and span["first_session"] <= piece["last"]
            ]
        contributors = {}
        for name, source in inputs.items():
            for mode, view in source["views"].items():
                if any(run["owner"][0] == pid for run in view["ownership"]):
                    contributors.setdefault(mode, set()).add(name)
        result[pid] = {
            "rows": rows,
            "offsets": offsets,
            "coverage": {m: extend_coverage(names) for m, names in contributors.items()},
            "last_session": record["validated_through"],
            "record": record,
            "refresh_gap_sources": False,
        }
    return result
