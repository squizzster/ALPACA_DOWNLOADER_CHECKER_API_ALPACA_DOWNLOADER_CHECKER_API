"""Documented own-common continuation when an older provider UUID is absent.

The metadata producer has already bound and interpreted each observed capital
repayment. This stage consumes that result. It adds no provider identifiers and
never changes an established row owner or constructs adjustment factors.
"""

from copy import deepcopy

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_security_lineage.event_supplements import RECAPITALIZATION_KIND


def project(inputs, reports, evidence):
    result = dict(reports)
    for name, original in reports.items():
        facts = [
            item for item in evidence[name].get("event_supplements", ()) if item["kind"] == RECAPITALIZATION_KIND
        ]
        if not facts or not original["uncorrelated"]["segments"]:
            continue
        by_asset, templates = {}, {}
        for segment in original["correlated"]["segments"]:
            identity = segment["security_identity"]
            templates[identity["security_id"]] = segment
            for asset in identity["member_alpaca_asset_ids"]:
                by_asset.setdefault(asset, set()).add(identity["security_id"])
        lookups = evidence[name].get("asset_lookups", ())
        own = {
            (item.get("asset") or {}).get("id")
            for item in lookups
            if item.get("lookup_kind") == "symbol" and item["lookup"] == name
        }
        interpreted = {
            item["id"]: item
            for item in evidence[name].get("corporate_actions", ())
            if item.get("identity_interpretation") == RECAPITALIZATION_KIND
        }
        scopes = []
        for item in facts:
            fact = item["facts"]
            if (
                fact["symbol"] != name
                or inputs[name].source["latest_request"]["asof"] < fact["exchange_effective_date"]
                or item["match"]["id"] not in interpreted
            ):
                continue
            endpoint_records = {
                role: [
                    lookup
                    for lookup in lookups
                    if lookup.get("lookup_kind") == "cusip" and lookup["lookup"] == fact[role + "_cusip"]
                ]
                for role in ("old", "new")
            }
            if any(
                not records
                or any(
                    record.get("outcome") not in {"found", "not_found"}
                    or record.get("outcome") == "not_found"
                    and record.get("http_status") != 404
                    for record in records
                )
                for records in endpoint_records.values()
            ):
                continue
            endpoints = {
                role: {(lookup.get("asset") or {}).get("id") for lookup in records}
                for role, records in endpoint_records.items()
            }
            targets = set().union(*(by_asset.get(asset, set()) for asset in endpoints["new"] if asset))
            if len(targets) != 1 or len(own) != 1:
                continue
            pid = next(iter(targets))
            members = set(templates[pid]["security_identity"]["member_alpaca_asset_ids"])
            if (
                not own <= members
                or len(endpoints["old"]) != 1
                or not (endpoints["old"] <= members or endpoints["old"] == {None})
            ):
                continue
            preceding = {
                previous["facts"]["exchange_effective_date"]
                for previous in facts
                if previous["facts"]["new_cusip"] == fact["old_cusip"]
                and previous["facts"]["exchange_effective_date"] < fact["exchange_effective_date"]
                and previous["match"]["id"] in interpreted
            }
            start = fact["predecessor_history_start"]
            if start is None and len(preceding) == 1:
                start = next(iter(preceding))
            if start is not None:
                scopes.append(
                    (
                        item,
                        pid,
                        next(iter(endpoints["old"])),
                        fact["old_cusip"],
                        start,
                        fact["exchange_effective_date"],
                    )
                )
            if endpoints["new"] == own and not any(
                later["facts"]["old_cusip"] == fact["new_cusip"] for later in facts
            ):
                scopes.append(
                    (item, pid, next(iter(own)), fact["new_cusip"], fact["exchange_effective_date"], None)
                )
        if not scopes:
            continue
        report = result[name] = deepcopy(original)
        positive, negative, dates = list(report["correlated"]["segments"]), [], {}
        for segment in original["uncorrelated"]["segments"]:
            rows = inputs[name].views[segment["adjustment"]]
            run = None
            last_key = None
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                stamp = rows[index][0]
                if stamp not in dates:
                    dates[stamp] = timestamp(stamp).astimezone(NEW_YORK).date().isoformat()
                day = dates[stamp]
                matches = [scope for scope in scopes if scope[4] <= day and (scope[5] is None or day < scope[5])]
                if len(matches) == 1 and all(
                    candidate.get("asset_id")
                    in templates[matches[0][1]]["security_identity"]["member_alpaca_asset_ids"]
                    for candidate in segment.get("candidate_securities", ())
                ):
                    item, pid, old_id, cusip, start, end = matches[0]
                    fact = item["facts"]
                    piece = {
                        **templates[pid],
                        "adjustment": segment["adjustment"],
                        "historical_symbol": name,
                        "canonical_security": {
                            "security_id": pid,
                            "alpaca_asset_id": old_id,
                            "cusip": cusip,
                        },
                        "identity_confidence": "documented_own_common_recapitalization",
                        "correlation_method": "sourced_capital_repayment_continuation",
                        "correlation_rule": {"order": None, "name": "sourced_capital_repayment_continuation"},
                        "ticker_ownership_interval": {
                            "historical_symbol": name,
                            "cusip": cusip,
                            "asset_id": old_id,
                            "start_date_inclusive": start,
                            "end_date_exclusive": end,
                            "supporting_corporate_action_ids": [item["match"]["id"]],
                        },
                        "warnings": ["provider_predecessor_uuid_unavailable"] if old_id is None else [],
                        "observation_tickers": [name],
                    }
                    piece.pop("observation_selection", None)
                    key = "correlated", pid, old_id, item["match"]["id"], start, end
                    selected = positive
                else:
                    piece = segment
                    key = "uncorrelated", None
                    selected = negative
                if run is None or last_key != key:
                    run = {
                        **piece,
                        "row_start": index + 1,
                        "row_end": index + 1,
                        "row_count": 1,
                        "first_timestamp": stamp,
                        "last_timestamp": stamp,
                        "from": day,
                        "to": day,
                    }
                    selected.append(run)
                else:
                    run.update(row_end=index + 1, row_count=run["row_count"] + 1, last_timestamp=stamp, to=day)
                last_key = key
        for branch, segments in (("correlated", positive), ("uncorrelated", negative)):
            report[branch] = {
                **report[branch],
                "segments": segments,
                "row_count": sum(s["row_end"] - s["row_start"] + 1 for s in segments),
            }
    return result
