"""Refine dated metadata within a documented continuing public component.

An explicit primary chronology can correct labels and dated identifiers while
preserving source positions and numerical states. Foreign components remain
outside this metadata refinement; no prices establish identity here.
"""

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .errors import SecurityCorrelationError


def refine(inputs, reports, plan):
    from .documented_markets import _allocate

    result, dates = {}, {}
    instants = next(
        (
            value.observation_instant_keys
            for value in inputs.values()
            if hasattr(value, "observation_instant_keys")
        ),
        {},
    )

    def session(stamp):
        if stamp not in dates:
            if stamp not in instants:
                instants[stamp] = timestamp(stamp)
            dates[stamp] = instants[stamp].astimezone(NEW_YORK).date().isoformat()
        return dates[stamp]

    for source, original in reports.items():
        segments = []
        for segment in original["correlated"]["segments"]:
            members = set(segment["security_identity"]["member_alpaca_asset_ids"])
            owners = [
                owner for owner in plan["owners"] if members <= set(owner["identity"]["member_alpaca_asset_ids"])
            ]
            if not owners:
                segments.append(segment)
                continue
            if len({owner["identity"]["security_id"] for owner in owners}) != 1:
                raise SecurityCorrelationError("dated refinement has competing documented public components")
            old_asset = segment["canonical_security"].get("alpaca_asset_id")
            if old_asset is not None and old_asset not in owners[0]["identity"]["member_alpaca_asset_ids"]:
                raise SecurityCorrelationError("foreign dated owner requires owning reconciliation")
            first = segment.get("from") or session(segment["first_timestamp"])
            last = segment.get("to") or session(segment["last_timestamp"])
            periods = [
                (owner, ep)
                for owner in owners
                for ep in owner["episodes"]
                if ep["start_inclusive"] <= last and (ep["end_exclusive"] is None or first < ep["end_exclusive"])
            ]

            def decorate(piece, day):
                candidates = {
                    (owner["identity"]["security_id"], ep["symbol"], ep["cusip"]): (owner, ep)
                    for owner, ep in periods
                    if ep["start_inclusive"] <= day
                    and (ep["end_exclusive"] is None or day < ep["end_exclusive"])
                }
                if not candidates:
                    return piece
                if len(candidates) != 1:
                    raise SecurityCorrelationError("dated refinement has competing primary chronologies")
                owner, episode = next(iter(candidates.values()))
                projected = _allocate(piece, owner, episode, "primary_dated_metadata_refinement")
                if "observation_selection" in piece:
                    projected["observation_selection"] = piece["observation_selection"]
                projected["observation_tickers"] = sorted(
                    {
                        episode["symbol"] if label == piece["historical_symbol"] else label
                        for label in piece.get("observation_tickers", [piece["historical_symbol"]])
                    }
                )
                projected["dated_identity_refinement"] = {
                    "version": 1,
                    "previous_historical_symbol": piece["historical_symbol"],
                    "previous_asset_id": piece["canonical_security"].get("alpaca_asset_id"),
                    "previous_cusip": piece["canonical_security"].get("cusip"),
                    "source_fact_id": owner["source_fact_id"],
                    "numerical_selection_unchanged": True,
                }
                return projected

            cuts = {
                bound
                for _owner, ep in periods
                for bound in (ep["start_inclusive"], ep["end_exclusive"])
                if bound is not None and first < bound <= last
            }
            if not cuts:
                segments.append(decorate(segment, first))
                continue
            rows = inputs[source].views[segment["adjustment"]]
            run, prior_scope = None, None
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                stamp = rows[index][0]
                day = session(stamp)
                scope = tuple(
                    (owner["source_fact_id"], ep["start_inclusive"])
                    for owner, ep in periods
                    if ep["start_inclusive"] <= day
                    and (ep["end_exclusive"] is None or day < ep["end_exclusive"])
                )
                if run is None or scope != prior_scope:
                    run = {
                        **decorate(segment, day),
                        "row_start": index + 1,
                        "row_end": index + 1,
                        "row_count": 1,
                        "from": day,
                        "to": day,
                        "first_timestamp": stamp,
                        "last_timestamp": stamp,
                    }
                    segments.append(run)
                else:
                    run.update(row_end=index + 1, row_count=run["row_count"] + 1, to=day, last_timestamp=stamp)
                prior_scope = scope
        result[source] = {**original, "correlated": {**original["correlated"], "segments": segments}}
        if source in inputs:
            inputs[source].observation_instant_keys = instants
    return result
