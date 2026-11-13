"""Project sourced marketplace dates onto an already established public owner.

These facts supply trading labels and bounded market authority, not a new UUID
union or a price check. A matching observation can recover a previously withheld
row only when its own source label, observed provider endpoint and an existing
group identity converge. Unrelated existing owners are never overwritten.
"""

from copy import deepcopy
from dataclasses import dataclass

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND, items

from .errors import SecurityCorrelationError as LineageError


class MarketDateProjectionRequired(LineageError):
    """A selected dated split needs the owner's exact stored timestamp grid."""


def _bounds(segment):
    return (
        segment.get("from") or timestamp(segment["first_timestamp"]).astimezone(NEW_YORK).date().isoformat(),
        segment.get("to") or timestamp(segment["last_timestamp"]).astimezone(NEW_YORK).date().isoformat(),
    )


def _whole_episode(segment, episodes):
    first, last = _bounds(segment)
    selected = [
        episode
        for episode in episodes
        if episode["start_inclusive"] <= last
        and (episode["end_exclusive"] is None or first < episode["end_exclusive"])
    ]
    if not selected or selected[0]["start_inclusive"] > first:
        return None
    if selected[-1]["end_exclusive"] is not None and selected[-1]["end_exclusive"] <= last:
        return None
    if len({(item["symbol"], item["market"]) for item in selected}) != 1:
        return None
    # A CUSIP-only episode boundary does not change an accepted owner or market
    # label. Preserve the accepted segment's identifier rather than inventing a
    # row position just to subdivide descriptive provenance.
    return {
        **selected[0],
        "end_exclusive": selected[-1]["end_exclusive"],
        "cusip": selected[0]["cusip"] if len({e["cusip"] for e in selected}) == 1 else None,
    }


def _decorate(segment, fact_id, episode, direct):
    piece = {**segment}
    for field in ("reason_codes", "candidate_securities", "market_data_series_identity"):
        piece.pop(field, None)
    piece["historical_symbol"] = episode["symbol"]
    piece["observation_tickers"] = [episode["symbol"]]
    piece["ticker_ownership_interval"] = {
        **(piece.get("ticker_ownership_interval") or {}),
        "historical_symbol": episode["symbol"],
        "start_date_inclusive": episode["start_inclusive"],
        "end_date_exclusive": episode["end_exclusive"],
        **({"cusip": episode["cusip"]} if episode["cusip"] is not None else {}),
        "supporting_sourced_fact_ids": [fact_id],
    }
    piece["observation_authority"] = {
        "version": 1,
        "source_fact_id": fact_id,
        "market": episode["market"],
        "direct_market_source": direct,
        "from": episode["start_inclusive"],
        "to": episode["end_exclusive"],
    }
    return piece


@dataclass(frozen=True)
class MarketPlan:
    bound: tuple
    cusips: dict
    templates: dict
    adjustment_bridges: tuple = ()


def refines_adjustment_basis(before, after):
    """A new adjustment assertion leaves the completed market dates untouched."""
    if before["facts"]["schema_version"] != 1 or after["facts"]["schema_version"] != 2:
        return False
    return before == {
        **after,
        "facts": {key: value for key, value in after["facts"].items() if key != "adjustment_basis"}
        | {"schema_version": 1},
    }


def adjustment_bridges(bound, facts, evidence):
    """Select new cross-market adjustment evidence from accepted action facts."""
    from libs.alpaca_security_lineage.inference_evidence import reject_assertions

    reject_assertions(facts, where="retired cross-market assertion facts")
    reject_assertions(evidence, where="retired cross-market assertion evidence")
    subjects = {identifier: pid for identifier, pid, _episodes in bound}
    result = []
    for identifier, item in facts.items():
        value = item["facts"]
        if value["schema_version"] != 2:
            continue
        for left, right in zip(value["episodes"], value["episodes"][1:]):
            if left["market"] != "when_issued" or right["market"] != "regular_way":
                continue
            events = {
                action["id"]: action["ex_date"]
                for source in evidence
                if source.get("query", {}).get("input_symbol") == right["symbol"]
                for action in source.get("corporate_actions", ())
                if action.get("action_type") == "cash_dividend"
                and action.get("symbol") == right["symbol"]
                and action.get("cusip") == right["cusip"]
                and action.get("ex_date")
                and action["ex_date"] >= left["end_exclusive"]
            }
            if events:
                result.append(
                    {
                        "source_fact_id": identifier,
                        "public_id": subjects[identifier],
                        "temporary_source": left["symbol"],
                        "continuing_source": right["symbol"],
                        "cusip": right["cusip"],
                        "from": left["start_inclusive"],
                        "to": left["end_exclusive"],
                        "post_market_dividends": events,
                    }
                )
    return tuple(result)


def bind(reports, evidence, auxiliary=None, *, inactive_memberships=None):
    """Resolve submitted market facts against accepted group identities once."""
    from libs.alpaca_security_lineage.inference_evidence import reject_assertions

    reject_assertions(evidence, where="retired researched plan")
    reject_assertions(auxiliary, where="retired auxiliary assertion evidence")
    facts = {}
    evidence = tuple(evidence.values()) + tuple((auxiliary or {}).values())
    for value in evidence:
        for item in items(value, MARKET_KIND):
            if item["facts"]["schema_version"] < 3:
                facts.setdefault(item["match"]["source_id"], item)
    if not facts:
        return MarketPlan((), {}, {})
    identities, templates, by_asset, cusips = {}, {}, {}, {}
    for report in reports.values():
        for segment in report["correlated"]["segments"]:
            identity = segment["security_identity"]
            pid = identity["security_id"]
            identities[pid] = identity
            templates.setdefault(pid, segment)
            for asset in identity["member_alpaca_asset_ids"]:
                by_asset.setdefault(asset, set()).add(pid)
    for value in evidence:
        for lookup in value.get("asset_lookups", ()):
            asset = lookup.get("asset") or {}
            if lookup.get("lookup_kind") == "cusip" and asset.get("id"):
                cusips.setdefault(lookup["lookup"], set()).add(asset["id"])
    bound = []
    for identifier, item in facts.items():
        subject = item["facts"]["successor"]["cusip"]
        assets = cusips.get(subject, set())
        targets = set().union(*(by_asset.get(asset, set()) for asset in assets))
        episode_sources = {episode["symbol"] for episode in item["facts"]["episodes"]}
        if (
            not targets
            and inactive_memberships is not None
            and episode_sources <= inactive_memberships.keys()
            and all(reports.get(name, {}).get("source_row_count") == 0 for name in episode_sources)
        ):
            # Every source for this already completed market has no new rows.
            # Its accepted public membership remains authority even though the
            # suffix reports contain no segments. This cannot allocate a new
            # unresolved row or supply an invented historical row template.
            targets = {
                identity["security_id"]
                for name in episode_sources
                for asset in assets
                if (identity := inactive_memberships[name].get(asset)) is not None
            }
        if len(assets) != 1 or len(targets) != 1:
            raise LineageError(f"{identifier}: market occupancy lacks one observed public-identity binding")
        bound.append((identifier, next(iter(targets)), item["facts"]["episodes"]))
    return MarketPlan(tuple(bound), cusips, templates, adjustment_bridges(bound, facts, evidence))


def project(
    inputs,
    reports,
    evidence,
    auxiliary=None,
    *,
    metadata_only=False,
    plan=None,
    source_names=None,
    recover_unassigned=True,
    allow_adjustment_continuity=False,
    inactive_memberships=None,
):
    from libs.alpaca_security_lineage.inference_evidence import reject_assertions

    reject_assertions(evidence, where="retired assertion projector")
    reject_assertions(auxiliary, where="retired auxiliary assertion evidence")
    if plan is not None:
        raise ValueError("researched ownership plans are inadmissible for new inference")
    plan = (
        bind(reports, evidence, auxiliary, inactive_memberships=inactive_memberships) if plan is None else plan
    )
    bound, cusips, templates = plan.bound, plan.cusips, plan.templates
    if not bound:
        return reports
    evidence = tuple(evidence.values()) + tuple((auxiliary or {}).values())
    source_names = source_names or {}
    result = dict(reports)
    dates = {}
    targets = {pid for _, pid, _ in bound}
    market_symbols = {episode["symbol"] for _, _, episodes in bound for episode in episodes}
    for name, original in reports.items():
        source_name = source_names.get(name, name)
        if not any(
            segment["security_identity"]["security_id"] in targets
            for segment in original["correlated"]["segments"]
        ) and not (
            recover_unassigned and source_name in market_symbols and original["uncorrelated"]["segments"]
        ):
            continue
        report = result[name] = deepcopy(original)
        branches = {branch: [] for branch in ("correlated", "uncorrelated")}
        source_evidence = next(
            (value for value in evidence if value.get("query", {}).get("input_symbol") == source_name), {}
        )
        own_lookups = [
            lookup.get("asset")
            for lookup in source_evidence.get("asset_lookups", ())
            if lookup.get("lookup_kind") == "symbol"
            and lookup.get("lookup") == source_name
            and lookup.get("asset")
        ]
        for branch in branches:
            for segment in original[branch]["segments"]:
                owner = (segment.get("security_identity") or {}).get("security_id")
                if (
                    owner is not None
                    and owner not in targets
                    or owner is None
                    and (not recover_unassigned or source_name not in market_symbols)
                ):
                    branches[branch].append(segment)
                    continue
                relevant = [(fact_id, pid, episodes) for fact_id, pid, episodes in bound if pid == owner]
                first_day, last_day = _bounds(segment)
                whole = [
                    (fact_id, episode)
                    for fact_id, _pid, episodes in relevant
                    if (episode := _whole_episode(segment, episodes)) is not None
                ]
                if len(whole) == 1:
                    fact_id, episode = whole[0]
                    asset_ids = cusips.get(episode["cusip"], set())
                    direct = (
                        source_name == episode["symbol"]
                        and len(asset_ids) == 1
                        and all(asset["id"] in asset_ids for asset in own_lookups)
                    )
                    branches[branch].append(_decorate(segment, fact_id, episode, direct))
                    continue
                if owner is not None and not any(
                    episode["start_inclusive"] <= last_day
                    and (episode["end_exclusive"] is None or first_day < episode["end_exclusive"])
                    for _fact_id, _pid, episodes in relevant
                    for episode in episodes
                ):
                    branches[branch].append(segment)
                    continue
                if metadata_only:
                    raise MarketDateProjectionRequired(
                        f"{name}/{segment['adjustment']}: dated market split requires exact timestamps "
                        f"at positions {segment['row_start']}..{segment['row_end']}"
                    )
                rows = inputs[name].views[segment["adjustment"]]
                run, last_key = None, None
                for index in range(segment["row_start"] - 1, segment["row_end"]):
                    key = id(rows), index
                    if key not in dates:
                        dates[key] = timestamp(rows[index][0]).astimezone(NEW_YORK).date().isoformat()
                    day = dates[key]
                    matches = [
                        (fact_id, pid, episode)
                        for fact_id, pid, episodes in bound
                        if owner is None or owner == pid
                        for episode in episodes
                        if episode["start_inclusive"] <= day
                        and (episode["end_exclusive"] is None or day < episode["end_exclusive"])
                        and (owner is not None or source_name == episode["symbol"])
                    ]
                    if len(matches) > 1:
                        raise LineageError(f"{name}: conflicting sourced market occupancy on {day}")
                    if not matches:
                        selected_branch, piece = branch, segment
                        grouping = branch, None
                    else:
                        fact_id, pid, episode = matches[0]
                        asset_ids = cusips.get(episode["cusip"], set())
                        direct = (
                            source_name == episode["symbol"]
                            and len(asset_ids) == 1
                            and all(asset["id"] in asset_ids for asset in own_lookups)
                        )
                        if owner is None and not direct:
                            selected_branch, piece, grouping = branch, segment, (branch, None)
                        else:
                            selected_branch = "correlated"
                            piece = _decorate(
                                templates[pid] if owner is None else segment, fact_id, episode, direct
                            )
                            if owner is None:
                                piece.pop("observation_selection", None)
                                observed = next(iter(asset_ids))
                                if observed in piece["security_identity"]["member_alpaca_asset_ids"]:
                                    piece["canonical_security"] = {
                                        **piece["canonical_security"],
                                        "alpaca_asset_id": observed,
                                        "cusip": episode["cusip"],
                                    }
                                piece["identity_confidence"] = "documented_market_occupancy"
                                piece["correlation_method"] = "sourced_dated_market_occupancy"
                            grouping = (
                                selected_branch,
                                fact_id,
                                pid,
                                episode["symbol"],
                                direct,
                                episode["start_inclusive"],
                            )
                    if run is None or grouping != last_key:
                        run = {
                            **piece,
                            "adjustment": segment["adjustment"],
                            "row_start": index + 1,
                            "row_end": index + 1,
                            "row_count": 1,
                            "first_timestamp": rows[index][0],
                            "last_timestamp": rows[index][0],
                            "from": day,
                            "to": day,
                        }
                        branches[selected_branch].append(run)
                    else:
                        run.update(
                            row_end=index + 1,
                            row_count=run["row_count"] + 1,
                            last_timestamp=rows[index][0],
                            to=day,
                        )
                    last_key = grouping
        for branch, segments in branches.items():
            report[branch] = {
                **original[branch],
                "segments": segments,
                "row_count": sum(s["row_end"] - s["row_start"] + 1 for s in segments),
            }
    return apply_adjustment_authority(result, source_names, plan) if allow_adjustment_continuity else result


def apply_adjustment_authority(reports, source_names, plan):
    """Add the completed new evidence without replaying owners or dated labels."""
    if plan.adjustment_bridges:
        raise ValueError("researched adjustment bridge recipes are inadmissible for new inference")
    result = dict(reports)
    for key, report in reports.items():
        name = source_names.get(key, key)
        selected = [bridge for bridge in plan.adjustment_bridges if bridge["continuing_source"] == name]
        if not selected:
            continue
        segments = []
        changed = False
        for segment in report["correlated"]["segments"]:
            proof = next(
                (
                    bridge
                    for bridge in selected
                    if bridge["public_id"] == segment["security_identity"]["security_id"]
                    and segment.get("observation_authority", {}).get("market") == "when_issued"
                    and bridge["from"] <= _bounds(segment)[0] <= _bounds(segment)[1] < bridge["to"]
                ),
                None,
            )
            if proof is None:
                segments.append(segment)
            else:
                changed = True
                segments.append(
                    {
                        **segment,
                        "observation_authority": {
                            **segment["observation_authority"],
                            "continuing_adjustments": proof,
                        },
                    }
                )
        if changed:
            result[key] = {**report, "correlated": {**report["correlated"], "segments": segments}}
    return result
