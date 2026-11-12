"""Allocate documented trading episodes without requiring an existing owned row.

Metadata owns the accepted assertions and exact provider observations. This
producer binds real UUIDs once, then projects new ownership only into unresolved
positions. Existing positive decisions and numerical states remain authoritative.
"""

from copy import deepcopy
from dataclasses import dataclass

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND, items

from .errors import SecurityCorrelationError
from .security_identity import security_identity_for_alpaca_assets


def facts_for(evidence):
    return tuple(fact for fact in items(evidence, MARKET_KIND) if fact["facts"]["schema_version"] in (3, 4, 5))


def bind(
    evidence,
    *,
    accepted_memberships=None,
    allow_containing_membership=False,
    allow_reclassification=False,
    allow_holding_exchange=False,
):
    """Produce a completed identity handoff from selected metadata, without rows."""
    accepted_memberships = accepted_memberships or {}
    facts, lookups, routes = {}, {}, {}
    for name, report in evidence.items():
        for fact in facts_for(report):
            key = fact["match"]["source_id"]
            old = facts.setdefault(key, fact)
            if old is not fact and old != fact:
                raise SecurityCorrelationError(f"{key}: conflicting documented security assertions")
        own = set()
        for lookup in report.get("asset_lookups", ()):
            if lookup["lookup_kind"] == "cusip":
                lookups.setdefault(lookup["lookup"], []).append(lookup)
            elif lookup["lookup"] == name and lookup.get("asset"):
                own.add(lookup["asset"]["id"])
        routes[name] = sorted(own)
    owners = []
    for key, fact in sorted(facts.items()):
        value = fact["facts"]
        if value["schema_version"] == 5 and not allow_holding_exchange:
            raise SecurityCorrelationError("documented public holding exchange requires correlation policy 90")
        if value["schema_version"] == 4 and not allow_reclassification:
            raise SecurityCorrelationError("documented public reclassification requires correlation policy 89")
        cusips = {value[role]["cusip"] for role in ("predecessor", "successor")} | {
            episode["cusip"] for episode in value["episodes"]
        }
        endpoints = {}
        for cusip in sorted(cusips):
            observed = lookups.get(cusip, ())
            if not observed or any(
                item.get("outcome") not in {"found", "not_found"}
                or item["outcome"] == "found"
                and (item.get("http_status") != 200 or not (item.get("asset") or {}).get("id"))
                or item["outcome"] == "not_found"
                and (item.get("http_status") != 404 or item.get("asset") is not None)
                for item in observed
            ):
                raise SecurityCorrelationError(f"{cusip}: owning exact metadata recovery is required")
            found = {(item.get("asset") or {}).get("id") for item in observed}
            if len(found) != 1:
                raise SecurityCorrelationError(f"{cusip}: conflicting completed provider mappings")
            endpoints[cusip] = next(iter(found))
        members = {identifier for identifier in endpoints.values() if identifier is not None}
        if not members:
            raise SecurityCorrelationError(f"{key}: the documented owner needs an observed provider UUID")
        old = {
            identity["security_id"]: identity
            for scope in accepted_memberships.values()
            for identifier, identity in scope.items()
            if identifier in members
        }
        containing = (
            [
                identity
                for identity in old.values()
                if all(
                    set(peer["member_alpaca_asset_ids"]) <= set(identity["member_alpaca_asset_ids"])
                    for peer in old.values()
                )
            ]
            if allow_containing_membership and len(old) > 1
            else []
        )
        if len(old) > 1 and len(members) == 1 and len(containing) != 1:
            raise SecurityCorrelationError(f"{key}: source-scoped public membership needs owning recovery")
        for identity in old.values():
            members.update(identity["member_alpaca_asset_ids"])
        labels = {episode["symbol"] for episode in value["episodes"]}
        existing = containing[0] if len(containing) == 1 else next(iter(old.values())) if len(old) == 1 else None
        if existing is not None and set(existing["member_alpaca_asset_ids"]) == members:
            identity = existing
            if labels - set(identity["member_tickers"]):
                identity = {**identity, "member_tickers": sorted(labels | set(identity["member_tickers"]))}
        else:
            identity = security_identity_for_alpaca_assets(
                members, [], tickers=labels, basis="sourced_documented_public_security"
            ).as_report_object()
        owners.append(
            {
                "source_fact_id": key,
                "identity": identity,
                "cusip_assets": endpoints,
                "episodes": value["episodes"],
                "instrument_kind": value["successor"]["instrument_kind"],
                "registrant_cik": value["registrant_cik"],
                "membership_resolution": "completed_containing_component"
                if containing
                else "documented_endpoint_component",
            }
        )
    return {"version": 1, "owners": owners, "source_routes": routes}


def _episode(owner, day):
    return next(
        (
            item
            for item in owner["episodes"]
            if item["start_inclusive"] <= day and (item["end_exclusive"] is None or day < item["end_exclusive"])
        ),
        None,
    )


@dataclass(frozen=True)
class _Choice:
    selected: tuple | None
    alternatives: tuple = ()

    def __bool__(self):
        return self.selected is not None


def _choice(plan, source, day):
    route = set(plan["source_routes"].get(source, ()))
    current = [owner for owner in plan["owners"] if route & set(owner["identity"]["member_alpaca_asset_ids"])]
    direct = [(owner, episode) for owner in current if (episode := _episode(owner, day)) is not None]
    if direct:
        candidates, basis = direct, "continuing_provider_identity"
    elif current or not route:
        # The documented current security does not cover this date. A dated
        # former occupant can explain a reused ticker's retained observations.
        candidates = [
            (owner, episode)
            for owner in plan["owners"]
            if (episode := _episode(owner, day)) is not None and episode["symbol"] == source
        ]
        basis = "dated_former_ticker_occupant" if current else "dated_label_with_exact_provider_identity"
    else:
        raise SecurityCorrelationError(f"{source}: owning source-context recovery is required")
    unique = {
        (owner["identity"]["security_id"], episode["symbol"], episode["cusip"]): (owner, episode)
        for owner, episode in candidates
    }
    if len(unique) != 1:
        return _Choice(None, tuple(unique.values()))
    return _Choice((*next(iter(unique.values())), basis))


def _nonallocation(original, choice):
    return {
        **original,
        "reason_codes": list(
            dict.fromkeys(
                (
                    *original.get("reason_codes", ()),
                    "competing_documented_public_owners"
                    if choice.alternatives
                    else "outside_documented_public_trading_episodes",
                )
            )
        ),
        "candidate_securities": [
            {
                "security_id": owner["identity"]["security_id"],
                "alpaca_asset_id": owner["cusip_assets"][episode["cusip"]],
                "cusip": episode["cusip"],
                "symbol": episode["symbol"],
            }
            for owner, episode in choice.alternatives
        ],
        "correlation_rule": {
            "name": "documented_public_security_nonallocation",
            "allocation_assessment": {
                "nonallocation": "genuine_owner_ambiguity"
                if choice.alternatives
                else "most_likely_after_known_dated_owners_excluded",
            },
        },
    }


def _allocate(original, owner, episode, basis):
    piece = {
        key: value
        for key, value in original.items()
        if key not in {"reason_codes", "candidate_securities", "market_data_series_identity", "branch"}
    }
    identifier = owner["cusip_assets"][episode["cusip"]]
    piece.update(
        historical_symbol=episode["symbol"],
        security_identity=owner["identity"],
        canonical_security={
            "security_id": owner["identity"]["security_id"],
            "alpaca_asset_id": identifier,
            "cusip": episode["cusip"],
        },
        identity_confidence="documented_public_security",
        correlation_method="sourced_documented_public_security",
        correlation_rule={
            "name": "documented_public_security",
            "supporting_sourced_fact_ids": [owner["source_fact_id"]],
            "allocation_assessment": {
                "selected_basis": basis,
                "nonallocation": "weaker_than_dated_security_and_completed_provider_binding",
                "material_weaknesses": ["dated_provider_uuid_unavailable"] if identifier is None else [],
            },
        },
        ticker_ownership_interval={
            "historical_symbol": episode["symbol"],
            "cusip": episode["cusip"],
            "asset_id": identifier,
            "start_date_inclusive": episode["start_inclusive"],
            "end_date_exclusive": episode["end_exclusive"],
            "supporting_sourced_fact_ids": [owner["source_fact_id"]],
            "security_description": {
                "version": 1,
                "name": episode["name"],
                "exchange": episode["exchange"],
                "asset_class": "us_equity",
                "instrument_kind": owner["instrument_kind"],
                "registrant_cik": owner["registrant_cik"],
            },
        },
        observation_tickers=[episode["symbol"]],
        warnings=list(original.get("warnings", ())),
    )
    # Ownership does not approve a numerical bundle. Its owning selector must
    # establish the new state independently in every adjustment view.
    piece.pop("observation_selection", None)
    return piece


def refinement_ids(evidence, plan, *, default):
    """Select explicit per-fact intent without broadening unresolved-only scopes."""
    intents = {}
    for report in evidence.values():
        for identifier, refine in report.get("sourced_fact_intents", {}).items():
            if identifier in intents and intents[identifier] != refine:
                raise ValueError("conflicting documented refinement intent")
            intents[identifier] = refine
    return frozenset(
        owner["source_fact_id"] for owner in plan["owners"] if intents.get(owner["source_fact_id"], default)
    )


def project(
    inputs,
    reports,
    evidence,
    *,
    plan=None,
    allow_containing_membership=False,
    allow_reclassification=False,
    allow_holding_exchange=False,
    refine_accepted_dates=False,
    allow_report_memberships=False,
):
    """Fill unresolved dated positions, keeping all accepted positives intact."""
    if plan is None and not any(facts_for(value) for value in evidence.values()):
        return reports
    if plan is None:
        plan = next(
            (
                view.accepted_documented_membership
                for view in inputs.values()
                if getattr(view, "accepted_documented_membership", None) is not None
            ),
            None,
        )
    if plan is None:
        accepted = {
            name: getattr(view, "accepted_identity_memberships", None) or {} for name, view in inputs.items()
        }
        if allow_report_memberships:
            for name, report in reports.items():
                identities = {
                    segment["security_identity"]["security_id"]: segment["security_identity"]
                    for segment in report["correlated"]["segments"]
                }
                for public_id, identity in identities.items():
                    accepted[name, public_id] = dict.fromkeys(identity["member_alpaca_asset_ids"], identity)
        plan = bind(
            evidence,
            accepted_memberships=accepted,
            allow_containing_membership=allow_containing_membership,
            allow_reclassification=allow_reclassification,
            allow_holding_exchange=allow_holding_exchange,
        )
    if refine_accepted_dates:
        from .documented_refinement import refine

        selected_ids = refinement_ids(evidence, plan, default=True)
        reports = refine(
            inputs,
            reports,
            {
                **plan,
                "owners": [owner for owner in plan["owners"] if owner["source_fact_id"] in selected_ids],
            },
        )
    result, decisions, dates = {}, {}, {}
    for name, original in reports.items():
        report = result[name] = {**original, "documented_identity_membership": plan}
        if not original["uncorrelated"]["segments"]:
            continue
        report["correlated"] = {
            "row_count": original["correlated"]["row_count"],
            "segments": list(original["correlated"]["segments"]),
        }
        report["uncorrelated"] = {"row_count": 0, "segments": []}
        for segment in original["uncorrelated"]["segments"]:
            first = (
                segment.get("from")
                or timestamp(segment["first_timestamp"]).astimezone(NEW_YORK).date().isoformat()
            )
            last = (
                segment.get("to") or timestamp(segment["last_timestamp"]).astimezone(NEW_YORK).date().isoformat()
            )
            cuts = {
                day
                for owner in plan["owners"]
                for episode in owner["episodes"]
                for day in (episode["start_inclusive"], episode["end_exclusive"])
                if day is not None and first < day <= last
            }
            if not cuts:
                key = name, first
                if key not in decisions:
                    decisions[key] = _choice(plan, name, first)
                choice = decisions[key]
                branch = "correlated" if choice else "uncorrelated"
                piece = _allocate(segment, *choice.selected) if choice else _nonallocation(segment, choice)
                report[branch]["segments"].append(piece)
                report[branch]["row_count"] += segment["row_count"]
                continue
            rows = inputs[name].views[segment["adjustment"]]
            run, run_key = None, None
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                stamp = rows[index][0]
                if stamp not in dates:
                    dates[stamp] = timestamp(stamp).astimezone(NEW_YORK).date().isoformat()
                day = dates[stamp]
                key = name, day
                if key not in decisions:
                    decisions[key] = _choice(plan, name, day)
                choice = decisions[key]
                grouping = (
                    (
                        choice.selected[0]["source_fact_id"],
                        choice.selected[1]["start_inclusive"],
                        choice.selected[2],
                    )
                    if choice
                    else tuple(owner["identity"]["security_id"] for owner, _ in choice.alternatives)
                )
                branch = "correlated" if choice else "uncorrelated"
                if run is None or grouping != run_key:
                    piece = (
                        _allocate(segment, *choice.selected)
                        if choice
                        else _nonallocation(deepcopy(segment), choice)
                    )
                    run = {
                        **piece,
                        "row_start": index + 1,
                        "row_end": index + 1,
                        "row_count": 1,
                        "from": day,
                        "to": day,
                        "first_session_date": day,
                        "last_session_date": day,
                        "first_timestamp": stamp,
                        "last_timestamp": stamp,
                    }
                    report[branch]["segments"].append(run)
                    run_key = grouping
                else:
                    run.update(
                        row_end=index + 1,
                        row_count=run["row_count"] + 1,
                        to=day,
                        last_session_date=day,
                        last_timestamp=stamp,
                    )
                report[branch]["row_count"] += 1
    return result
