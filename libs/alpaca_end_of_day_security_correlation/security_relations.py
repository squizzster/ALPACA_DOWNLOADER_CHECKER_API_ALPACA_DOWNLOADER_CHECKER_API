"""Metadata-only non-union relations and exact continuing-acquirer exemptions.

A certificate contains no public IDs. Publication projects those references from
completed membership; price changes and identity projection never recertify facts.
"""

from copy import deepcopy

from libs.alpaca_security_lineage.sourced_facts import CONVERSION_KIND, items


def endpoint(evidences, fact):
    observations = [
        lookup
        for evidence in evidences
        for lookup in evidence.get("asset_lookups", ())
        if lookup.get("lookup_kind") == "cusip" and lookup.get("lookup") == fact["cusip"]
    ]
    ids = sorted({lookup["asset"]["id"] for lookup in observations if (lookup.get("asset") or {}).get("id")})
    contradictory = len(ids) > 1 or bool(ids) and any(not item.get("asset") for item in observations)
    return {
        "instrument_kind": fact["instrument_kind"],
        "cusip": fact["cusip"],
        "historical_symbol": fact["symbol"],
        "asset_id": ids[0] if len(ids) == 1 and not contradictory else None,
        "resolution": "ambiguous" if contradictory else "resolved" if ids else "external_identifiers_only",
        "candidate_asset_ids": ids if contradictory else [],
    }


def certify_group(evidences, *, complete=True, extended_terms=True):
    evidences = tuple(evidences)
    submitted = {}
    for evidence in evidences:
        for item in items(evidence, CONVERSION_KIND):
            submitted.setdefault(item["match"]["source_id"], []).append(item)
    relations, issues = [], []
    for identifier, observations in sorted(submitted.items()):
        item = observations[0]
        fact = item["facts"]
        match = fact["provider_event_match"]
        if (
            not complete
            or fact["schema_version"] == 2
            and not extended_terms
            or any(other != item for other in observations)
            or not bind_provider_event(fact, evidences, conversion=True)
        ):
            issues.append(
                {"source_fact_id": identifier, "reason_code": "conflicting_or_incomplete_conversion_evidence"}
            )
            continue
        predecessor, successor = (endpoint(evidences, fact[role]) for role in ("predecessor", "successor"))
        resolved = sum(value["asset_id"] is not None for value in (predecessor, successor))
        if predecessor["asset_id"] is not None and predecessor["asset_id"] == successor["asset_id"]:
            issues.append(
                {
                    "source_fact_id": identifier,
                    "reason_code": "distinct_conversion_endpoints_share_provider_identity",
                }
            )
            continue
        relations.append(
            {
                "relation_id": "sourced:" + identifier,
                "relation_kind": CONVERSION_KIND,
                "identity_union": False,
                "source_fact_id": identifier,
                "corporate_action_id": match["id"] if match else None,
                "effective_date": fact["legal_effective_date"],
                "first_successor_trading": fact["first_successor_trading"],
                "trading_boundary": fact["trading_boundary"],
                "predecessor": predecessor,
                "successor": successor,
                "consideration": fact["consideration"],
                **({"consideration_schema_version": 2} if fact["schema_version"] == 2 else {}),
                "consideration_status": fact["consideration_status"],
                "continuing_acquirer": fact["acquirer_security_continues"],
                "ownership_effective_dates": fact["ownership_effective_dates"],
                "settlement_dates": fact["settlement_dates"],
                "endpoint_resolution": "resolved"
                if resolved == 2
                else "partial"
                if resolved
                else "external_identifiers_only",
                "source_provenance": item["sources"],
            }
        )
    return {"version": 1, "relations": relations, "issues": issues}


def acquirer_nonconflicts(relations, rename):
    """Produce exact action/rename/component/date exemptions, never action flags."""
    result = set()
    for relation in relations["relations"]:
        acquirer, target = relation["successor"], relation["predecessor"]
        dates = [relation["effective_date"], *relation["ownership_effective_dates"]]
        trading = relation["first_successor_trading"]
        if trading:
            dates.append(trading["date"])
        if not (
            relation["corporate_action_id"]
            and relation["continuing_acquirer"]
            and relation["ownership_effective_dates"]
            and acquirer["instrument_kind"] in {"common_stock", "public_common_stock"}
            and (acquirer["asset_id"], acquirer["cusip"]) == (rename.old_id, rename.old_cusip)
            and target["cusip"] not in {rename.old_cusip, rename.new_cusip}
            and target["resolution"] != "ambiguous"
            and all(day < rename.boundary for day in dates)
        ):
            continue
        covered = next(
            (
                label
                for label in rename.labels
                if label[0] == acquirer["historical_symbol"]
                and all(label[1] <= day < label[2] for day in dates if day == relation["effective_date"])
            ),
            None,
        )
        if covered is None or any(day < rename.labels[0][1] for day in dates):
            continue
        result.add(
            (
                relation["source_fact_id"],
                relation["corporate_action_id"],
                rename.action_id,
                rename.old_id,
                rename.old_cusip,
                covered,
                rename.boundary,
            )
        )
    return frozenset(result)


def project(certificate, memberships):
    """Consume completed metadata and public membership; perform no ID hashing."""
    result = deepcopy(certificate)
    for relation in result["relations"]:
        for role in ("predecessor", "successor"):
            end = relation[role]
            identity = memberships.get(end["asset_id"])
            end["security_id"] = identity["security_id"] if identity is not None else None
    return result


def bind_provider_event(fact, evidences, *, conversion):
    """Accept a sourced claim's exact raw-event role/date/term binding once.

    Absence is legitimate only when no real event was asserted. Payment and
    operational processing dates are not ownership dates; explicit effective
    fields are. Known provider stock ratios must agree with known sourced terms.
    """
    from decimal import Decimal

    match = fact["provider_event_match"]
    if match is None:
        return True
    actions = [
        action
        for evidence in evidences
        for action in evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ()))
        if action.get("id") == match["id"]
    ]
    if not actions:
        return False
    p, s = fact["predecessor"], fact["successor"]
    allowed_dates = {fact["legal_effective_date"], *fact.get("ownership_effective_dates", ())}
    if fact.get("exchange_effective_date"):
        allowed_dates.add(fact["exchange_effective_date"])
    for action in actions:
        if any(action.get(key) != value for key, value in match.items()):
            return False
        if action.get("action_type") != "stock_merger":
            return False
        if (action.get("acquiree_cusip"), action.get("acquirer_cusip")) != (p["cusip"], s["cusip"]):
            return False
        if any(
            action.get(key) not in (None, expected)
            for key, expected in (("acquiree_symbol", p["symbol"]), ("acquirer_symbol", s["symbol"]))
        ):
            return False
        effective = [action[key] for key in ("effective_date", "ex_date") if action.get(key)]
        if not effective or any(day not in allowed_dates for day in effective):
            return False
        # A process_date is bound only if supplied as an ownership-effective
        # fact. Do not import name_change._action_dates' payment chronology.
        if "process_date" in match and action["process_date"] not in allowed_dates:
            return False
        rates = {}
        for key in ("acquiree_rate", "acquirer_rate"):
            if action.get(key) is not None:
                try:
                    rates[key] = Decimal(str(action[key]))
                    if not rates[key].is_finite() or rates[key] <= 0:
                        return False
                except ArithmeticError:
                    return False
        if len(rates) == 2:
            if conversion:
                terms = fact["consideration"]
                if not terms:
                    continue
                if len(terms) != 1:
                    return False
                old_units, new_units = terms[0]["old_units"], terms[0]["new_units"]
            else:
                treatment = fact["public_share_treatment"]
                old_units, new_units = treatment["predecessor_units"], treatment["successor_units"]
            if rates["acquirer_rate"] * Decimal(old_units) != rates["acquiree_rate"] * Decimal(new_units):
                return False
    return True
