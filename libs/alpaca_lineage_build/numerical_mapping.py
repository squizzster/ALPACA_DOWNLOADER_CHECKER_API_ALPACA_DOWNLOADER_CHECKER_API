"""Select dated provider mappings before new numerical source decisions.

Accepted ownership and provider observations are inputs, never conclusions from
research. Closed numerical decisions bypass this owner. A bounded negative
assessment records provider-view limits without changing identity or eligibility.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_security_lineage.inference_evidence import provider_evidence

from .contract import LineageError, RecoveryRequired, document_digest

VERSION = 1
FIELD = "numerical_mapping_assessments"


@dataclass(frozen=True)
class PendingNumericalWork:
    required: dict


class AcceptedAssessments(dict):
    """Receipt-ingress acceptance; consumers retain this completed capability."""


def accept_assessments(receipt):
    value = receipt.get(FIELD)
    if value is None or isinstance(value, AcceptedAssessments):
        return value
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "decisions"}
        or type(value["version"]) is not int
        or value["version"] != VERSION
        or not isinstance(value["decisions"], list)
    ):
        raise LineageError("unsupported numerical mapping assessment; owning recovery required")
    for decision in value["decisions"]:
        if (
            not isinstance(decision, dict)
            or set(decision) != {"candidate_id", "binding", "outcome", "witness", "result_generation", "scope"}
            or not isinstance(decision["candidate_id"], str)
            or not isinstance(decision["binding"], dict)
            or decision["outcome"]
            not in {
                "provider_mapping_unavailable",
                "provider_coverage_insufficient",
                "mapped_history_acquired",
            }
            or not isinstance(decision["witness"], dict)
            or not isinstance(decision["scope"], dict)
        ):
            raise LineageError("invalid numerical mapping assessment; owning recovery required")
    accepted = AcceptedAssessments(value)
    receipt[FIELD] = accepted
    return accepted


def carry(previous, current):
    """Carry completed provenance; applicability remains bound to actual inputs."""
    if FIELD in previous and FIELD not in current:
        current[FIELD] = previous[FIELD]


def stamp(receipts, decisions):
    if not decisions:
        return
    for name, receipt in receipts.items():
        relevant = [item for item in decisions.values() if name in item["binding"]["members"]]
        if not relevant:
            continue
        old = receipt.get(FIELD, {}).get("decisions", ())
        merged = {item["candidate_id"]: item for item in (*old, *relevant)}
        receipt[FIELD] = AcceptedAssessments(version=VERSION, decisions=list(merged.values()))
        issues = list(receipt.get("evidence_issues", ()))
        for item in relevant:
            if item["outcome"] == "mapped_history_acquired":
                continue
            binding = item["binding"]
            issue = {
                "reason_code": item["outcome"],
                "source": binding["symbol"],
                "asof": binding["asof"],
                "witness_session": binding["witness_session"],
                "scope": item["scope"]["kind"],
            }
            if issue not in issues:
                issues.append(issue)
        receipt["evidence_issues"] = issues


def _day(value):
    return timestamp(value).astimezone(NEW_YORK).date().isoformat()


def _within(day, interval):
    lower, upper = interval.get("start_date_inclusive"), interval.get("end_date_exclusive")
    return (lower is None or lower <= day) and (upper is None or day < upper)


def select(store, sources, generations, reports, evidence, *, selected=False, completed=None):
    """Return only an admitted generic mapping need, with no provider/storage I/O.

    This first rule supports a two-source, unchanged-UUID/CUSIP direct rename.
    More complex or ambiguous groups retain their existing owning decision.
    """
    if not selected or len(sources) != 2 or sources.keys() != reports.keys():
        return {}
    names = set(sources)
    memberships = [report.get("identity_memberships", ()) for report in reports.values()]
    if any(
        report.get("unresolved_rows", report.get("uncorrelated", {}).get("row_count", 0))
        for report in reports.values()
    ):
        return {}
    for report in reports.values():
        total = report.get("source_rows", report.get("source_row_count"))
        assigned = report.get("correlated_rows", report.get("correlated", {}).get("row_count"))
        if total is None or assigned != total:
            return {}
    if any(len(items) != 1 or len(items[0]["member_alpaca_asset_ids"]) != 1 for items in memberships):
        return {}
    owners = {(items[0]["security_id"], items[0]["member_alpaca_asset_ids"][0]) for items in memberships}
    if len(owners) != 1:
        return {}
    public_id, uuid = next(iter(owners))
    routes = {}
    for name, report in reports.items():
        route = report.get("observation_routes", {}).get(public_id)
        if (
            not route
            or route[1].get("historical_symbol") != name
            or route[1].get("asset_id") != uuid
            or not _within(_day(route[0]), route[1])
        ):
            return {}
        routes[name] = route
    cusips = {route[1].get("cusip") for route in routes.values()}
    if len(cusips) != 1 or not next(iter(cusips)):
        return {}
    cusip = next(iter(cusips))
    actions, lookups = {}, {}
    facts_by_name = {name: provider_evidence(evidence[name]) for name in names}
    for facts in facts_by_name.values():
        if (
            facts.get("unresolved_cusips")
            or facts.get("query", {}).get("corporate_actions_data_quality") != "complete"
            or not names <= set(facts.get("queried_symbols", ()))
            or any(asset.get("id") not in {None, uuid} for asset in facts.get("identities", ()))
        ):
            return {}
        for lookup in facts.get("asset_lookups", ()):
            if lookup.get("lookup") not in names | {cusip}:
                continue
            asset = lookup.get("asset") or {}
            if asset.get("id") not in {None, uuid}:
                return {}
            key = lookup.get("lookup_kind"), lookup.get("lookup")
            value = lookup.get("outcome"), asset.get("id")
            if key in lookups and lookups[key] != value:
                return {}
            lookups[key] = value
        if any(
            item.get("symbol") in names and item.get("asset_id") not in {None, uuid}
            for item in facts.get("symbol_occupancy_evidence", ())
        ):
            return {}
        for action in facts.get("corporate_actions", ()):
            key = action.get("id")
            if not key or key in actions and actions[key] != action:
                return {}
            actions[key] = action
    renames = [
        action
        for action in actions.values()
        if action.get("action_type") == "name_change"
        and names & {action.get("old_symbol"), action.get("new_symbol")}
    ]
    if len(renames) != 1 or any(
        action.get("action_type") not in {"cash_dividend", "name_change"} for action in actions.values()
    ):
        return {}
    rename = renames[0]
    old, new = rename.get("old_symbol"), rename.get("new_symbol")
    if {old, new} != names or rename.get("old_cusip") != cusip or rename.get("new_cusip") != cusip:
        return {}
    boundary = routes[old][1].get("end_date_exclusive")
    if not boundary or boundary != routes[new][1].get("start_date_inclusive"):
        return {}
    if any(rename["id"] not in routes[name][1].get("supporting_corporate_action_ids", ()) for name in names):
        return {}
    summaries = {name: reports[name].get("assignment_summary", {}).get("all", {}) for name in names}
    if any(
        not item.get("rows") or not item.get("first_timestamp") or not item.get("last_timestamp")
        for item in summaries.values()
    ):
        return {}
    first = {
        name: (_day(item["first_timestamp"]), _day(item["last_timestamp"])) for name, item in summaries.items()
    }
    last = {name: dates[1] for name, dates in first.items()}
    first = {name: dates[0] for name, dates in first.items()}
    if not first[old] <= last[old] < boundary <= first[new] <= last[new]:
        return {}
    if not all(_within(day, routes[name][1]) for name in names for day in (first[name], last[name])):
        return {}
    asof = _day(routes[new][0])
    if asof != last[new] or sources[new]["latest_request"]["asof"] == asof:
        return {}
    for facts in facts_by_name.values():
        query = facts["query"]
        if (
            not query.get("corporate_actions_start")
            or not query.get("corporate_actions_end")
            or query["corporate_actions_start"] > first[old]
            or query["corporate_actions_end"] < last[new]
        ):
            return {}
    if not any(
        item.get("action_type") == "cash_dividend"
        and item.get("symbol") in names
        and boundary <= (item.get("ex_date") or "") <= last[new]
        for item in actions.values()
    ):
        return {}
    # This is a new decision digest, not authentication of accepted receipt bytes.
    # Receipt generations are deliberately excluded: stamping this result must
    # not invalidate its own completion on the next selected invocation.
    binding = {
        "store": str(store),
        "members": sorted(names),
        "pricing_generations": {name: generations[name] for name in sorted(names)},
        "requests": {name: sources[name]["latest_request"] for name in sorted(names)},
        "public_id": public_id,
        "asset_id": uuid,
        "cusip": cusip,
        "routes": {name: list(routes[name]) for name in sorted(names)},
        "actions": [actions[key] for key in sorted(actions)],
        "lookups": [[*key, *value] for key, value in sorted(lookups.items())],
        "symbol": new,
        "asof": asof,
        "witness_session": first[old],
    }
    candidate_id = document_digest(binding)
    accepted = dict(completed or {})
    for report in reports.values():
        for decision in report.get(FIELD, {}).get("decisions", ()):
            accepted.setdefault(decision["candidate_id"], decision)
    if candidate_id in accepted:
        return {}
    return {
        new: {
            "from": min(source["history_start"][:10] for source in sources.values()),
            "end_exclusive": sources[new]["latest_request"]["end_exclusive"][:10],
            "refresh_all": True,
            "selected_source_sha256": generations[new],
            "mapping_asof": asof,
            "numerical_mapping": {"version": VERSION, "candidate_id": candidate_id, "binding": binding},
        }
    }


def witness_window(need):
    day = need["binding"]["witness_session"]
    start = datetime.fromisoformat(day).replace(tzinfo=NEW_YORK)
    return start, start + timedelta(days=1)


def required_sessions(inputs, reports, *, checkpoint=None):
    """Lend the complete selected owner grid, never guess it from a calendar.

    Only complete already held source views can construct this projection.
    An accepted matching serving checkpoint may supply closed prefixes; callers
    establish its selector/source binding before lending its date capability.
    """
    dates = set(checkpoint or ())
    for name, view in inputs.items():
        if not getattr(view, "materialized", True):
            if not checkpoint:
                return None
            continue
        scope = getattr(view, "materialization_scope", "complete_history")
        if scope != "complete_history" and not checkpoint:
            return None
        report = reports.get(name)
        if report is None:
            return None
        for segment in report.get("correlated", {}).get("segments", ()):
            rows = view.views[segment["adjustment"]]
            dates.update(_day(row[0]) for row in rows[segment["row_start"] - 1 : segment["row_end"]])
    return tuple(sorted(dates)) if dates else None


def prepare(store, sources, generations, reports, evidence, cache, *, selected, inputs=None, previous=None):
    """Memoize this operation's new planning without reopening accepted bodies."""
    if not selected:
        return {}
    previous = previous or {}
    if any(
        previous.get(name) is None
        or previous[name].get("source_sha256") != generations[name]
        or not previous[name].get("observation_routes")
        for name in sources
    ):
        return {}  # Freshly inferred routes are outside this retained-evidence producer.
    key = tuple(sorted(generations.items()))
    plans = cache.setdefault("plans", {})
    held = plans.get(key)
    if held is None or not (
        held["evidence"].keys() == evidence.keys()
        and all(held["evidence"][name] is facts for name, facts in evidence.items())
        and held["reports"].keys() == sources.keys()
        and all(held["reports"][name] is previous[name] for name in sources)
    ):
        needs = select(
            store,
            sources,
            generations,
            {name: previous[name] for name in sources},
            evidence,
            selected=True,
            completed=cache.setdefault("completed", {}),
        )
        held = {
            "evidence": dict(evidence),
            "reports": {name: previous[name] for name in sources},
            "required": needs,
        }
        plans[key] = held
    completed = cache.setdefault("completed", {})
    return {
        name: request
        for name, request in held["required"].items()
        if request["numerical_mapping"]["candidate_id"] not in completed
    }


def checkpoint_sessions(context, generations, public_id, cache, *, partitions=None):
    """Borrow a matching completed serving grid for selected closed sources."""
    if context is None or context.index is None:
        return None
    from libs.alpaca_serving_catalogue import storage as serving_storage

    try:
        catalogue = context.select_serving()
    except (OSError, RuntimeError):
        return None
    keys = {catalogue["members"].get(name) for name in generations}
    if None in keys or len(keys) != 1:
        return None
    key = next(iter(keys))
    selection = catalogue["groups"][key]
    references = selection["references"]
    lineage = {
        name: context.index["symbols"][name]["sha256"]
        for name in generations
        if name in context.index["symbols"]
    }
    if references["pricing"] != generations or references["lineage"] != lineage:
        return None
    token = catalogue["state"]["generation"], key
    selected = cache.setdefault("checkpoints", {})
    if token not in selected:
        selected[token] = serving_storage.read_groups(
            context.store, {key}, generation=catalogue["state"]["generation"]
        )[key]
    records = [
        record
        for record in selected[token]["history_validation"]["prefix_records"]
        if record["public_id"] == public_id
    ]
    if len(records) != 1:
        return None
    record = records[0]
    if "session_dates" in record:
        return tuple(record["session_dates"])
    if record.get("session_projection_version") is not None:
        raise RecoveryRequired("declared mapping contribution dates are missing; owning recovery required")
    if partitions is None:
        raise RecoveryRequired("legacy mapping contribution dates need their selected assembly owner")
    from libs.alpaca_serving_catalogue.contract import CatalogueError
    from libs.alpaca_serving_catalogue.session_projection import legacy_dates

    projections = cache.setdefault("legacy_dates", {})
    projection_key = token, public_id
    if projection_key not in projections:
        try:
            projections[projection_key] = legacy_dates(
                context.store,
                selected[token],
                record,
                cache.get("source_selections", {}),
                partitions,
            )
        except CatalogueError as error:
            raise RecoveryRequired(str(error)) from error
    return projections[projection_key]
