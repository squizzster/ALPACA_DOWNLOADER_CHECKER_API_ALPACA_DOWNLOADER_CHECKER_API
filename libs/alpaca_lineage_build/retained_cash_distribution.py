"""Carry accepted own-label positions through a native due-bill cash distribution."""

from datetime import date, timedelta

from libs.alpaca_end_of_day_security_correlation.due_bill_cash import (
    METHOD,
    due_bill_cash_terms,
    positive_activity,
)
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    AppliedOwnershipIntervalRule,
    TickerOwnershipInterval,
)
from libs.alpaca_pricing_download import check_summary
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.alpaca_security_lineage.inference_evidence import provider_evidence

from .accepted_owner_recovery import LOSS
from .assignment_handoff import produce, serving_ownership
from .contract import VERSION, LineageError, document_digest
from .duplicate_assignments import UnsupportedOverlap, bind_retained_labels
from .prepared_recovery import PreparedCandidate
from .source_work import assignment_documents


def _activity_tail(context, name, source, generation):
    request = source["latest_request"]
    if context.summary is not None:
        entry = next((item for item in context.summary["entries"] if item["symbol"] == name), None)
    else:
        summary = check_summary.selected_inputs(context.store, [name])
        entry = None
        if summary and summary["complete"] and "pricing" not in summary["pending"][name]:
            entry = summary["entries"].get(name)
    if (
        entry
        and (entry.get("source_sha256"), entry.get("asof"), entry.get("end_exclusive"))
        == (generation, request["asof"], request["end_exclusive"])
        and entry.get("tail_all") is not None
    ):
        return entry["tail_all"]
    root = context.store / "end_of_day" / "pricing" / name
    endpoint = pricing_storage.read_validator(root, source)
    tail = getattr(endpoint, "tail_all", None)
    if tail is None:
        raise LineageError("the selected pricing generation has no completed activity tail")
    return tail


def _source_terms(previous, name, pid):
    members = previous["identity_memberships"]
    terms = due_bill_cash_terms(provider_evidence(previous["evidence"]), name)
    if (
        terms is None
        or len(members) != 1
        or members[0]["security_id"] != pid
        or members[0]["member_alpaca_asset_ids"] != [terms.asset_id]
        or any(edge.get("action_id") == terms.departure_id for edge in previous.get("uuid_lineage_edges", ()))
    ):
        raise LineageError("native cash distribution and separate dated replacement do not support this owner")
    request = previous["pricing_request"]
    try:
        mapped_after_distribution = (
            request["asof"] == "-" or date.fromisoformat(request["asof"]) >= terms.ex_date
        )
    except (TypeError, ValueError):
        mapped_after_distribution = False
    if (request["symbol"], request["timeframe"], request["feed"]) != (
        name,
        "1Day",
        "sip",
    ) or not mapped_after_distribution:
        raise LineageError("pricing mapping does not select the paying security")
    route = previous.get("observation_routes", {}).get(pid)
    old = route[1] if route else {}
    cash_rules = [
        r
        for r in old.get("ownership_interval_rules", ())
        if r["name"] == "cash_merger_acquiree_terminal_boundary"
    ]
    if (
        (old.get("historical_symbol"), old.get("cusip"), old.get("asset_id"), old.get("end_date_exclusive"))
        != (name, terms.cusip, terms.asset_id, terms.ex_date.isoformat())
        or not {terms.cash_merger_id, terms.departure_id} <= set(old.get("supporting_corporate_action_ids", ()))
        or len(cash_rules) != 1
    ):
        raise LineageError(
            "the accepted terminal interval does not identify the paying security and both native actions"
        )
    return terms, cash_rules[0]["order"]


def _lost_suffix(group, losses, approved, previous):
    pid, owner, prior, size = approved
    name = group.seed
    old = prior["history_validation"]["inputs"][name]
    ordered = sorted(losses, key=lambda x: x["row_start"])
    start = ordered[0]["row_start"]
    next_row = start
    for loss in ordered:
        if (
            (loss["source"], loss["public_id"], loss["previous_pricing_generation"], loss["pricing_generation"])
            != (name, pid, old["pricing_generation"], group.pricing[name])
            or loss["boundary"] not in previous["boundaries"]
            or loss["row_start"] != next_row
            or loss["row_end"] < next_row
        ):
            raise LineageError("the selected trailing owner loss is not contiguous on its accepted inputs")
        next_row = loss["row_end"] + 1
    expected = {
        **old["views"]["raw"],
        "ownership": [{"owner": owner, "rows": start - 1}, {"owner": [None, None], "rows": size - start + 1}],
    }
    if (
        not 1 < start <= size
        or next_row != size + 1
        or (previous.get("serving_ownership") or {}).get("views") != dict.fromkeys(ADJUSTMENTS, expected)
        or previous["unresolved_rows"] != (size - start + 1) * len(ADJUSTMENTS)
    ):
        raise LineageError("the candidate-free suffix must be the only change to the approved all-view grid")
    return ordered


def _activity_scopes(tail, terms, ordered, size):
    count = size - ordered[0]["row_start"] + 1
    if len(tail) < count:
        raise LineageError("completed activity tail does not cover every lost position")
    chosen = tail[-count:]
    days = [timestamp(row[0]).astimezone(NEW_YORK).date() for row in chosen]
    if days[-1] <= terms.ex_date or not all(
        terms.ex_date <= day < terms.departure_date and positive_activity(row[5], row[6])
        for day, row in zip(days, chosen)
    ):
        raise LineageError("the complete lost suffix needs reported trading before the separate replacement")
    physical = {}
    positions = {day.isoformat(): index for index, day in enumerate(days, start=ordered[0]["row_start"])}
    for scope in ordered:
        lo, hi = scope["segment_from"], scope["segment_to"]
        if positions.get(lo) != scope["row_start"] or positions.get(hi) != scope["row_end"]:
            raise LineageError("dated activity does not coincide with the accepted lost positions")
        physical.setdefault(scope["boundary"], set()).add((lo, hi))
        if scope["boundary"] == "current_month":
            # The year and month selectors both publish the live month's assignments.
            physical.setdefault("current_year", set()).add((lo, hi))
    return days, positions, physical


def _restore(kind, document, scopes, membership, terms, interval, positions, offsets):
    entries = []
    canonical = {
        "security_id": membership["security_id"],
        "alpaca_asset_id": terms.asset_id,
        "cusip": terms.cusip,
    }
    for entry in document["adjustments"]:
        segments, found = [], set()
        offset = offsets[entry["adjustment"]][kind]
        for seg in entry["segments"]:
            key = seg["from"], seg["to"]
            if key not in scopes:
                segments.append(seg)
                continue
            if (
                key in found
                or seg["branch"] != "uncorrelated"
                or seg.get("reason_codes") != [LOSS]
                or seg.get("candidate_securities")
                or seg["row_count"] != positions[key[1]] - positions[key[0]] + 1
                or offset + seg["row_start"] != positions[key[0]]
                or offset + seg["row_end"] != positions[key[1]]
            ):
                raise LineageError("a physical assignment differs from the candidate-free traded suffix")
            found.add(key)
            segments.append(
                {
                    **{
                        key: seg[key]
                        for key in (
                            "row_start",
                            "row_end",
                            "row_count",
                            "from",
                            "to",
                            "first_timestamp",
                            "last_timestamp",
                        )
                    },
                    "branch": "correlated",
                    "historical_symbol": terms.symbol,
                    "canonical_security": canonical,
                    "security_identity": membership,
                    "identity_confidence": "action_supported",
                    "correlation_method": METHOD,
                    "correlation_rule": {"order": None, "name": METHOD},
                    "ticker_ownership_interval": interval,
                    "warnings": [
                        "observed_trading_dates_only",
                        "cash_merger_effective_date_matches_distribution_ex_date",
                    ],
                    "observation_selection": "selected",
                    "observation_tickers": [terms.symbol],
                }
            )
        if found != scopes:
            raise LineageError("an adjustment view lacks a required physical copy of the traded suffix")
        entries.append({**entry, "segments": segments})
    if {entry["adjustment"] for entry in entries} != set(ADJUSTMENTS):
        raise LineageError("the selected physical boundary lacks an adjustment view")
    return {**document, "schema_version": VERSION, "adjustments": entries}


def prepare(context, root, group, source, previous, losses, approved, *, group_policy):
    pid, _, prior, size = approved
    name = group.seed
    ordered = _lost_suffix(group, losses, approved, previous)
    terms, rule_order = _source_terms(previous, name, pid)
    tail = _activity_tail(context, name, source, group.pricing[name])
    days, positions, scopes = _activity_scopes(tail, terms, ordered, size)
    interval = TickerOwnershipInterval(
        name,
        terms.cusip,
        terms.asset_id,
        days[0],
        days[-1] + timedelta(days=1),
        (AppliedOwnershipIntervalRule(rule_order, METHOD, terms.action_ids),),
    ).as_report_object()
    accepted = assignment_documents(root, previous, {}, kinds=set(scopes))
    documents, references, handoffs = {}, dict(previous["boundaries"]), dict(previous["assignment_handoffs"])
    for kind, wanted in scopes.items():
        changed = _restore(
            kind,
            accepted[kind],
            wanted,
            previous["identity_memberships"][0],
            terms,
            interval,
            positions,
            previous["serving_ownership"]["offsets"],
        )
        generation = document_digest(changed)
        documents[kind] = changed
        references[kind] = {**references[kind], "sha256": generation}
        handoffs[kind] = produce(changed, generation)
    receipt = {
        **previous,
        "schema_version": VERSION,
        "boundaries": references,
        "assignment_handoffs": handoffs,
        "assignment_handoff_kinds": sorted(handoffs),
        "unresolved_rows": 0,
        "correlated_rows": previous["correlated_rows"] + previous["unresolved_rows"],
        "observation_routes": {**previous["observation_routes"], pid: [tail[-1][0], interval]},
        "identity_intervals": [
            *previous["identity_intervals"],
            *({"from": loss["segment_from"], "to": loss["segment_to"], "security_id": pid} for loss in ordered),
        ],
        "retained_owner_restoration": {
            "version": 1,
            "rule": METHOD,
            "public_id": pid,
            "row_start": ordered[0]["row_start"],
            "row_end": size,
            "decision_generation": prior["decision_generation"],
            "references": prior["references"],
            "former_reason": LOSS,
            "supporting_corporate_action_ids": list(terms.action_ids),
            "timing_basis": terms.timing_basis,
            "observed_from": days[0].isoformat(),
            "observed_through": days[-1].isoformat(),
            "separate_asset_id": terms.replacement_asset_id,
            "correlation_policy_sha256": group_policy,
            "pricing_generation": group.pricing[name],
        },
    }
    receipt["serving_ownership"] = serving_ownership(receipt)
    if receipt["serving_ownership"]["views"] != prior["history_validation"]["inputs"][name]["views"]:
        raise LineageError("restored ownership does not reproduce the completed numerical input grid")
    receipts = {name: receipt}
    try:
        bind_retained_labels(receipts, {pid})
    except UnsupportedOverlap as error:
        raise LineageError(str(error)) from error
    return PreparedCandidate(
        receipts, {name: documents}, {name: previous}, receipt["correlation_policy"], {name: documents}
    )
