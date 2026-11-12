"""Carry recorded lineage through a completed, value-only latest correction.

Inputs are selected producer handoffs, not filenames. This owner performs no
storage/provider I/O and does not execute ownership or numerical-selection rules.
The publisher checks the returned named bindings under its normal group locks.
Changed values remain new serving inputs; this is not a price-equality approval.
"""

from decimal import Decimal
from pathlib import Path

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError

BOUNDARY = "alpaca_lineage_correction_binding"
VERSION = 1

# These recorded decisions use metadata/calendar identity and trading-state
# predicates. A positive count revision with unchanged OHLC/VWAP changes neither
# their owner nor their accepted continuity witness. No predicate is rerun here.
_INVARIANT_RULES = frozenset(
    {
        "asof_selected_uuid_lineage_interval",
        "stable_continuous_ticker_identity_set",
    }
)
_VALUE_CAPABILITIES = (
    "numerical_source_revision",
    "numerical_source_revision_version",
    "numerical_source_equivalence",
    "numerical_source_equivalence_version",
    "selected_observation_bundles",
    "dated_market_projection",
    "dated_market_projection_version",
    "membership_only_rebind",
)


def _require(condition, message):
    if not condition:
        raise LineageError("correction binding: " + message)


def _mapping(request):
    return {key: value for key, value in request.items() if key != "start"}


def _only_count_revision(old, new, accepted):
    """Establish this new carry's applicability to one newly corrected row."""
    key = tuple(old), tuple(new)
    if key in accepted:
        return
    _require(len(old) == len(new) == 8, "corrected row lacks its complete producer values")
    _require(
        all(old[position] == new[position] for position in (0, 1, 2, 3, 4, 7)),
        "timestamp, price or VWAP changed",
    )
    _require(
        all(Decimal(row[position]) > 0 for row in (old, new) for position in (5, 6)),
        "trading-state boundary changed",
    )
    accepted.add(key)


def _segment(assignment, previous, generation, name, day, adjustment):
    _require(
        assignment.get("receipt_generation") == generation
        and assignment.get("kind") == "current_month"
        and assignment.get("generation") == previous["boundaries"]["current_month"]["sha256"],
        "accepted assignment generation changed",
    )
    document = assignment["document"]
    _require(
        document["symbol"] == name and document["anchor_month"] == previous["anchor_month"],
        "assignment source changed",
    )
    view = next((view for view in document["adjustments"] if view["adjustment"] == adjustment), None)
    _require(view is not None and bool(view["segments"]), "corrected observation has no accepted assignment")
    segment = view["segments"][-1]
    _require(
        segment["from"] <= day == segment["to"]
        and segment["branch"] == "correlated"
        and segment.get("observation_selection") == "selected"
        and tuple(segment.get("observation_tickers", ())) == (name,)
        and segment.get("correlation_rule", {}).get("name") in _INVARIANT_RULES,
        "recorded corrected-row decision is outside the supported carry",
    )
    return segment


def _no_competitor(previous, name, public_id, day, adjustment):
    for peer, receipt in previous.items():
        if peer == name or public_id not in {
            identity["security_id"] for identity in receipt["identity_memberships"]
        }:
            continue
        summary = receipt["assignment_summary"][adjustment]
        if not summary["rows"] or summary["last_session"] is not None and summary["last_session"] < day:
            continue
        kind = (
            "current_month"
            if day[:7] >= receipt["anchor_month"]
            else "current_year"
            if day[:4] >= receipt["anchor_month"][:4]
            else "completed_years"
        )
        choice = receipt.get("assignment_handoffs", {}).get(kind, {}).get("observation_choices")
        _require(choice is not None, "peer lacks its completed numerical-choice scope")
        partition = choice["partitions"][choice["views"][adjustment]]
        _require(
            not any(run["owner"] == public_id and run["from"] <= day <= run["to"] for run in partition["runs"]),
            "corrected date has a competing observation",
        )


def prepare(store, *, previous, receipt_generations, assignments, sources, obligations, partitions):
    """Return a completed publication plan; serialize it rather than preparing twice.

    ``previous`` contains each complete affected group. ``sources`` names only
    corrected pricing manifests with their accepted generation. ``obligations``
    supplies the durable correction ID and its original comparison selection.
    ``partitions`` is the existing producer-held partition handoff, including its
    explicit (source, boundary kind, boundary generation) keys. Only the final row
    of each view is consumed. Peers and assignment bodies remain unchanged.
    """
    store = str(Path(store).absolute())
    _require(
        bool(sources) and sources.keys() == obligations.keys() == assignments.keys(),
        "correction scope is incomplete",
    )
    _require(
        previous.keys() == receipt_generations.keys() and sources.keys() <= previous.keys(),
        "selected receipt scope is incomplete",
    )
    members = set(previous)
    old_generations = {name: receipt["source_sha256"] for name, receipt in previous.items()}
    for receipt in previous.values():
        _require(set(receipt["input_sources"]) <= members, "complete dependency group required")
        _require(
            all(receipt["input_sources"][name] == old_generations[name] for name in receipt["input_sources"]),
            "accepted group pricing bindings disagree",
        )
        _require(
            not any(receipt.get(key) is not None for key in _VALUE_CAPABILITIES),
            "value-bound capability requires its owning scoped projection",
        )
    current = {**old_generations, **{name: selected["generation"] for name, selected in sources.items()}}
    receipts = {name: {**receipt} for name, receipt in previous.items()}
    changed, accepted = {}, set()
    for name, selected in sources.items():
        old, source, generation = previous[name], selected["source"], selected["generation"]
        obligation = obligations[name]
        contract = obligation["pricing_correction"]
        correction = source.get("latest_session_correction") or {}
        _require(
            contract["store"] == store
            and contract["symbol"] == source["symbol"] == name
            and contract["scope"] == "latest_session"
            and contract["source_sha256"] == old["source_sha256"]
            and contract["selection"]["generation"] == old["source_sha256"]
            and contract["selection"]["comparison"]["decision"] == "latest_session"
            and source.get("history_correction_id") == obligation["correction_id"]
            and generation != old["source_sha256"]
            and correction.get("preserves_timestamps") is True
            and old["source_sha256"]
            in {correction.get("source_sha256"), *correction.get("unchanged_ancestors", ())},
            "completed correction does not bind the accepted source",
        )
        session = correction["session"]
        day = timestamp(session).astimezone(NEW_YORK).date().isoformat()
        request = {**source["latest_request"], "history_start": source.get("history_start")}
        _require(
            source["anchor_month"] == old["anchor_month"] == day[:7]
            and _mapping(request) == _mapping(old["pricing_request"])
            and request["asof"] == contract["asof"]
            and request["end_exclusive"] == old["processed_through"] == contract["end_exclusive"]
            and source.get("acquisition_evidence_sha256") == old.get("acquisition_evidence_sha256"),
            "pricing mapping, coverage or metadata binding changed",
        )
        _require(
            old["row_partition_complete"] is True and old["unresolved_rows"] == 0,
            "accepted corrected-row ownership is incomplete",
        )
        boundary = source["boundaries"]["current_month"]["sha256"]
        views = partitions.get(("pricing_partition", (name, "current_month", boundary)))
        _require(
            views is not None and set(views) == set(ADJUSTMENTS), "completed corrected rows are unavailable"
        )
        endpoints = dict(contract["selection"]["points"])
        _require(set(endpoints) == set(ADJUSTMENTS), "old corrected-row observations are incomplete")
        for adjustment in ADJUSTMENTS:
            _require(bool(views[adjustment]) and bool(endpoints[adjustment]), "corrected row is missing")
            before, after = endpoints[adjustment][-1], views[adjustment][-1]
            _require(
                before[0] == after[0] == session == old["assignment_summary"][adjustment]["last_timestamp"],
                "corrected observation scope changed",
            )
            _only_count_revision(before, after, accepted)
            segment = _segment(assignments[name], old, receipt_generations[name], name, day, adjustment)
            _no_competitor(previous, name, segment["security_identity"]["security_id"], day, adjustment)
        receipts[name] = {
            **old,
            "source_sha256": generation,
            "pricing_boundaries": source["boundaries"],
            "pricing_request": request,
            "processed_through": request["end_exclusive"],
            "history_correction_id": source["history_correction_id"],
        }
        changed[name] = {"session": day, "source_before": old["source_sha256"], "source_after": generation}
    for name, receipt in receipts.items():
        receipt["input_sources"] = {peer: current[peer] for peer in previous[name]["input_sources"]}
    return {
        "boundary": BOUNDARY,
        "version": VERSION,
        "store": store,
        "receipt_generations": dict(receipt_generations),
        "pricing_generations": current,
        "previous": previous,
        "receipts": receipts,
        "boundaries": {name: {} for name in previous},
        "changed": changed,
        "serving_invalidated_sessions": {name: [item["session"]] for name, item in changed.items()},
        "preserved": [
            "dated ownership",
            "identity membership",
            "labels",
            "numerical source choices",
            "assignment bodies",
        ],
    }


def assert_current(plan, store, *, receipt_generations, pricing_generations):
    """Check only named selector applicability, never repeat the completed carry."""
    _require(plan["store"] == str(Path(store).absolute()), "publication store changed")
    for name, generation in plan["receipt_generations"].items():
        _require(receipt_generations.get(name) == generation, "selected lineage generation changed")
    for name, generation in plan["pricing_generations"].items():
        _require(pricing_generations.get(name) == generation, "selected pricing generation changed")
