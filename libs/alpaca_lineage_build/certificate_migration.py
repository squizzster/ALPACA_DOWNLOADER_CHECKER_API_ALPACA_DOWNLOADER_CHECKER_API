"""Exact v76/v77 -> v78 applicability proof and metadata-only migration.

This doorman inspects whether the changed producers can apply. It neither
assigns rows nor predicts a mapping response. Unproved groups use the ordinary
pipeline; unchanged groups preserve their already completed decisions.
"""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.name_change import action_labels

from .acquisition import unavailable_pricing
from .contract import LineageError, reusable_decision, versions
from .correlation_policy_contract import (
    RETAINED_V76_SEMANTIC_SHA256,
    expected_policy_identity,
    valid_policy_provenance,
)
from .metadata import mapping_dependencies, retained_symbol_observations


def retained_version(value):
    previous = value.get("versions", {}).get("correlation")
    if previous not in (76, 77):
        return False
    semantic = RETAINED_V76_SEMANTIC_SHA256 if previous == 76 else versions()["correlation_policy_sha256"]
    expected = {**versions(), "correlation": previous, "correlation_policy_sha256": semantic}
    provenance = value.get("correlation_policy")
    old_policy = {
        **expected_policy_identity(),
        "execution_profile": f"correlation_v{previous}_policy_engine",
        "implementation": {"correlation_version": previous, "lineage_version": 34},
        "semantic_sha256": semantic,
    }
    return (
        versions()["lineage"] == 34
        and versions()["correlation"] == 78
        and value.get("versions") == expected
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == item for key, item in old_policy.items())
    )


def group_evidence(receipts):
    first = next(iter(receipts.values()))
    return [receipt["evidence"] for receipt in receipts.values()] + [
        item["evidence"] for item in unavailable_pricing(first).values() if "evidence" in item
    ]


def _v77_affected_inputs(receipts):
    """Only prerequisites and concrete dependency changes, without price reads."""
    evidence = group_evidence(receipts)
    symbols = retained_symbol_observations(evidence)
    changed_symbols = {name for name, observations in symbols.items() if len(observations) > 1}
    changed_cusips, mappings = set(), {}
    actions, unknown = {}, set()
    for facts in evidence:
        for lookup in facts.get("asset_lookups", ()):
            if lookup["lookup_kind"] != "cusip":
                continue
            cusip = lookup["lookup"]
            mappings.setdefault(cusip, set()).add((lookup.get("asset") or {}).get("id"))
            if any(
                label in symbols and set(symbols[label]) != {baseline}
                for label, baseline in mapping_dependencies(facts, lookup).items()
            ):
                changed_cusips.add(cusip)
        for action in facts.get("corporate_actions", ()):
            actions.setdefault(action.get("id"), []).append(action)
        for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
            unknown.update(item.get("symbol") for item in facts.get(field, ()) if not item.get("asset_id"))
    changed_cusips.update(cusip for cusip, ids in mappings.items() if len(ids) > 1)
    reasons = []
    if changed_symbols or changed_cusips:
        reasons.append("metadata_dependency_changed")
    raw_actions = [action for values in actions.values() for action in values]
    structural = {
        "cash_merger",
        "forward_split",
        "name_change",
        "reorganization",
        "reverse_split",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
        "unit_split",
    }
    for action in raw_actions:
        if action.get("action_type") != "name_change":
            continue
        old, new = action.get("old_symbol"), action.get("new_symbol")
        old_cusip, new_cusip = action.get("old_cusip"), action.get("new_cusip")
        if not old_cusip or not new_cusip or old not in receipts or new not in receipts:
            continue
        if old == new:
            distinct = any(
                left != right
                for left in mappings.get(old_cusip, set()) - {None}
                for right in mappings.get(new_cusip, set()) - {None}
            )
            if old_cusip != new_cusip and distinct and old in unknown:
                reasons.append("same_symbol_unknown_occupant")
            continue
        if old_cusip != new_cusip:
            continue
        if {old, new} & unknown:
            reasons.append("component_unknown_occupant")
        current_labels = {
            (lookup.get("asset") or {}).get("symbol")
            for facts in evidence
            for lookup in facts.get("asset_lookups", ())
            if lookup["lookup_kind"] == "cusip" and lookup["lookup"] == new_cusip
        } & receipts.keys() - {old, new}
        for current in current_labels:
            labels = {old, new, current}
            if labels & unknown or any(
                other.get("id") != action.get("id")
                and other.get("action_type") in structural
                and action_labels(other) & labels
                for other in raw_actions
            ):
                reasons.append("source_bridge_conflicting_evidence")
    return {
        "reasons": sorted(set(reasons)),
        "cusips": sorted(changed_cusips),
        "symbols": sorted(changed_symbols),
    }


def affected_inputs(receipts):
    """Conservative prerequisites, not a second correlation engine.

    A potential applicability hit requires ordinary complete-group evaluation.
    It is never interpreted as proof that publication will change.
    """
    legacy = any(receipt["versions"]["correlation"] == 76 for receipt in receipts.values())
    result = _v77_affected_inputs(receipts) if legacy else {"reasons": [], "cusips": [], "symbols": []}
    evidence = group_evidence(receipts)
    actions = [action for facts in evidence for action in facts.get("corporate_actions", ())]
    renames = [action for action in actions if action.get("action_type") == "name_change"]
    if any(
        edge.get("old_symbol") == edge.get("new_symbol")
        and edge.get("old_cusip") != edge.get("new_cusip")
        and any(
            action.get("action_type") == "stock_merger"
            and action.get("acquirer_symbol") == edge.get("old_symbol")
            for action in actions
        )
        for edge in renames
    ):
        result["reasons"].append("continuing_acquirer_handoff")
    if any(
        action.get("action_type") == "cash_merger"
        and any(action.get("acquiree_symbol") in action_labels(edge) for edge in renames)
        for action in actions
    ):
        result["reasons"].append("terminal_inference_after_closed_occupant")
    if renames and any(
        facts.get("query", {}).get("corporate_actions_data_quality") == "complete"
        and set(facts.get("queried_symbols", ())) - {facts.get("query", {}).get("input_symbol")}
        for facts in evidence
    ):
        result["reasons"].append("completed_related_query_coverage")
    result["reasons"] = sorted(set(result["reasons"]))
    return result


def can_promote_group(receipts, group):
    if not receipts or not all(receipt and retained_version(receipt) for receipt in receipts.values()):
        return False
    first = next(iter(receipts.values()))
    if set(first["input_sources"]) != set(receipts) or set(group) != set(receipts) | set(
        unavailable_pricing(first)
    ):
        return False
    if any(
        receipt["input_sources"] != first["input_sources"]
        or unavailable_pricing(receipt) != unavailable_pricing(first)
        for receipt in receipts.values()
    ):
        return False
    return not affected_inputs(receipts)["reasons"]


def _read_group(root, index, group):
    from . import storage

    return {name: storage.read_receipt(root, name, index) for name in group if name in index["symbols"]}


def preview(store):
    """Declared audit: compact singleton exclusion, then selected metadata only."""
    from . import storage
    from .migration import _selector_groups
    from .projection import require_complete

    store = Path(store).absolute()
    root = store / "end_of_day/lineage"
    result = {
        "affected_groups": [],
        "promotable_groups": 0,
        "promotable_receipts": 0,
        "current_groups": 0,
        "metadata_receipts": 0,
        "candidate_row_views": 0,
    }
    with storage.publication_lock(root):
        index = storage.read_index(root)
        require_complete(index)
        result["selector"] = index["projection_state"]
        for group in _selector_groups(index):
            entries = [index["symbols"][s] for s in group if s in index["symbols"]]
            if all(reusable_decision(entry) for entry in entries):
                result["current_groups"] += 1
                continue
            exact = all(retained_version(entry) for entry in entries)
            compact = exact and len(group) == 1 and len(entries[0]["asset_ids"]) <= 1
            if compact:
                promotable, facts = True, {"reasons": [], "cusips": [], "symbols": []}
            else:
                receipts = _read_group(root, index, group)
                result["metadata_receipts"] += len(receipts)
                facts = affected_inputs(receipts)
                promotable = exact and can_promote_group(receipts, group)
            if promotable:
                result["promotable_groups"] += 1
                result["promotable_receipts"] += len(entries)
            else:
                rows = sum(entry["projection"]["source_rows"] for entry in entries)
                result["candidate_row_views"] += rows
                result["affected_groups"].append({"group": list(group), "row_views": rows, **facts})
    result["affected_symbols"] = sorted({s for item in result["affected_groups"] for s in item["group"]})
    return result


def migrate(store, *, progress=None):
    """Publish exact compatible headers and their durable serving handoff."""
    from libs.alpaca_pricing_download.check_summary import read_snapshot, summary_path
    from libs.alpaca_pricing_download.update_checks import read_checks
    from libs.alpaca_serving_catalogue import work

    from . import storage
    from .correlation_policy import load_authorized_policy
    from .migration import _locked_sources_for_migration, _selector_groups
    from .projection import require_complete

    store = Path(store).absolute()
    promoted, current, affected, failures = 0, 0, [], []
    promoted_receipts = current_receipts = 0
    with storage.writer_snapshot(store) as view:
        recovered = storage.recover(store, view.root)
        checks = read_checks(store, readonly=True)
        if not checks.get("complete") or checks.get("epoch") is None or checks["checks"]:
            raise LineageError("policy migration requires a completed empty lineage update-check queue")
        if summary_path(store).exists():
            read_snapshot(store)
        index, root = view.index(), view.root
        require_complete(index)
        work.upgrade(store)
        policy = load_authorized_policy()
        groups = _selector_groups(index)
        for number, group in enumerate(groups, 1):
            entries = [index["symbols"][s] for s in group if s in index["symbols"]]
            if all(reusable_decision(entry) for entry in entries):
                current += 1
                current_receipts += len(entries)
            elif not all(retained_version(entry) for entry in entries):
                failures.append(
                    {"group": list(group), "reason": "exact version-76/77 to 78 promotion is unavailable"}
                )
            else:
                receipts = _read_group(root, index, group)
                if not can_promote_group(receipts, group):
                    affected.append(list(group))
                else:
                    first = next(iter(receipts.values()))
                    with storage.pricing_locks(store, group, known=receipts) as locked:
                        sources = _locked_sources_for_migration(
                            store, group, first["input_sources"], unavailable_pricing(first), locked
                        )
                        outputs = {
                            name: {**receipt, "versions": versions(), "correlation_policy": policy.provenance()}
                            for name, receipt in receipts.items()
                        }
                        storage.publish(
                            store,
                            root,
                            index,
                            outputs,
                            {name: {} for name in outputs},
                            {},
                            checks=checks,
                            previous=receipts,
                            sources=sources,
                            unchanged_decision=True,
                        )
                    promoted += 1
                    promoted_receipts += len(receipts)
            if progress and (number % 100 == 0 or number == len(groups)):
                progress(number, len(groups))
        pending = work.read(store)
    return {
        "schema_version": 1,
        "boundary": "alpaca_lineage_policy_migration",
        "complete": not failures,
        "failed": len(failures),
        "failed_groups": len(failures),
        "failure_examples": failures[:20],
        "failures_truncated": len(failures) > 20,
        "selected": len(groups),
        "selected_groups": len(groups),
        "selected_receipts": len(index["symbols"]),
        "promoted": promoted,
        "promoted_receipts": promoted_receipts,
        "already_current": current,
        "already_current_groups": current,
        "already_current_receipts": current_receipts,
        "serving_catalogue_action": "pending_update" if pending else "none",
        "serving_pending_symbols": len(pending),
        "policy": policy.provenance(),
        "pricing_writes": 0,
        "recovered_publication": recovered,
        "promoted_groups": promoted,
        "current_groups": current,
        "affected_groups": affected,
        "affected_symbols": sorted({s for group in affected for s in group}),
        "provider_requests": 0,
        "pricing_payload_reads": 0,
        "identity_rule_executions": 0,
    }
