"""Recover missing membership handoffs in an already selected legacy component."""

from libs.alpaca_end_of_day_security_correlation.component_refinement import membership_pairs
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges

from .contract import RecoveryRequired

FIELD = "legacy_membership_screen_v94"


def select_legacy_groups(store, index, serving, *, changed_rules, publish=False):
    """Schedule adoption of the v94 membership producer from completed, group-bound metadata once."""
    from libs.alpaca_serving_catalogue.membership_selection import read_legacy_overlaps

    from .storage import publish_membership_screens

    entries = index["symbols"]
    unknown, selected, pending = {}, {}, {}
    for key, group in serving["groups"].items():
        refs = group["references"]["lineage"]
        if group["online_identities"] + group["ineligible_identities"] < 2 or len(refs) < 2:
            continue
        stored = [s for s in refs if s in entries]
        if (
            len(stored) < 2
            or any(s in changed_rules for s in stored)
            or not any(entries[s]["versions"]["correlation"] < 94 for s in stored)
        ):
            continue
        if any((entries.get(s) or {}).get("sha256") != generation for s, generation in refs.items()):
            continue  # A pending publication will supply its own dated selection.
        seed = min(stored)
        prior = entries[seed].get(FIELD, {})
        if prior.get("lineage") == refs and (
            prior.get("assessed") or prior.get("serving") == group["decision_generation"]
        ):
            if prior.get("overlap") and not prior.get("assessed"):
                selected[seed] = refs
        else:
            unknown[key] = group
            pending[key] = seed
    if unknown:
        updates = {}
        for key, positive in read_legacy_overlaps(store, unknown).items():
            group, seed = unknown[key], pending[key]
            refs = group["references"]["lineage"]
            updates[seed] = {"lineage": refs, "serving": group["decision_generation"], "overlap": positive}
            if positive:
                selected[seed] = refs
        if publish:
            publish_membership_screens(store / "end_of_day/lineage", index, updates)
    return selected


def _binding(previous, entries):
    seed = min(previous)
    names = entries[seed].get("group_members") or sorted(previous)
    return seed, {s: (entries.get(s) or {}).get("sha256") for s in names}


def complete_negative(root, index, previous, reason, issues=()):
    """A completed assessment updates scheduling alone; the public decisions still apply."""
    from .storage import publish_membership_screens

    seed, refs = _binding(previous, index["symbols"])
    done = {"lineage": refs, "assessed": reason, "issues": list(issues)}
    publish_membership_screens(root, index, {seed: done})


def selected_pairs(previous, pricing, policy, cache, index_entries):
    """Screen held receipts once. An unchanged current publication needs no assessment."""
    if (
        policy.implementation_correlation_version < 94
        or len(previous) < 2
        or any(not item or item.get("input_sources") != pricing for item in previous.values())
        or all(
            item["versions"]["correlation"] >= 94 or item.get("retained_membership_assessed") == pricing
            for item in previous.values()
        )
    ):
        return None
    seed, refs = _binding(previous, index_entries)
    assessed = index_entries[seed].get(FIELD, {})
    if assessed.get("lineage") == refs and assessed.get("assessed"):
        return None
    key = tuple((name, index_entries[name]["sha256"]) for name in sorted(previous))
    selections = cache.setdefault("legacy_membership_pairs", {})
    if key not in selections:
        selections[key] = membership_pairs(previous)
    return selections[key]


def donor_edges(evidence, symbol, cache, *, receipt_generation, accepted_issues):
    """Recover the final edges from metadata when chronology needs no historical witness.

    The policy's continuity step filters cross-UUID name changes. That step can
    depend on source history. Such a donor needs the full selected group owner.
    Same-UUID aliases and identity-preserving splits have no such dependency.
    """
    key = symbol, receipt_generation, id(evidence)
    results = cache.setdefault("legacy_membership_edges", {})
    if key not in results:
        # The normal edge owner already resolves metadata-only splits, mergers,
        # alias edges and superseded actions. Only an uncertified cross-UUID
        # rename can add another edge after the historical continuity phase.
        edges, issues = build_uuid_lineage_edges(evidence)
        missing = {
            issue["corporate_action_id"]
            for issue in issues
            if issue.get("reason_code") == "name_change_requires_independent_continuity"
        }
        if missing:
            missing.difference_update(
                issue.get("corporate_action_id")
                for issue in accepted_issues
                if issue.get("reason_code") == "name_change_requires_independent_continuity"
            )
        if missing:
            raise RecoveryRequired(
                f"{symbol}: legacy membership edges require complete-group retained lineage recovery"
            )
        results[key] = evidence, [edge.as_report_object() for edge in edges]
    return results[key][1]
