"""Explicit candidate replay, once per complete group from one held selector."""

import time
from pathlib import Path

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_policy_lab.comparison import _compare_loaded_group, _load_group
from libs.alpaca_pricing_download.contract import symbol_label


def failure(error):
    causes = []
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        causes.append({"type": type(error).__name__, "message": str(error)})
        error = error.__cause__
    return causes


def select(store):
    store = Path(store).absolute()
    storage.check_store(store)
    root = store / "end_of_day/lineage"
    with storage.publication_lock(root):
        if storage.path_at(root, ".pending.json").exists():
            raise ValueError("lineage publication is incomplete; no diagnostic recovery is performed")
        index = storage.read_index(root)
    if index.get("projection_state", {}).get("complete") is not True:
        raise ValueError("a completed lineage selector is required")
    return store, root, index


def held_selection(index, group):
    def select_group(root):
        storage.assert_index_current(root, index)
        return storage.SelectedIndex(
            {name: index["symbols"].get(name) for name in group}, root, index["projection_state"]
        )

    return select_group


def evaluate_group(store, seed, policy, index, group, maximum):
    # Keep materialized inputs local to one call, including its exception path.
    # The returned comparison and failure strings retain no pricing objects.
    with _load_group(store, seed, selection=held_selection(index, group)) as selected:
        return _compare_loaded_group(store, seed, policy, selected, max_differences=maximum)


def run(store, policy, symbols, *, all_groups=False, max_differences=20, timeout_seconds=None):
    started = time.monotonic()
    store, root, index = select(store)
    entries = index["symbols"]
    requested = sorted({symbol_label(name) for name in symbols})
    missing = [name for name in requested if name not in entries]
    seeds = sorted(entries) if all_groups else [name for name in requested if name in entries]
    groups = sorted({tuple(entries[name]["group_members"]) for name in seeds})
    results = []
    failures = [
        {"group": [name], "error": [{"type": "MissingLineage", "message": "not indexed"}]} for name in missing
    ]
    timed_out = False
    for group in groups:
        if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
            timed_out = True
            break
        seed = next(name for name in group if name in entries)
        try:
            report = evaluate_group(store, seed, policy, index, group, max_differences)
            # Each group releases its materialized histories here. Only compact
            # comparison/trace records survive; no unpublished handoff is saved.
            results.append(report)
        except (RuntimeError, ValueError, OSError) as error:
            failures.append({"group": list(group), "error": failure(error)})
    if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
        timed_out = True
    # Earlier results belong to this exact selector. A genuine changed selector
    # invalidates the operation, without reselecting or replaying a completed group.
    with storage.publication_lock(root):
        storage.assert_index_current(root, index)
    complete = not failures and not timed_out and len(results) == len(groups)
    return {
        "status": "timed_out" if timed_out else "complete" if complete else "incomplete",
        "execution_complete": complete,
        "mode": "explicit_candidate_replay",
        "candidate_policy": policy.as_report_object(),
        "scope": {
            "selection": "all_indexed_groups" if all_groups else "selected_complete_groups",
            "requested_symbols": requested,
            "selector": index["projection_state"],
            "selected_groups": len(groups),
            "selected_sources": len(set(name for group in groups for name in group)),
        },
        "summary": {
            "evaluated_groups": len(results),
            "failed_groups": len(failures),
            "different_groups": sum(not item["comparison"]["parity"] for item in results),
            "decision_changed_groups": sum(not item["comparison"]["decision_parity"] for item in results),
            "parity": complete and all(item["comparison"]["parity"] for item in results),
        },
        "groups": results,
        "failures": failures,
        "elapsed_seconds": time.monotonic() - started,
        "timeout": {
            "seconds": timeout_seconds,
            "enforcement": "between complete groups; an active group is allowed to finish",
        },
        "limits": [
            "Comparison uses published assignments as baseline data, never as candidate evidence.",
            "Candidate replay is new research, not upstream authentication or production approval.",
            "Unrecorded predicate failure reasons in older publications cannot be recovered by replay.",
        ],
    }
