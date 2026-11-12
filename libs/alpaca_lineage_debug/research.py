"""Explicit candidate replay, once per complete group from one held selector."""

import time
from pathlib import Path

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_policy_lab.comparison import _compare_loaded_group, _load_group
from libs.alpaca_pricing_download.contract import symbol_label

from . import accept_report
from .policy import policy_applies


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


def evaluate_group(
    store, seed, policy, index, group, maximum, control_policy=None, screen_record=None, accepted_store=None
):
    # Keep materialized inputs local to one call, including its exception path.
    # The returned comparison and failure strings retain no pricing objects.
    extra = {} if screen_record is None else {"held_receipts": screen_record["receipts"]}
    if accepted_store is not None:
        extra["accepted_store"] = accepted_store
    with _load_group(store, seed, selection=held_selection(index, group), **extra) as selected:
        if screen_record is not None and "membership_pairs" in screen_record:
            selected["membership_pair_handoff"] = [
                {
                    "source": pair["source"],
                    "donor": pair["donor"],
                    "old": next(
                        item
                        for item in selected["receipts"][pair["source"]]["identity_memberships"]
                        if item["security_id"] == pair["old_public_id"]
                    ),
                    "target": next(
                        item
                        for item in selected["receipts"][pair["donor"]]["identity_memberships"]
                        if item["security_id"] == pair["new_public_id"]
                    ),
                    "old_members": frozenset(pair["old_members"]),
                    "target_members": frozenset(pair["target_members"]),
                }
                for pair in screen_record["membership_pairs"]
            ]
        if control_policy is not None:
            from .paired import evaluate

            return evaluate(store, seed, policy, control_policy, selected, maximum)
        return _compare_loaded_group(store, seed, policy, selected, max_differences=maximum)


def assert_reused_applicable(store, index, result, accepted_store):
    """A real invocation gap checks selected generations, never old prices."""
    from libs.alpaca_lineage_build.contract import document_digest
    from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

    with storage.reader_snapshot(
        store,
        held_selection(index, result["group"]),
        include_receipts=False,
        include_boundaries=False,
        accepted_store=accepted_store,
    ):
        for source, generation in result["baseline"]["pricing_generations"].items():
            root = regular_path(store, f"end_of_day/pricing/{source}", published=True)
            if read_update(root) is not None:
                raise ValueError(f"{source}: completed comparison has pending pricing work")
            current = load_source(root, source)
            if current is None or document_digest(current) != generation:
                raise ValueError(f"{source}: completed comparison selects a changed pricing generation")


def run(
    store,
    policy,
    symbols,
    *,
    all_groups=False,
    max_differences=20,
    timeout_seconds=None,
    control_policy=None,
    screen_report=None,
    reuse_report=None,
):
    started = time.monotonic()
    if screen_report is None:
        store, root, index = select(store)
    else:
        from .screen import accept_screen

        store = Path(store).absolute()
        root, index = accept_screen(screen_report, store, policy, control_policy)
    entries = index["symbols"]
    accepted_store = storage.AcceptedStore(store, root)
    requested = sorted({symbol_label(name) for name in symbols})
    missing = [name for name in requested if name not in entries]
    seeds = sorted(entries) if all_groups else [name for name in requested if name in entries]
    groups = sorted({tuple(entries[name]["group_members"]) for name in seeds})
    screened = (
        {}
        if screen_report is None
        else {
            tuple(item["group"]): item
            for item in screen_report["candidate_groups"] + screen_report["unknown_groups"]
        }
    )
    unaffected = (
        {}
        if screen_report is None
        else {tuple(item["group"]): item for item in screen_report["unaffected_groups"]}
    )
    reused = {}
    if reuse_report is not None:
        accept_report(reuse_report, "run")
        if (
            reuse_report.get("mode") != "paired_same_inputs"
            or reuse_report.get("store") != str(store)
            or not policy_applies(reuse_report.get("candidate_policy", {}), policy)
            or control_policy is None
            or not policy_applies(reuse_report.get("control_policy", {}), control_policy)
            or reuse_report.get("scope", {}).get("selector") != index["projection_state"]
            or type(reuse_report.get("execution_complete")) is not bool
            or reuse_report.get("status") not in {"complete", "incomplete", "timed_out"}
        ):
            raise ValueError("completed paired report has incompatible operation bindings")
        for result in reuse_report["groups"]:
            if any(
                entries.get(source, {}).get("sha256") != generation
                for source, generation in result["baseline"]["receipt_generations"].items()
            ):
                raise ValueError("completed paired report selects a changed receipt generation")
            reused[tuple(result["group"])] = result
    results = []
    proven, carried = [], []
    failures = [
        {"group": [name], "error": [{"type": "MissingLineage", "message": "not indexed"}]} for name in missing
    ]
    timed_out = False
    for group in groups:
        if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
            timed_out = True
            break
        seed = next(name for name in group if name in entries)
        if group in unaffected:
            proven.append(unaffected[group])
            continue
        try:
            if group in reused:
                assert_reused_applicable(store, index, reused[group], accepted_store)
                results.append(reused[group])
                carried.append(list(group))
                continue
            report = evaluate_group(
                store,
                seed,
                policy,
                index,
                group,
                max_differences,
                control_policy,
                screened.get(group),
                accepted_store,
            )
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
    complete = not failures and not timed_out and len(results) + len(proven) == len(groups)
    return {
        "status": "timed_out" if timed_out else "complete" if complete else "incomplete",
        "execution_complete": complete,
        "mode": "paired_same_inputs" if control_policy is not None else "explicit_candidate_replay",
        "store": str(store),
        "candidate_policy": policy.as_report_object(),
        "control_policy": control_policy.as_report_object() if control_policy is not None else None,
        "scope": {
            "selection": "all_indexed_groups" if all_groups else "selected_complete_groups",
            "requested_symbols": requested,
            "selector": index["projection_state"],
            "selected_groups": len(groups),
            "selected_sources": len(set(name for group in groups for name in group)),
            "unevaluated_groups": [
                list(group) for group in groups[len(results) + len(proven) + len(failures) - len(missing) :]
            ],
        },
        "summary": {
            "evaluated_groups": len(results),
            "reused_groups": len(carried),
            "proven_candidate_unaffected_groups": len(proven),
            "failed_groups": len(failures),
            "different_groups": sum(not item["comparison"]["parity"] for item in results),
            "decision_changed_groups": sum(not item["comparison"]["decision_parity"] for item in results),
            "parity": complete and all(item["comparison"]["parity"] for item in results),
            "published_control_drift_groups": sum(
                not item["published_to_control"]["parity"] for item in results if "published_to_control" in item
            ),
            "control_drift_assessed_groups": len(results),
            "control_drift_unassessed_groups": len(proven),
        },
        "groups": results,
        "unaffected_groups": proven,
        "reused_groups": carried,
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
