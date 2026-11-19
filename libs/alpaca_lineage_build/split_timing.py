"""Carry the split-timing producer's three witnessed sessions across full lineage builds."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import RecoveryRequired
from .resume import continuing_request

_NO_CONTRIBUTION = dict.fromkeys(ADJUSTMENTS, ())


def _partition(receipt, day):
    month = day[:7]
    anchor = receipt["anchor_month"]
    if month > anchor:
        return None
    if month == anchor:
        return "current_month"
    return "current_year" if month[:4] == anchor[:4] else "completed_years"


def _choices(handoff, day, pid):
    """Use the completed per-view contributor choices; empty labels mean this member did not supply the day."""
    choices = handoff.get("observation_choices")
    if choices is None:
        return None
    labels = []
    for part in choices["partitions"]:
        selected = ()
        for run in part["runs"]:
            if run["from"] <= day <= run["to"] and run["owner"] == pid and run["state"] != "superseded":
                if run["state"] not in (None, "selected") or not run["tickers"] or selected:
                    return None
                selected = tuple(run["tickers"])
        labels.append(selected)
    return {view: labels[slot] for view, slot in choices["views"].items()}


def _contribution(receipt, kind, day, pid, legacy):
    if kind is None:
        return _NO_CONTRIBUTION
    handoff = receipt.get("assignment_handoffs", {}).get(kind, {})
    if "observation_choices" in handoff:
        return _choices(handoff, day, pid)
    # Legacy ownership can prove absence across a complete partition. It
    # cannot resolve a dated choice when that identity owns any rows there.
    ownership = handoff.get("ownership")
    if ownership is None:
        return None
    key = id(ownership), pid
    if key not in legacy:
        legacy[key] = (
            _NO_CONTRIBUTION
            if ownership.keys() == _NO_CONTRIBUTION.keys()
            and all(run["owner"][0] != pid for view in ownership.values() for run in view["ownership"])
            else None
        )
    return legacy[key]


def _same_witness(previous, receipt, pid, interval, day, legacy, *, require_absence):
    old_kind, new_kind = _partition(previous, day), _partition(receipt, day)
    if old_kind is None and new_kind is None:
        return True
    old = previous.get("assignment_handoffs", {}).get(old_kind, {})
    new = receipt.get("assignment_handoffs", {}).get(new_kind, {})
    same_assignment = old_kind == new_kind and (
        previous["boundaries"][old_kind] == receipt["boundaries"][new_kind]
        or old.get("comparison")
        and old["comparison"] == new.get("comparison")
    )
    if same_assignment and not require_absence:
        return True
    after = _contribution(receipt, new_kind, day, pid, legacy)
    if after is None or (require_absence and any(after.values())):
        return False
    if same_assignment:
        return True
    before = _contribution(previous, old_kind, day, pid, legacy)
    if before is None and not require_absence and len(interval["members"]) == 1:
        # A completed single-source correction already established this owner
        # and its sole trading label on all three sessions in every view.
        return all(labels == (interval["symbol"],) for labels in after.values())
    return before == after


def require_rebuilt_timing(previous, receipt, source, intervals):
    generation = previous["source_sha256"]
    change_from = None
    if receipt["source_sha256"] != generation:
        # With no applicable ancestry, every witness requires a completed
        # exclusion of this member from its numerical inputs.
        change_from = ""
        if source is not None and continuing_request(source, previous):
            change_from = next(
                (
                    item["changed_from"]
                    for item in source.get("history_changes", ())
                    if item["source_sha256"] == generation
                ),
                change_from,
            )
    legacy = {}
    for pid, item in intervals.items():
        for day in (item["witness_start"], item["first"], item["witness_end"]):
            require_absence = change_from is not None and change_from <= day
            if not _same_witness(previous, receipt, pid, item, day, legacy, require_absence=require_absence):
                reason = (
                    "price history or mapping changed"
                    if require_absence
                    else "ownership or numerical selection changed"
                )
                raise RecoveryRequired(f"split timing {reason}; selected pricing recovery required")
