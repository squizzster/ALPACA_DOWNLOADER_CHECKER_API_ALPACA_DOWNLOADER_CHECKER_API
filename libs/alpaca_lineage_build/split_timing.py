"""Carry the split-timing producer's three witnessed sessions across full lineage builds."""

from .contract import RecoveryRequired
from .resume import continuing_request


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


def _same_witness(previous, receipt, pid, interval, day):
    old_kind, new_kind = _partition(previous, day), _partition(receipt, day)
    if old_kind is None or new_kind is None:
        # Pricing ancestry already established that no row changed on the witness.
        # A partition beyond either member's last month cannot contribute a row.
        return True
    if old_kind == new_kind and previous["boundaries"][old_kind] == receipt["boundaries"][new_kind]:
        return True
    old = previous.get("assignment_handoffs", {}).get(old_kind, {})
    new = receipt.get("assignment_handoffs", {}).get(new_kind, {})
    if old_kind == new_kind and old.get("comparison") and old["comparison"] == new.get("comparison"):
        return True
    after = _choices(new, day, pid)
    if after is None:
        return False
    if "observation_choices" not in old and len(interval["members"]) == 1:
        # A completed single-source correction already established this owner
        # and its sole trading label on all three sessions in every view.
        return all(labels == (interval["symbol"],) for labels in after.values())
    return _choices(old, day, pid) == after


def require_rebuilt_timing(previous, receipt, source, intervals):
    generation = previous["source_sha256"]
    if receipt["source_sha256"] != generation:
        through = max(item["witness_end"] for item in intervals.values())
        if not (
            source is not None
            and continuing_request(source, previous)
            and any(
                change["source_sha256"] == generation
                and (change["changed_from"] is None or change["changed_from"] > through)
                for change in source.get("history_changes", ())
            )
        ):
            raise RecoveryRequired(
                "split timing price history or mapping changed; selected pricing recovery required"
            )
    for pid, item in intervals.items():
        if not all(
            _same_witness(previous, receipt, pid, item, item[key])
            for key in ("witness_start", "first", "witness_end")
        ):
            raise RecoveryRequired(
                "split timing ownership or numerical selection changed; selected pricing recovery required"
            )
