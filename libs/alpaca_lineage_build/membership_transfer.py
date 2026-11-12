"""Publish retained numerical selections after final lineage observation choice."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import LineageError


def carry_prefix(previous, current_receipt, prefix, *, unchanged_selection=False):
    """Carry an unconsumed transfer under an approved unchanged pricing prefix.

    The original serving donor remains the authority across append/rollover;
    today's receipt must not replace its original generations. Current accepted
    assignment metadata locates its positions in the new physical partitions.
    A correction intersecting those positions selects explicit owning recovery.
    No old prices or assignments are reopened to recover a lost decision.
    """
    prior = previous.get("membership_only_rebind")
    if prior is None:
        return None
    if any(run["row_end"] > prefix[run["adjustment"]]["rows"] for run in prior["runs"]):
        raise LineageError("changed membership-transfer donor positions require owning transfer recovery")
    if unchanged_selection and previous["anchor_month"] == current_receipt["anchor_month"]:
        return {**prior, "pricing_generation": current_receipt["source_sha256"]}
    runs = []
    for original in prior["runs"]:
        offset, covered = 0, 0
        adjustment = original["adjustment"]
        for kind in ("completed_years", "current_year", "current_month"):
            handoff = current_receipt["assignment_handoffs"][kind]
            choices = handoff["observation_choices"]
            part = choices["partitions"][choices["views"][adjustment]]
            count = handoff["ownership"][adjustment]["rows"]
            for item in part["runs"]:
                if item["row_start"] > count:
                    continue  # Physical current-year overlap is not logical history.
                first = max(original["row_start"], offset + item["row_start"])
                last = min(original["row_end"], offset + item["row_end"], offset + count)
                if first > last:
                    continue
                if not unchanged_selection and (
                    item["state"] not in {"selected", "conflict", "incomplete"}
                    or item["owner"] != original["new_public_id"]
                    or list(item["tickers"]) != original["labels"]
                ):
                    raise LineageError("changed membership-transfer selection requires owning transfer recovery")
                runs.append(
                    {
                        **original,
                        "boundary": kind,
                        "row_start": first,
                        "row_end": last,
                        "from": max(original["from"], item["from"]),
                        "to": min(original["to"], item["to"]),
                    }
                )
                covered += last - first + 1
            offset += count
        if covered != original["row_end"] - original["row_start"] + 1:
            raise LineageError("missing membership-transfer positions require owning transfer recovery")
    return {**prior, "pricing_generation": current_receipt["source_sha256"], "runs": runs}


def build(
    source,
    *,
    old_identity_memberships,
    old_serving_ownership,
    old_receipt_generation,
    old_pricing_generation,
    old_boundaries,
    current_receipt,
    current_documents,
    preserves_pricing,
    accepted_identity_changes=None,
    allow_label_changes=False,
):
    """Bind only actual surviving old selections to their final public identity.

    Inputs are completed ownership and pricing handoffs. Assignment bodies are
    metadata assembly, never permission to reopen prices or calculate ownership.
    A callable document supplier is opened only for a selected nonempty donor.
    The owning legacy recovery may supply no old boundary map and wrap the result
    in its separately versioned accepted-serving-checkpoint envelope.
    """
    if not preserves_pricing or not old_serving_ownership:
        return None
    changes = dict(accepted_identity_changes or {})
    for identity in old_identity_memberships:
        members = set(identity["member_alpaca_asset_ids"])
        targets = [
            current
            for current in current_receipt["identity_memberships"]
            if members <= set(current["member_alpaca_asset_ids"])
        ]
        if len(targets) == 1 and (targets[0]["security_id"] != identity["security_id"] or allow_label_changes):
            changes[identity["security_id"]] = targets[0]
    old_runs = {}
    for adjustment in ADJUSTMENTS:
        runs, position = [], 0
        for run in old_serving_ownership["views"][adjustment]["ownership"]:
            first, last = position + 1, position + run["rows"]
            old_id, labels = run["owner"]
            if old_id in changes:
                runs.append((first, last, old_id, labels))
            position = last
        old_runs[adjustment] = runs
    if not any(old_runs.values()):
        return None
    documents = current_documents() if callable(current_documents) else current_documents
    transfers, used = [], set()
    offsets = dict.fromkeys(ADJUSTMENTS, 0)
    for kind in ("completed_years", "current_year", "current_month"):
        document = documents[kind]
        for entry in document["adjustments"]:
            adjustment = entry["adjustment"]
            offset = offsets[adjustment]
            for segment in entry["segments"]:
                if (
                    segment["branch"] != "correlated"
                    or segment.get("observation_selection") == "superseded"
                    or kind == "current_year"
                    and segment["from"][:7] >= current_receipt["anchor_month"]
                ):
                    continue
                first, last = offset + segment["row_start"], offset + segment["row_end"]
                labels = segment.get("observation_tickers", [segment["historical_symbol"]])
                new_id = segment["security_identity"]["security_id"]
                for old_first, old_last, old_id, old_labels in old_runs[adjustment]:
                    if (
                        last < old_first
                        or first > old_last
                        or labels != old_labels
                        and not allow_label_changes
                        or changes[old_id]["security_id"] != new_id
                    ):
                        continue
                    if first < old_first or last > old_last:
                        raise LineageError(
                            f"{source}: retained membership transfer needs dated assignment subdivision"
                        )
                    transfer = {
                        "source": source,
                        "adjustment": adjustment,
                        "boundary": kind,
                        "row_start": first,
                        "row_end": last,
                        "from": segment["from"],
                        "to": segment["to"],
                        "old_public_id": old_id,
                        "new_public_id": new_id,
                        "labels": labels,
                    }
                    if allow_label_changes:
                        transfer["old_labels"] = old_labels
                    transfers.append(transfer)
                    used.add(old_id)
            offsets[adjustment] += current_receipt["assignment_handoffs"][kind]["ownership"][adjustment]["rows"]
    if not transfers:
        return None
    return {
        "version": 3 if allow_label_changes else 1,
        "source": source,
        "previous_receipt_generation": old_receipt_generation,
        "previous_pricing_generation": old_pricing_generation,
        "pricing_generation": current_receipt["source_sha256"],
        "previous_boundaries": old_boundaries,
        "identities": {old: changes[old] for old in sorted(used)},
        "runs": transfers,
    }
