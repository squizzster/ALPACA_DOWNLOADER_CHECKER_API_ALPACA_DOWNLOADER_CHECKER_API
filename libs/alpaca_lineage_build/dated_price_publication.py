"""Select a completed bounded pricing publication under frozen group ownership."""

from datetime import date
from pathlib import Path
from uuid import UUID

from libs.common.store_lock import exclusive_owned

from . import storage
from .contract import LineageError, RecoveryRequired, document_digest

FIELD = "dated_price_intervals"
TOKENS = "dated_price_tokens"


def action_window(evidence, first, last, *, exclude_id=None, symbol=None):
    from libs.alpaca_security_lineage.inference_evidence import provider_actions, reject_assertions

    if "provider_corporate_actions" not in evidence:
        # Legacy providers can supply a plain action list. An authored projection
        # without the separate original observations cannot supply a new decision.
        from libs.alpaca_security_lineage.inference_evidence import ASSERTION_FIELDS

        reject_assertions(
            {key: evidence[key] for key in ASSERTION_FIELDS if key in evidence},
            where="original dated action observations",
        )
    for action in provider_actions(evidence):
        if exclude_id is not None and action.get("id") == exclude_id:
            continue
        if symbol is not None:
            from .metadata import action_symbols

            if symbol not in action_symbols(action):
                continue
        if any(first <= str(value) <= last for name, value in action.items() if name.endswith("_date")):
            reject_assertions(action, where="split timing provider action")
            yield action


def _action_binding_applies(previous, receipt, interval):
    if interval.get("repair_kind") == "retired_unit":
        from .retired_unit import applies

        return applies(previous, receipt, interval)
    if previous.get("evidence") is receipt.get("evidence"):
        return True
    if interval.get("repair_kind") == "former_label_basis":
        from .rename_split_basis import selected_actions

        symbols = {interval["former_symbol"], interval["symbol"]}

        def relevant(evidence):
            return {
                document_digest(a)
                for a in selected_actions(evidence, symbols, interval["first"], interval["cut"])
            }

        return relevant(previous["evidence"]) == relevant(receipt["evidence"])
    if interval.get("repair_kind") == "duplicate_split":
        from .duplicate_split import relevant_actions

        return relevant_actions(previous["evidence"], interval) == relevant_actions(
            receipt["evidence"], interval
        )
    if interval.get("repair_kind") == "rename_split_basis":
        from .rename_split_basis import relevant_actions

        return relevant_actions(previous["evidence"], interval) == relevant_actions(
            receipt["evidence"], interval
        )
    if interval.get("repair_kind") == "carried_rename":
        from .carried_rename import relevant_actions

        return relevant_actions(previous["evidence"], interval) == relevant_actions(
            receipt["evidence"], interval
        )
    if interval.get("repair_kind") not in ("split_timing", "split_carry", "conversion_timing"):
        return True
    from .metadata import action_symbols

    def selected(item):
        return [
            a
            for a in action_window(item["evidence"], interval["witness_start"], interval["witness_end"])
            if interval["symbol"] in action_symbols(a)
        ]

    before = selected(previous)
    after = selected(receipt)
    if len(before) != len(after) or any(a not in before for a in after):
        return False
    if interval.get("repair_kind") == "split_carry":
        from .split_carry import endpoints

        codes = interval["endpoint_cusips"]
        return endpoints(previous["evidence"], codes) == endpoints(receipt["evidence"], codes)
    return True


def continue_closed(previous, receipt, first_changed):
    """A retired member can have no new rows and end before the group's witness."""
    retained = {
        pid: interval
        for pid, interval in previous.get(FIELD, {}).items()
        if first_changed is None
        or interval.get("basis_action_scope", {}).get("through", interval["witness_end"]) < first_changed
    }
    if retained:
        receipt[FIELD] = retained


def retain(previous, receipt, *, continued=False, full_materialization=False, source=None):
    """Numerical selections cannot be introduced by copying or editing metadata."""
    old = (previous or {}).get(FIELD, {})
    candidate = receipt.get(FIELD, {})
    if not (old or candidate):
        return
    same = (
        previous is not None
        and previous["source_sha256"] == receipt["source_sha256"]
        and previous["boundaries"] == receipt["boundaries"]
    )
    retained = old if same else candidate if continued else {}
    if any(interval != old.get(pid) for pid, interval in retained.items()):
        raise LineageError("changed dated price selection requires its owning publication")
    required = {}
    if full_materialization:
        from .retired_unit import require_rebuilt_pricing
        from .split_timing import require_rebuilt_timing

        retired = {
            pid: interval for pid, interval in old.items() if interval.get("repair_kind") == "retired_unit"
        }
        if retired:
            require_rebuilt_pricing(previous, receipt, source, retired.values())
        timing = {
            pid: interval for pid, interval in old.items() if interval.get("repair_kind") == "split_timing"
        }
        if timing and not same:
            require_rebuilt_timing(previous, receipt, source, timing)
        required = {**retired, **timing}
        retained = {**retained, **required}
    if retained:
        retained = {
            pid: interval
            for pid, interval in retained.items()
            if set(interval["members"]) == receipt["input_sources"].keys()
            and set(interval["unavailable"]) == receipt.get("unavailable_pricing", {}).keys()
            and _action_binding_applies(previous, receipt, interval)
        }
    if required.keys() - retained.keys():
        raise RecoveryRequired("dated price binding changed; selected pricing recovery required")
    if retained:
        receipt[FIELD] = dict(retained)
    else:
        receipt.pop(FIELD, None)


def _valid_descriptor(interval, pid):
    from .carried_rename import valid_changes
    from .duplicate_split import valid_scope as valid_duplicate_scope
    from .rename_split_basis import valid_scope
    from .retired_unit import valid_scope as valid_retired_scope

    try:
        token = interval["token"]
        dates = [interval[name] for name in ("first", "cut", "witness_end")]
        return (
            type(interval["schema_version"]) is int
            and interval["schema_version"] == 1
            and (
                "repair_kind" not in interval
                or interval["repair_kind"]
                in (
                    "split_timing",
                    "split_carry",
                    "conversion_timing",
                    "retired_unit",
                    "duplicate_split",
                    "carried_rename",
                    "same_label_basis",
                    "rename_split_basis",
                    "former_label_basis",
                )
            )
            and (
                interval.get("repair_kind") != "former_label_basis"
                or (
                    isinstance(interval["symbol"], str)
                    and isinstance(interval["former_symbol"], str)
                    and interval["symbol"] != interval["former_symbol"]
                    and {interval["former_symbol"], interval["symbol"]} <= set(interval["members"])
                )
            )
            and valid_retired_scope(interval)
            and valid_duplicate_scope(interval)
            and valid_changes(interval)
            and valid_scope(interval)
            and len(pid) == 64
            and all(c in "0123456789abcdef" for c in pid)
            and interval["public_id"] == pid
            and UUID(hex=token).hex == token
            and isinstance(interval["source_decision"], str)
            and bool(interval["source_decision"])
            and all(date.fromisoformat(day).isoformat() == day for day in dates)
            and dates[0] < dates[1] <= dates[2]
            and (
                interval.get("repair_kind") not in ("split_timing", "split_carry", "conversion_timing")
                or (
                    isinstance(interval["symbol"], str)
                    and interval["symbol"]
                    and date.fromisoformat(interval["witness_start"]).isoformat() == interval["witness_start"]
                    and interval["witness_start"] < interval["first"]
                )
            )
            and (
                interval.get("repair_kind") != "split_carry"
                or (
                    isinstance(interval["endpoint_cusips"], list)
                    and len(interval["endpoint_cusips"]) == len(set(interval["endpoint_cusips"])) == 2
                    and all(isinstance(code, str) and len(code) == 9 for code in interval["endpoint_cusips"])
                    and interval["cut"] == interval["witness_end"]
                )
            )
            and interval["members"]
            and interval["members"] == sorted(set(interval["members"]))
            and interval["unavailable"] == sorted(set(interval["unavailable"]))
            and all(isinstance(name, str) and name for name in (*interval["members"], *interval["unavailable"]))
            and not set(interval["members"]).intersection(interval["unavailable"])
        )
    except (TypeError, ValueError, AttributeError, KeyError):
        return False


def accept(receipt, entry):
    """Accept the descriptor's own schema and, when available, its index selection."""
    intervals = receipt.get(FIELD, {})
    if not isinstance(intervals, dict) or any(
        not _valid_descriptor(interval, pid) or receipt["symbol"] not in interval["members"]
        for pid, interval in intervals.items()
    ):
        raise RecoveryRequired("invalid selected dated price interval")
    if TOKENS in entry and entry[TOKENS] != {pid: v["token"] for pid, v in intervals.items()}:
        raise RecoveryRequired("missing or changed declared dated price selection")


def bound_descriptor(group, descriptor):
    references = group["references"]
    members = sorted(references["pricing"])
    unavailable = sorted(set(references["lineage"]) - set(members))
    result = {
        **{
            name: descriptor.get(name)
            for name in ("public_id", "token", "source_decision", "first", "cut", "witness_end")
        },
        **(
            {"former_symbol": descriptor["former_symbol"], "symbol": descriptor["symbol"]}
            if descriptor.get("repair_kind") == "former_label_basis"
            else {}
        ),
        **({"repair_kind": descriptor["repair_kind"]} if "repair_kind" in descriptor else {}),
        **(
            {"symbol": descriptor["symbol"], "witness_start": descriptor["witnesses"][0]}
            if descriptor.get("repair_kind") in ("split_timing", "split_carry", "conversion_timing")
            else {}
        ),
        **(
            {"endpoint_cusips": descriptor["endpoint_cusips"]}
            if descriptor.get("repair_kind") == "split_carry"
            else {}
        ),
        **(
            {"basis_action_scope": descriptor["basis_action_scope"]}
            if "basis_action_scope" in descriptor
            else {}
        ),
        **(
            {"duplicate_scope": descriptor["duplicate_scope"]}
            if descriptor.get("repair_kind") == "duplicate_split"
            else {}
        ),
        **(
            {"retired_unit_scope": descriptor["retired_unit_scope"]}
            if descriptor.get("repair_kind") == "retired_unit"
            else {}
        ),
        **({"label_changes": descriptor["label_changes"]} if "label_changes" in descriptor else {}),
        "schema_version": 1,
        "members": members,
        "unavailable": unavailable,
    }
    if (
        not _valid_descriptor(result, result.get("public_id"))
        or descriptor.get("source_decision") != group["decision_generation"]
        or any(references["lineage"][s] is not None for s in unavailable)
    ):
        raise LineageError("invalid dated price publication binding")
    return result


def publish(store, group, descriptor, *, previous=None, pending_lineage=None):
    """Publish only receipt pointers; the pricing owner already committed the selected rows."""
    interval = bound_descriptor(group, descriptor)
    return publish_selection(
        store,
        group,
        interval,
        interval["public_id"],
        FIELD,
        TOKENS,
        previous=previous,
        pending_lineage=pending_lineage,
    )


def publish_selection(store, group, interval, key, field, tokens, *, previous=None, pending_lineage=None):
    """Select a completed numerical or coverage decision under the same atomic group journal."""
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise LineageError("dated price publication requires the exclusive store operation")
    token = interval["token"]
    refs = group["references"]
    pending_lineage = pending_lineage or {}
    with storage.writer_lock(store) as root:
        pending = storage.path_at(root, ".pending.json")
        if pending.exists():
            journal = storage.read_document(pending)
            if journal.get("dated_price_publication") != token:
                raise RecoveryRequired("another lineage publication requires owning recovery")
            if any(
                journal["export_input_changes"].get(s) != generation for s, generation in pending_lineage.items()
            ):
                raise RecoveryRequired("another lineage publication changed the dated price selection")
            storage.recover_journal(store, root, journal)
            return journal["export_input_changes"]
        with storage.pricing_locks(store, refs["lineage"], known=refs["pricing"]):
            index = storage.read_index(root, group["locator"], group=True)
            entries = index["symbols"]
            if (
                entries.keys() != refs["pricing"].keys()
                or any(set(entry["group_members"]) != refs["lineage"].keys() for entry in entries.values())
                or any(entry["source_sha256"] != refs["pricing"][s] for s, entry in entries.items())
            ):
                raise RecoveryRequired("dated price group or source changed before publication")
            if all(entry.get(tokens, {}).get(key) == token for entry in entries.values()):
                if any(
                    entries.get(s, {}).get("sha256") != generation for s, generation in pending_lineage.items()
                ):
                    raise RecoveryRequired("another lineage publication changed the dated price selection")
                return {s: e["sha256"] for s, e in entries.items()}
            if pending_lineage:
                raise RecoveryRequired("another lineage publication changed the dated price selection")
            if any(entry["sha256"] != refs["lineage"][s] for s, entry in entries.items()):
                raise RecoveryRequired("dated price lineage selection changed before publication")
            if previous is not None and previous.keys() != entries.keys():
                raise LineageError("incomplete held dated price publication group")
            documents, updates = {}, {}
            for name, entry in entries.items():
                prior = previous[name] if previous is not None else storage.read_receipt(root, name, index)
                receipt = {**prior, field: {**prior.get(field, {}), key: interval}}
                updates[name] = {
                    **entry,
                    "sha256": document_digest(receipt),
                    tokens: {k: v["token"] for k, v in receipt[field].items()},
                }
                documents[f"{name}/lineage.manifest.json"] = receipt
            changed = {s: entry["sha256"] for s, entry in updates.items()}
            journal = {
                "schema_version": storage.VERSION,
                "boundary": "alpaca_lineage_transaction",
                "dated_price_publication": token,
                "correlation_policy": entries[group["locator"]]["correlation_policy"],
                "index_updates": updates,
                "documents": documents,
                "retire": [],
                "sources": refs["pricing"],
                "unavailable_pricing": interval["unavailable"],
                "notices": {},
                "export_input_changes": changed,
            }
            storage._validate_journal_policy(journal)
            with storage.publication_lock(root, exclusive=True):
                storage.atomic_json(pending, journal, compare=False)
                storage.finish_publication(store, root, journal, receipts={})
            return changed
