"""Consume completed correction decisions within their accepted request scope."""


def applicable(
    item, *, store, entry, generation, live_all, changed_sessions=None, added_sessions=(), invalidated=False
):
    """Bind new observations to an already completed full-range disposition.

    A correction whose input and output generations are identical has already
    resolved this request-context contradiction. Identical new selected witnesses
    select that disposition; a new workflow attempt cannot revoke it. This is an
    applicability comparison of new observations, not a replay of the producer's
    historical acquisition or logical-digest calculations.
    """
    if not item or invalidated:
        return False
    contract = item["pricing_correction"]
    completed = item["correction_completion"]
    if (
        contract.get("schema_version") != 3
        or contract.get("scope") != "full_history"
        or contract.get("cause") != "selected_all_rows_changed"
        or contract.get("store") != str(store)
        or contract.get("symbol") != entry["symbol"]
        or not generation
        or completed.get("source_sha256") != generation
        or completed.get("correction_id") != item.get("correction_id")
        or added_sessions
    ):
        return False
    old_end = contract.get("comparison_end_exclusive", contract.get("end_exclusive"))
    old_asof = contract.get("comparison_asof", contract.get("asof"))
    if old_end == entry["end_exclusive"]:
        if old_asof != entry["asof"]:
            return False
    elif not old_end or old_end > entry["end_exclusive"]:
        return False
    old = {row[0]: tuple(row) for row in contract.get("observations", {}).get("all", ())}
    if changed_sessions is None:
        return tuple(old.values()) == tuple(tuple(row) for row in live_all)
    if not changed_sessions:
        return False
    current = {row[0]: tuple(row) for row in live_all}
    return all(session < old_end and old.get(session) == current.get(session) for session in changed_sessions)


def covers_selected_advance(contract, *, store, entry, publication, target, invalidated):
    """The completed replacement can finish the exact advance that selected it."""
    if (
        invalidated
        or not contract
        or contract.get("advance_end_exclusive") != target
        or contract.get("scope") != "full_history"
        or contract.get("cause") != "selected_all_rows_changed"
        or (contract.get("store"), contract.get("symbol")) != (str(store), entry["symbol"])
        or contract.get("start") != publication["history_start"]
        or contract.get("end_exclusive") != publication["request"]["end_exclusive"]
        or contract.get("asof") != publication["request"]["asof"]
        or contract.get("comparison_end_exclusive") != contract["end_exclusive"]
        or contract.get("comparison_asof") != contract["asof"]
        or (entry["end_exclusive"], entry["asof"]) != (contract["end_exclusive"], contract["asof"])
        or entry.get("source_sha256") != publication["source_sha256"]
    ):
        return False
    # The pricing selector already established the exact correction ID. An
    # inherited ID on a descendant does not approve that descendant's prices.
    if publication.get("correction_completion"):
        return publication["source_sha256"] == contract["source_sha256"]
    ancestry = publication.get("history_changes")
    return bool(ancestry) and ancestry[0]["source_sha256"] == contract["source_sha256"]
