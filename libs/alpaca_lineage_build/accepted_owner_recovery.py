"""Restore selected owner positions from their completed serving checkpoint."""

from dataclasses import replace

from alpaca_end_of_day._history_coverage import coverage_sources
from libs.alpaca_end_of_day_security_correlation.action_identity import (
    exact_cusip_lookup_asset,
    exact_symbol_lookup_asset,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.alpaca_serving_catalogue import storage as catalogue
from libs.alpaca_serving_catalogue.offline import REASON

from . import storage
from .assignment_handoff import produce, serving_ownership
from .contract import LineageError, document_digest
from .prepared_recovery import PreparedCandidate, publish_prepared
from .recovery_handoffs import remember

COLD = "completed_years"
LOSS = "outside_supported_name_change_history"
RULE = "retained_same_source_accepted_owner"


def approved_contributors(coverage, sources):
    """Read the source membership carried by either completed checkpoint format."""
    return set(coverage) == set(ADJUSTMENTS) and all(
        coverage_sources(view) == sources for view in coverage.values()
    )


def approved_identity(group, offline):
    envelope = offline.get("offline_ownership", {})
    checkpoints = envelope.get("checkpoints", ())
    if (
        offline["references"] != {"lineage": group.receipts, "pricing": group.pricing}
        or envelope.get("reason") != REASON
        or len(envelope.get("public_ids", ())) != 1
        or len(checkpoints) != 1
    ):
        raise LineageError("selected accepted owner has no applicable offline checkpoint")
    pid = envelope["public_ids"][0]
    prior = checkpoints[0]
    history = prior["history_validation"]
    records = history["prefix_records"]
    inputs = history["inputs"]
    if (
        prior["policy"] != offline["policy"]
        or len(prior["decisions"]) != 1
        or prior["decisions"][0]["public_id"] != pid
        or prior["decisions"][0]["eligible"] is not True
        or len(records) != 1
        or records[0]["public_id"] != pid
        or inputs.keys() != group.receipts.keys()
        or prior["references"]["pricing"] != {name: item["pricing_generation"] for name, item in inputs.items()}
    ):
        raise LineageError("selected accepted owner has no applicable approved checkpoint")
    return pid, prior


def _accepted_source_checkpoint(group, offline, source):
    name = group.seed
    pid, prior = approved_identity(group, offline)
    history = prior["history_validation"]
    inputs = history["inputs"]
    record = history["prefix_records"][0]
    first, last = record["first_session"], record["validated_through"]
    old_generation = inputs[name]["pricing_generation"]
    if old_generation != group.pricing[name] and not any(
        item["source_sha256"] == old_generation and (item["changed_from"] is None or item["changed_from"] > last)
        for item in source.get("history_changes", ())
    ):
        raise LineageError("selected pricing ancestry does not retain the approved owner positions")
    size = record["coverage"]["session_count"]
    owner = [pid, [name]]
    if (
        record["last_session"] != last
        or record["ticker_ranges"] != [{"ticker": name, "first_session": first, "last_session": last}]
        or record["price_reasons"]
        or record["missing_sessions"]
        or not approved_contributors(history["identities"][pid], {name})
        or inputs[name]["views"]
        != {
            view: {"rows": size, "last_session": last, "ownership": [{"rows": size, "owner": owner}]}
            for view in ADJUSTMENTS
        }
    ):
        raise LineageError("accepted owner does not cover a clean all-view source and label grid")

    return pid, owner, prior, size


def _cold_prefix(group, losses, approved):
    pid, _, prior, size = approved
    name = group.seed
    record = prior["history_validation"]["prefix_records"][0]
    first, last = record["first_session"], record["validated_through"]
    old_generation = prior["references"]["pricing"][name]
    extents = []
    for loss in sorted(losses, key=lambda item: item["row_start"]):
        if (
            loss["source"] != name
            or loss["public_id"] != pid
            or loss["boundary"] != COLD
            or loss["previous_pricing_generation"] != old_generation
            or loss["pricing_generation"] != group.pricing[name]
            or loss["row_start"] != (extents[-1][1] + 1 if extents else 1)
            or loss["row_start"] > loss["row_end"]
            or loss["row_end"] >= size
            or not first <= loss["segment_from"] <= loss["segment_to"] < last
        ):
            raise LineageError("accepted owner loss is outside the selected contiguous cold prefix")
        extents.append((loss["row_start"], loss["row_end"], loss["segment_from"], loss["segment_to"]))
    if not extents or extents[0][2] != first:
        raise LineageError("accepted owner loss lacks the original first position")
    return tuple(extents)


def _require_current_owner(previous, pid, owner, stop, accepted_size):
    members = previous["identity_memberships"]
    name = owner[1][0]
    views = previous["serving_ownership"]["views"]
    if (
        len(members) != 1
        or members[0]["security_id"] != pid
        or len(members[0]["member_alpaca_asset_ids"]) != 1
        or set(previous["asset_ids"]) != set(members[0]["member_alpaca_asset_ids"])
        or set(views) != set(ADJUSTMENTS)
        or any(
            view["rows"] < accepted_size
            or view["ownership"]
            != [
                {"rows": stop, "owner": [None, None]},
                {"rows": view["rows"] - stop, "owner": owner},
            ]
            for view in views.values()
        )
    ):
        raise LineageError("current ownership differs from the retained same-source identity and label")
    evidence = provider_evidence(previous["evidence"])
    actions = [action for action in evidence["corporate_actions"] if action.get("action_type") == "name_change"]
    if len(actions) != 1 or actions[0].get("new_symbol") != name:
        raise LineageError("selected ownership loss lacks one continuing same-class name change")
    action = actions[0]
    cusip = action.get("old_cusip")
    if not cusip or cusip != action.get("new_cusip"):
        raise LineageError("retained name change does not retain its security identifier")
    asset_id = members[0]["member_alpaca_asset_ids"][0]
    old_label = action.get("old_symbol")
    # The acquisition producer also projects the name change's own endpoint
    # into occupancy evidence. That projection adds no independent old-label
    # observation. An exact old lookup or other occupancy provenance does.
    action_endpoint = {
        "symbol": old_label,
        "cusip": cusip,
        "asset_id": asset_id,
        "evidence": [
            {
                "symbol": old_label,
                "cusip": cusip,
                "role": "old",
                "action_type": "name_change",
                "action_id": action.get("id"),
                "process_date": action.get("process_date"),
            }
        ],
    }
    if (
        not old_label
        or old_label == name
        or exact_symbol_lookup_asset(evidence, old_label) is not None
        or any(
            item.get("symbol") == old_label and item != action_endpoint
            for field in ("input_symbol_occupants", "symbol_occupancy_evidence")
            for item in evidence.get(field, ())
        )
    ):
        raise LineageError("retired label has evidence requiring its own dated assessment")
    for asset in (exact_symbol_lookup_asset(evidence, name), exact_cusip_lookup_asset(evidence, cusip)):
        if asset is None or asset.get("id") != asset_id or asset.get("symbol") != name:
            raise LineageError("retained name change endpoints do not establish the surviving own-label asset")


def _restore_prefix(document, pid, name, extents, *, interval=None, rule=RULE):
    entries, restored_views = [], {}
    for entry in document["adjustments"]:
        segments = entry["segments"]
        if id(segments) in restored_views:
            entries.append({**entry, "segments": restored_views[id(segments)]})
            continue
        prefix = segments[: len(extents)]
        suffix = segments[len(extents) :]
        if (
            not suffix
            or suffix[0]["row_start"] != extents[-1][1] + 1
            or suffix[0].get("historical_symbol") != name
            or suffix[0].get("security_identity", {}).get("security_id") != pid
            or not suffix[0].get("canonical_security")
            or tuple((seg["row_start"], seg["row_end"], seg["from"], seg["to"]) for seg in prefix) != extents
            or any(
                seg["branch"] != "uncorrelated"
                or seg.get("reason_codes") != [LOSS]
                or seg.get("candidate_securities")
                for seg in prefix
            )
        ):
            raise LineageError("an adjustment view differs from the selected candidate-free ownership loss")
        if interval is not None:
            following = suffix[0]
            canonical = following["canonical_security"]
            if (
                following["from"] != interval["end_date_exclusive"]
                or canonical.get("alpaca_asset_id") != interval["asset_id"]
                or canonical.get("cusip") != interval["cusip"]
                or following.get("observation_selection") != "selected"
                or following.get("observation_tickers") != [name]
            ):
                raise LineageError("the first surviving observation differs from the native successor")
        restored = [
            {
                **{
                    key: seg[key]
                    for key in (
                        "row_start",
                        "row_end",
                        "row_count",
                        "from",
                        "to",
                        "first_timestamp",
                        "last_timestamp",
                    )
                },
                "branch": "correlated",
                "historical_symbol": name,
                "canonical_security": suffix[0]["canonical_security"],
                "security_identity": suffix[0]["security_identity"],
                "identity_confidence": rule,
                "correlation_method": rule,
                "correlation_rule": {"order": None, "name": rule},
                "ticker_ownership_interval": interval,
                "observation_selection": "selected",
                "observation_tickers": [name],
            }
            for seg in prefix
        ]
        restored_views[id(segments)] = [*restored, *suffix]
        entries.append({**entry, "segments": restored_views[id(segments)]})
    return {**document, "adjustments": entries}


def restore_positions(previous, document, pid, extents, prior):
    generation = document_digest(document)
    stop = extents[-1][1]
    restored_rows = stop * len(ADJUSTMENTS)
    if previous["unresolved_rows"] != restored_rows:
        raise LineageError("selected accepted owner does not exhaust the reported unresolved prefix")
    receipt = {
        **previous,
        "correlated_rows": previous["correlated_rows"] + restored_rows,
        "unresolved_rows": 0,
        "boundaries": {
            **previous["boundaries"],
            COLD: {**previous["boundaries"][COLD], "sha256": generation},
        },
        "assignment_handoffs": {**previous["assignment_handoffs"], COLD: produce(document, generation)},
        "identity_intervals": [
            *({"from": left, "to": right, "security_id": pid} for _, _, left, right in extents),
            *previous["identity_intervals"],
        ],
        "retained_owner_restoration": {
            "version": 1,
            "decision_generation": prior["decision_generation"],
            "references": prior["references"],
            "boundary": COLD,
            "public_id": pid,
            "row_start": 1,
            "row_end": stop,
            "former_reason": LOSS,
        },
    }
    receipt["serving_ownership"] = serving_ownership(receipt)
    return receipt


def _prepare_single(context, root, group, offline, source, index, policy):
    from libs.alpaca_end_of_day_security_correlation.due_bill_cash import VERSION as DUE_BILL_CASH_VERSION

    approved = _accepted_source_checkpoint(group, offline, source)
    losses = offline["offline_ownership"]["outcome"]["lost_scopes"]
    if losses and min(loss["row_start"] for loss in losses) > 1:
        from .retained_cash_distribution import prepare

        if policy.implementation_correlation_version < DUE_BILL_CASH_VERSION:
            raise LineageError("selected policy does not support retained cash-distribution ownership")
        previous = storage.read_receipt(root, group.seed, index)
        return prepare(
            context, root, group, source, previous, losses, approved, group_policy=policy.semantic_sha256
        )
    pid, owner, prior, size = approved
    extents = _cold_prefix(group, losses, approved)
    name = group.seed
    previous = storage.read_receipt(root, name, index)
    _require_current_owner(previous, pid, owner, extents[-1][1], size)
    old = storage.read_boundaries(root, previous, kinds={COLD})[COLD]
    document = _restore_prefix(old, pid, name, extents)
    receipt = restore_positions(previous, document, pid, extents, prior)
    return PreparedCandidate(
        {name: receipt},
        {name: {COLD: document}},
        {name: previous},
        previous["correlation_policy"],
        {name: {COLD: document}},
    )


def restore(selection, context, *, progress=None):
    """Publish the exact accepted positions while retaining their owning locks."""
    from .pipeline import _current_sources

    store = selection.store
    group = selection.groups[0]
    name = group.seed
    with storage.writer_lock(store, accepted_store=context.accept_store()) as root:
        recovered = context.recover_lineage(root)
        policy = context.accept_correlation_policy()
        if policy.semantic_sha256 != selection.policy_sha256:
            raise LineageError("accepted-owner restoration policy changed from selected scope")
        with storage.publication_lock(root):
            index = context.select_index(root)
        pending, completed = selection.select(index)
        if not pending:
            return {
                "failed": 0,
                "recovered": recovered,
                "previously_completed_groups": len(completed),
                "published_prepared_groups": 0,
                "recomputed_groups": 0,
            }
        selected = context.select_serving()
        locator = selected["members"].get(name)
        if locator is None or any(selected["members"].get(symbol) != locator for symbol in group.members):
            raise LineageError("selected accepted owner has no serving group")
        serving_generation = selected["state"]["generation"]
        offline = catalogue.read_groups(store, [locator], generation=serving_generation)[locator]
        remember(store, "serving_group", (serving_generation, locator), offline)
        seam = None
        if len(group.members) > 1:
            from . import retained_rename_seam

            seam = retained_rename_seam.select(group, offline)
        with storage.pricing_locks(store, group.members, known=index["symbols"]):
            sources = _current_sources(store, group.members, known=index["symbols"])
            group.accept_sources(
                group.members, {symbol: document_digest(source) for symbol, source in sources.items()}
            )
            if seam is not None:
                previous = {symbol: storage.read_receipt(root, symbol, index) for symbol in group.members}
                prepared = retained_rename_seam.prepare(seam, previous, root)
            else:
                prepared = _prepare_single(context, root, group, offline, sources[name], index, policy)
            return publish_prepared(
                replace(selection, prepared=prepared),
                context,
                progress=progress,
                accepted_inputs=(root, index, sources, {}),
            )
