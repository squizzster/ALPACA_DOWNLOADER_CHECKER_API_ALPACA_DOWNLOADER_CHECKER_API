"""Generation-bound completion of explicitly selected numerical choices only."""

from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from . import storage, universe_state
from .context import accept_context
from .contract import LineageError, document_digest
from .correlation_policy_contract import valid_sha256
from .retained_recovery import RecoveryGroup, RecoverySelection

BOUNDARY = "alpaca_lineage_numerical_recovery"
VERSION = 1
MODE = "numerical_only"
KINDS = ("completed_years", "current_year", "current_month")


@dataclass(frozen=True)
class NumericalRecoveryGroup(RecoveryGroup):
    scopes: dict


@dataclass(frozen=True)
class NumericalRecoverySelection(RecoverySelection):
    normalized: dict | None = None


def accept_selection(value, store):
    """Accept source positions once; they are observations, never owner assertions."""
    from .retained_recovery import RECOVERY_BOUNDARY
    from .retained_recovery import accept_selection as accept_base

    store = Path(store).absolute()
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "boundary", "mode", "store", "correlation_policy_sha256", "groups"}
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != VERSION
        or value.get("boundary") != BOUNDARY
        or value.get("mode") != MODE
        or not isinstance(value.get("groups"), list)
    ):
        raise LineageError("invalid numerical-only recovery selection")
    base = accept_base(
        {
            **{k: value[k] for k in ("schema_version", "store", "correlation_policy_sha256")},
            "boundary": RECOVERY_BOUNDARY,
            "groups": [
                {k: group.get(k) for k in ("members", "pricing_generations", "receipt_generations")}
                for group in value["groups"]
                if isinstance(group, dict)
            ],
        },
        store,
    )
    if len(base.groups) != len(value["groups"]):
        raise LineageError("invalid numerical-only recovery group")
    normalized, groups = [], []
    for group, original in zip(base.groups, value["groups"], strict=True):
        if set(original) != {"members", "pricing_generations", "receipt_generations", "scopes"}:
            raise LineageError("unknown numerical-only recovery group fields")
        scopes = original["scopes"]
        if not isinstance(scopes, dict) or not scopes or not scopes.keys() <= group.receipts.keys():
            raise LineageError("numerical scopes must select published group sources")
        accepted = {}
        for name, intervals in sorted(scopes.items()):
            if not isinstance(intervals, list) or not intervals:
                raise LineageError("numerical scopes require nonempty row intervals")
            ordered, seen = [], {}
            for interval in intervals:
                if (
                    not isinstance(interval, dict)
                    or set(interval) != {"adjustment", "boundary", "public_id", "row_start", "row_end"}
                    or interval.get("adjustment") not in ADJUSTMENTS
                    or interval.get("boundary") not in KINDS
                    or not valid_sha256(interval.get("public_id"))
                    or type(interval.get("row_start")) is not int
                    or type(interval.get("row_end")) is not int
                    or not 1 <= interval["row_start"] <= interval["row_end"]
                ):
                    raise LineageError("invalid numerical source row interval")
                ordered.append(dict(interval))
            ordered.sort(
                key=lambda item: (
                    ADJUSTMENTS.index(item["adjustment"]),
                    KINDS.index(item["boundary"]),
                    item["row_start"],
                )
            )
            owners = {}
            for interval in ordered:
                key = interval["adjustment"], interval["boundary"]
                if interval["row_start"] <= seen.get(key, 0):
                    raise LineageError("overlapping numerical source row intervals")
                seen[key] = interval["row_end"]
                owners.setdefault(interval["public_id"], set()).add(interval["adjustment"])
            if any(views != set(ADJUSTMENTS) for views in owners.values()):
                raise LineageError("numerical completion requires all eight selected views")
            accepted[name] = ordered
        record = {
            "members": list(group.members),
            "pricing_generations": group.pricing,
            "receipt_generations": group.receipts,
            "scopes": accepted,
        }
        operation_id = document_digest(
            {
                "version": VERSION,
                "mode": MODE,
                "store": str(store),
                "policy": base.policy_sha256,
                "group": record,
            }
        )
        groups.append(
            NumericalRecoveryGroup(group.members, group.pricing, group.receipts, operation_id, accepted)
        )
        normalized.append(record)
    return NumericalRecoverySelection(
        store,
        base.policy_sha256,
        tuple(groups),
        normalized={**value, "store": str(store), "groups": normalized},
    )


def single_selection(selection, group):
    record = next(item for item in selection.normalized["groups"] if tuple(item["members"]) == group.members)
    return {**selection.normalized, "groups": [record]}


def enroll_selection(selection, *, index=None, work=None):
    """Persist a selected group's requirement before any delegated publication."""
    from .work_contract import obligation

    previous = (
        universe_state.read_state(selection.store, symbols=selection.symbols)["work"] if work is None else work
    )
    changed, accepted = {}, {}
    for group in selection.groups:
        requirement = {
            "version": VERSION,
            "operation_id": group.operation_id,
            "selection": single_selection(selection, group),
        }
        done = index is not None and group.select(index)
        for name in group.receipts:
            old = previous.get(name, {})
            if old.get("status") != "complete" and old.get("required_numerical_completion") == requirement:
                accepted[name] = old
                continue
            if done:
                continue
            # A new numerical requirement cannot revive already completed
            # pricing, lookup, correction or coverage obligations.
            retained = old if old.get("status") != "complete" else {}
            target = retained.get("end_exclusive")
            if target is None and index is not None:
                target = index["symbols"][name]["projection"]["processed_through"][:10]
            if target is None:
                raise LineageError("numerical work requires its selected pricing target")
            item = {
                **retained,
                "end_exclusive": target,
                "pricing_end_exclusive": retained.get("pricing_end_exclusive", target),
                "required_group": list(group.members),
                "required_numerical_completion": requirement,
                "status": "pending",
                "reasons": sorted(set(retained.get("reasons", ())) | {"numerical_selection_incomplete"}),
                "acquire": retained.get("acquire", False),
                "invalidate_pricing": retained.get("invalidate_pricing", False),
                "refresh_metadata": retained.get("refresh_metadata", False),
                "required_history_check": retained.get("required_history_check", False),
            }
            changed[name] = obligation(item, old)
    if changed:
        accepted.update(universe_state.put_work_group(selection.store, changed))
    return accepted


def _validate_scopes(group, previous, documents):
    for name, intervals in group.scopes.items():
        for interval in intervals:
            kind, view = interval["boundary"], interval["adjustment"]
            entry = next(item for item in documents[name][kind]["adjustments"] if item["adjustment"] == view)
            position = interval["row_start"]
            for segment in entry["segments"]:
                if segment["row_end"] < position or segment["row_start"] > interval["row_end"]:
                    continue
                if (
                    segment["row_start"] > position
                    or segment["branch"] != "correlated"
                    or segment.get("security_identity", {}).get("security_id") != interval["public_id"]
                    or segment.get("observation_selection") not in (None, "selected")
                    or kind == "current_year"
                    and segment["from"][:7] >= previous[name]["anchor_month"]
                ):
                    raise LineageError("numerical scope differs from its accepted row ownership")
                position = min(interval["row_end"], segment["row_end"]) + 1
            if position != interval["row_end"] + 1:
                raise LineageError("numerical scope exceeds its accepted assignment boundary")


def _generation_applies(group, sources, name, generation, day):
    if name not in sources:
        return None
    if group.pricing[name] == generation:
        return True
    ancestor = next(
        (item for item in sources[name].get("history_changes", ()) if item["source_sha256"] == generation),
        None,
    )
    if ancestor is None:
        return None
    return ancestor["changed_from"] is None or day < ancestor["changed_from"]


def _require_uncompleted_scope(group, previous, sources, documents):
    """Closed equality needs its carry owner, never another numerical contest.

    Current receipts bind mapping and ownership. Original donor receipt tokens
    survive later metadata publication; current pricing or accepted prefix
    ancestry establishes numerical applicability. Interior partial intervals
    cannot distinguish every old date from metadata and fail explicitly too.
    """
    selected = {}
    for name, intervals in group.scopes.items():
        for interval in intervals:
            entry = next(
                value
                for value in documents[name][interval["boundary"]]["adjustments"]
                if value["adjustment"] == interval["adjustment"]
            )
            for segment in entry["segments"]:
                if segment["row_start"] <= interval["row_end"] and interval["row_start"] <= segment["row_end"]:
                    selected.setdefault(interval["public_id"], []).append((interval, segment))
    seen = set()
    for receipt in previous.values():
        for frame in receipt.get("numerical_source_equivalence", {}).get("decisions", ()):
            if id(frame) in seen:
                continue
            seen.add(id(frame))
            for pid in frame["identities"].keys() & selected.keys():
                for day, decision in frame["identities"][pid].items():
                    overlaps = [
                        (scope, segment)
                        for scope, segment in selected[pid]
                        if segment["from"] <= day <= segment["to"]
                    ]
                    if not overlaps:
                        continue
                    winner = decision["selected_source"]
                    winner_applies = _generation_applies(
                        group, sources, winner, frame["pricing_generations"][winner], day
                    )
                    donors = []
                    for donor in decision["donors"]:
                        bindings = [
                            _generation_applies(
                                group, sources, donor["source"], frame[field][donor["source"]], day
                            )
                            for field in ("pricing_generations", "previous_pricing_generations")
                        ]
                        donors.append(False if False in bindings else None if None in bindings else True)
                    if winner_applies is False or all(value is False for value in donors):
                        continue
                    applicable = winner_applies is True and any(value is True for value in donors)
                    exact = any(
                        scope["row_start"] <= segment["row_start"] <= segment["row_end"] <= scope["row_end"]
                        or day == segment["from"]
                        and scope["row_start"] <= segment["row_start"]
                        or day == segment["to"]
                        and segment["row_end"] <= scope["row_end"]
                        for scope, segment in overlaps
                    )
                    detail = "contains" if exact and applicable else "may overlap"
                    raise LineageError(
                        f"numerical scope {detail} an applicable completed source-equivalence decision; "
                        "consume its existing winner/equality through owning direct carry instead of reselecting"
                    )


def prepare_group(selection, group, previous, sources, *, root, cache, context):
    """Construct a complete output once from retained owners and scoped prices."""
    from libs.alpaca_serving_catalogue import storage as serving_storage

    from .assignment_handoff import produce, serving_ownership
    from .numerical_completion import complete
    from .numerical_revision import build
    from .numerical_scope import admit as admit_scope
    from .numerical_scope import complete as close_scope
    from .observations import _own_interval
    from .prepared_recovery import PreparedCandidate
    from .pricing import metadata_source_control
    from .source_choice_handoff import produce as produce_bundles
    from .source_equivalence import retire as retire_equivalence
    from .source_observations import apply_selection, counterpart_positions
    from .source_work import assignment_documents, retained_ownership_report

    if "documents" not in cache:
        cache["documents"] = {
            name: assignment_documents(
                root,
                previous[name],
                cache.setdefault("assignments", {}),
                kinds={item["boundary"] for item in intervals},
            )
            for name, intervals in group.scopes.items()
        }
    documents = cache["documents"]
    if not cache.get("scope_validated"):
        _validate_scopes(group, previous, documents)
        cache["scope_validated"] = True
    if "scope_closure" not in cache:
        cache["scope_closure"] = admit_scope(group, previous, documents)
    if not cache.get("scope_uncompleted"):
        _require_uncompleted_scope(group, previous, sources, documents)
        cache["scope_uncompleted"] = True
    controls = cache.setdefault("controls", {})
    accepted_reports = cache.setdefault("accepted_reports", {})
    scoped = cache.setdefault("scoped", {})
    for name, intervals in group.scopes.items():
        prior = previous[name]
        if name not in controls:
            controls[name] = metadata_source_control(
                selection.store, name, sources[name], group.pricing[name], prior, accepted_reuse=(True, True)
            )
        if name not in accepted_reports:
            memberships = {
                identifier: identity
                for identity in prior["identity_memberships"]
                for identifier in identity["member_alpaca_asset_ids"]
            }
            accepted_reports[name] = retained_ownership_report(
                prior,
                controls[name].local,
                memberships,
                root,
                cache["assignments"],
                kinds={item["boundary"] for item in intervals},
            )
        if name not in cache.setdefault("route_validated", set()):
            report = accepted_reports[name]
            raw_rows = sum(
                segment["row_count"]
                for branch in ("correlated", "uncorrelated")
                for segment in accepted_reports[name][branch]["segments"]
                if segment["adjustment"] == "raw"
            )
            if raw_rows != prior["assignment_summary"]["raw"]["rows"] and any(
                item["public_id"] not in accepted_reports[name].get("retained_observation_routes", {})
                for item in intervals
            ):
                raise LineageError(
                    "numerical completion lacks an accepted full-source observation route; "
                    "complete the owning metadata handoff before ranking a clipped source"
                )
            routes = report.get("retained_observation_routes", {})
            missing = {item["public_id"] for item in intervals} - routes.keys()
            if missing:
                # Full selected metadata can complete an absent route without
                # materializing any outside-scope numerical observations.
                from libs.alpaca_pricing_download.contract import timestamp

                routes = dict(routes)
                for pid in missing:
                    own = [
                        segment
                        for segment in report["correlated"]["segments"]
                        if segment["adjustment"] == "raw"
                        and segment["historical_symbol"] == name
                        and segment["security_identity"]["security_id"] == pid
                    ]
                    if own:
                        segment = max(own, key=lambda item: timestamp(item["last_timestamp"]))
                        last = segment["last_timestamp"]
                        routes[pid] = (last, _own_interval(report, segment, last, day=segment["to"]))
                accepted_reports[name] = {**report, "retained_observation_routes": routes}
            cache["route_validated"].add(name)
        if name not in scoped:
            scoped[name] = counterpart_positions(
                selection.store,
                controls[name],
                accepted_reports[name],
                intervals,
                cache.setdefault("partitions", {}),
            )
    inputs = {name: value[0] for name, value in scoped.items()}
    reports = {name: value[1] for name, value in scoped.items()}
    positions = {name: value[2] for name, value in scoped.items()}
    dates = {name: value[3] for name, value in scoped.items()}
    if not cache.get("scope_closed"):
        close_scope(group, previous, documents, scoped, cache["scope_closure"])
        cache["scope_closed"] = True
    if "numerical" not in cache:
        cache["numerical"] = complete(inputs, reports, positions=positions, dates=dates)
    completion = cache["numerical"]
    changed = cache.setdefault("changed_documents", {name: {} for name in previous})
    accepted = cache.setdefault("accepted_documents", {name: dict(documents.get(name, {})) for name in previous})
    patched = cache.setdefault("patched", set())
    for name, receipt in previous.items():
        if name not in completion.changes:
            continue
        changes = {**completion.changes[name], "dates": dates[name]}
        for kind in {item["boundary"] for item in group.scopes[name]}:
            if (name, kind) in patched:
                continue
            offsets = {view: receipt["serving_ownership"]["offsets"][view][kind] for view in ADJUSTMENTS}
            new = apply_selection(documents[name][kind], changes, offsets)
            if new != documents[name][kind]:
                changed[name][kind] = accepted[name][kind] = new
            patched.add((name, kind))
    if "serving_records" not in cache:
        serving = context.select_serving()
        keys = {serving["members"][name] for name in group.members if name in serving["members"]}
        priors = serving_storage.read_groups(selection.store, keys, generation=serving["state"]["generation"])
        records = {}
        for prior in priors.values():
            if (
                prior["references"]["lineage"] == group.receipts
                and prior["references"]["pricing"] == group.pricing
            ):
                records.update(
                    (item["public_id"], item) for item in prior["history_validation"]["prefix_records"]
                )
        cache["serving_records"] = records
    if "revision" not in cache:
        cache["revision"] = build(
            previous,
            group.receipts,
            completion,
            records=cache["serving_records"],
            positions=positions,
        )
    revision = cache["revision"]
    if "remaining_equivalence" not in cache:
        cache["remaining_equivalence"] = retire_equivalence(previous, completion.scopes)
    receipts = cache.setdefault("receipt_parts", {})
    produced = cache.setdefault("produced", set())
    finished = cache.setdefault("finished_receipts", set())
    for name, old in previous.items():
        if name not in receipts:
            receipt = receipts[name] = deepcopy(old)
            if name in accepted_reports:
                receipt["observation_routes"] = accepted_reports[name].get("retained_observation_routes", {})
            receipt["numerical_source_revision_version"] = revision["version"]
            receipt["numerical_source_revision"] = revision
            receipt.pop("numerical_source_equivalence_version", None)
            receipt.pop("numerical_source_equivalence", None)
            if cache["remaining_equivalence"] is not None:
                receipt["numerical_source_equivalence_version"] = 1
                receipt["numerical_source_equivalence"] = cache["remaining_equivalence"]
            market = receipt.get("dated_market_projection")
            if market is not None:
                kept = {
                    pid: value for pid, value in market["identities"].items() if pid not in completion.scopes
                }
                if kept:
                    receipt["dated_market_projection"] = {**market, "identities": kept}
                else:
                    receipt.pop("dated_market_projection_version", None)
                    receipt.pop("dated_market_projection", None)
        receipt = receipts[name]
        for kind, document in changed[name].items():
            if (name, kind) not in produced:
                generation = document_digest(document)
                receipt["boundaries"][kind] = {**receipt["boundaries"][kind], "sha256": generation}
                receipt["assignment_handoffs"][kind] = produce(document, generation)
                produced.add((name, kind))
        if name not in finished:
            if changed[name]:
                receipt["serving_ownership"] = serving_ownership(receipt)
            receipt["retained_recovery"] = group.completion()
            finished.add(name)
    if "bundle_handoff" not in cache:
        cache["bundle_handoff"] = (
            produce_bundles(completion.selected_bundles, revision)
            if any(not value for days in completion.equality_outcomes.values() for value in days.values())
            else None
        )
    if "prepared" not in cache:
        cache["prepared"] = PreparedCandidate(
            receipts,
            changed,
            previous,
            previous[group.seed]["correlation_policy"],
            accepted,
            cache["bundle_handoff"],
        )
    return cache["prepared"]


def resume(selection, *, context=None, work_policy=None, progress=None, enroll=False, writer_view=None):
    """Finish selected numerical work under its normal owning locks and journal."""
    from .pipeline import _current_sources
    from .prepared_recovery import publish_prepared
    from .recovery_handoffs import recall, remember

    context = accept_context(selection.store, context, work_policy)
    holder = context.group_completions
    result = {
        "failed": 0,
        "complete": True,
        "processed": [],
        "previously_completed_groups": 0,
        "published_prepared_groups": 0,
        "recomputed_groups": 0,
    }
    manager = (
        nullcontext(writer_view[0])
        if writer_view is not None
        else storage.writer_lock(selection.store, accepted_store=context.accept_store())
    )
    with manager as root:
        context.recover_lineage(root)
        if context.accept_correlation_policy().semantic_sha256 != selection.policy_sha256:
            raise LineageError("numerical recovery policy changed from its selected scope")
        if writer_view is None:
            with storage.publication_lock(root):
                index = context.select_index(root)
        else:
            index = writer_view[1]
        if enroll:
            enrolled = enroll_selection(selection, index=index)
            if work_policy is None and enrolled:
                from .work_contract import WorkPolicy

                work_policy = WorkPolicy(
                    selection.store,
                    index,
                    enrolled,
                    [],
                    max(item["end_exclusive"] for item in enrolled.values()),
                    context=context,
                )
        for group in selection.groups:
            if group.select(index):
                result["previously_completed_groups"] += 1
                result["processed"].extend(group.receipts)
                cache = holder.pop((MODE, group.operation_id), None)
                if cache is not None and "prepared" in cache:
                    prepared = cache["prepared"]
                    receipts = cache.get("publication_effects", {}).get("receipts", prepared.receipts)
                    for name, receipt in receipts.items():
                        if recall(selection.store, "receipt", (name, index["symbols"][name]["sha256"])) is None:
                            remember(
                                selection.store, "receipt", (name, index["symbols"][name]["sha256"]), receipt
                            )
                        for kind, document in prepared.assignments[name].items():
                            remember(
                                selection.store,
                                "assignment",
                                (name, kind, receipt["boundaries"][kind]["sha256"]),
                                document,
                            )
                continue
            key = (MODE, group.operation_id)
            cache = holder.setdefault(key, {})
            with storage.pricing_locks(selection.store, group.members, known=index["symbols"]):
                sources = _current_sources(selection.store, group.members, known=index["symbols"])
                group.accept_sources(
                    group.members, {name: document_digest(value) for name, value in sources.items()}
                )
                if "previous" not in cache:
                    cache["previous"] = {
                        name: storage.read_receipt(root, name, index) for name in group.receipts
                    }
                if "prepared" not in cache:
                    cache["prepared"] = prepare_group(
                        selection, group, cache["previous"], sources, root=root, cache=cache, context=context
                    )
                chosen = replace(selection, groups=(group,), prepared=cache["prepared"])
                published = publish_prepared(
                    chosen,
                    context,
                    progress=progress,
                    accepted_inputs=(root, index, sources, cache),
                    work_policy=work_policy,
                )
                result["published_prepared_groups"] += published["published_prepared_groups"]
                result["processed"].extend(group.receipts)
                holder.pop(key, None)
    if enroll and work_policy is not None:
        work_policy.acknowledge()
    result["processed"] = sorted(set(result["processed"]))
    result.update(
        symbol=min(selection.symbols),
        status="processed",
        provider_requests=0,
        pricing_writes=0,
        identity_rule_executions=0,
    )
    return result


enroll = enroll_selection
