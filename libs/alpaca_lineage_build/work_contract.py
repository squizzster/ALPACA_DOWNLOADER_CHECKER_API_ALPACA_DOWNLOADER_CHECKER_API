"""Accepted update obligations and restrictions shared by every processing phase."""

from uuid import uuid4

from libs.alpaca_pricing_download.contract import HISTORY_START, failure_outcome
from libs.alpaca_security_lineage.lineage import compact_asset
from libs.common.debug_trace import event, traced

from .contract import LineageError, document_digest, reusable_decision
from .selection import DependencyGraph


def obligation(item, previous=None):
    """An unfinished contract survives reselection, including catalogue absence."""
    previous = previous or {}
    if item.get("required_sourced_facts"):
        raise LineageError("authored fact requirements need explicit generic lineage recovery")
    keys = (
        "end_exclusive",
        "evidence_fingerprint",
        "acquisition_evidence_fingerprint",
        "pricing_start",
        "pricing_end_exclusive",
        "acquire",
        "invalidate_pricing",
        "refresh_metadata",
        "required_history_check",
        "correction_id",
        "pricing_correction",
        "required_group",
    )
    if previous.get("obligation_id") and all(item.get(k) == previous.get(k) for k in keys):
        return {**item, "obligation_id": previous["obligation_id"]}
    return {**item, "obligation_id": uuid4().hex}


def acquisition_fingerprint(item):
    """Older accepted obligations used one fingerprint for both requirements."""
    return item.get("acquisition_evidence_fingerprint", item.get("evidence_fingerprint"))


def correction_satisfied(item, generation, recorded_id=None):
    if not item.get("correction_id") or recorded_id == item["correction_id"]:
        return True
    completed = item.get("correction_completion") or {}
    return bool(generation) and (completed.get("correction_id"), completed.get("source_sha256")) == (
        item["correction_id"],
        generation,
    )


def replacement_fingerprint(lookups):
    """Bind a group's replacement to mappings, independently of mutable metadata."""
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # A replacement is caused by the accepted label-to-identity mapping of the
    # complete group. Status/name edits and retry timing cannot renew that work;
    # a later mapping change must renew it even for an unchanged sibling source.
    # Bind this at handoff creation so consumers only compare the accepted token.
    return document_digest(
        {
            "identity_mappings": {
                label: [(value.get("asset") or {}).get(key) for key in ("id", "symbol")]
                for label, value in lookups.items()
            }
        }
    )


def evidence_requirement(item):
    """Approval/coverage changes do not renew an accepted evidence requirement."""
    return item.get("evidence_fingerprint") or item["obligation_id"]


def pricing_target(item):
    return item.get("pricing_end_exclusive", item["end_exclusive"])


def covers_pricing_start(item, request):
    """Coverage includes the accepted inclusive start, even for an empty source."""
    start = item.get("pricing_start")
    return start is None or (
        request is not None and (request.get("history_start") or HISTORY_START)[:10] <= start
    )


def lookup_fact(lookup):
    """Catalogue and exact lookup provenance do not change the identity fact."""
    return lookup.get("outcome"), compact_asset(lookup["asset"]) if lookup.get("asset") else None


def symbol_lookups(evidence):
    result = {}
    for value in evidence.values():
        for lookup in value["asset_lookups"]:
            if lookup.get("lookup_kind") != "symbol":
                continue
            label = lookup["lookup"]
            if label in result and lookup_fact(result[label]) != lookup_fact(lookup):
                raise LineageError(f"{label}: conflicting symbol evidence in selected group")
            result[label] = {k: lookup[k] for k in ("outcome", "http_status", "asset", "source") if k in lookup}
    return result


def work_graph(entries, work):
    """Accepted, unfinished relationships survive changes in provider evidence."""
    selected = {s: {**entry, "dependencies": set(entry["dependencies"])} for s, entry in entries.items()}
    for symbol, item in work.items():
        if item.get("status") == "complete":
            continue
        group = set(item.get("required_group", [])) | {symbol}
        for member in group:
            selected.setdefault(member, {"dependencies": set(), "asset_ids": []})["dependencies"].update(group)
    return DependencyGraph(selected)


def satisfied(item, entry, pending, *, approved=False):
    # A matching publication stamp is one proof of completion. approved is only
    # supplied by the operation's generation-bound producer handoff below; it
    # must never be inferred from work.status, elapsed time or matching last rows.
    if not entry or not item.get("obligation_id"):
        return False
    projection = entry.get("projection") or {}
    return (
        not pending
        and covers_pricing_start(item, projection.get("pricing_request"))
        and projection.get("processed_through", "")[:10] >= pricing_target(item)
        and (approved or projection.get("update_obligation_id") == item["obligation_id"])
        and (
            not item.get("invalidate_pricing")
            or projection.get("acquisition_evidence_sha256") == acquisition_fingerprint(item)
        )
        and correction_satisfied(item, entry.get("source_sha256"), projection.get("history_correction_id"))
    )


def auxiliary_work(item):
    """Only dependency acquisition may finish with recorded missing pricing."""
    reasons = set(item.get("reasons", ()))
    return "dependency_reconciliation" in reasons and not reasons.intersection(
        {"initial_acquisition", "explicit_pricing_acquisition"}
    )


def satisfying_entry(symbol, item, entries, pending, *, approved=False):
    """Select completion from either the own receipt or its published auxiliary outcome."""
    entry = entries.get(symbol)
    if satisfied(item, entry, symbol in pending, approved=approved):
        return entry
    if entry or not item.get("obligation_id") or not auxiliary_work(item):
        return None
    group = set(item.get("required_group", ()))
    if group.intersection(pending):
        return None
    for member in sorted(group):
        entry = entries.get(member)
        published = (entry or {}).get("projection", {}).get("unavailable_obligations", {})
        if published.get(symbol) == item["obligation_id"]:
            return entry
    return None


def restriction(item, end, *, retry_blocked=False):
    from .universe_state import now_utc

    if item.get("status") == "complete":
        return None
    if item.get("required_sourced_facts"):
        return "generic_lineage_recovery_required"
    if max(item.get("end_exclusive", end), item.get("pricing_end_exclusive", end)) > end:
        return "pending_target_exceeds_requested_cutoff"
    if not retry_blocked:
        if item.get("status") == "blocked":
            return "blocked"
        if (item.get("retry_after_utc") or "") > now_utc():
            return "retry"
    return None


class WorkDeferred(LineageError):
    pass


class RegistrationIncomplete(LineageError):
    """Local union recovery is pending; no provider retry was attempted."""


class WorkPolicy:
    """One invocation's restrictions; the database owns intentions, never prices."""

    def __init__(self, store, index, work, deferred, end, *, context=None, completed_work=None):
        self.store, self.end = store, end
        self.index = index
        self.context = context
        self.completed_corrections = {
            s: item
            for s, item in (work if completed_work is None else completed_work).items()
            if item.get("correction_completion")
            and item.get("pricing_correction")
            and item["correction_completion"].get("correction_id") == item.get("correction_id")
        }
        self.work = {s: item for s, item in work.items() if item.get("status") != "complete"}
        self.graph = work_graph(index["symbols"], self.work)
        self.required_groups = {}
        for symbol, item in self.work.items():
            group = set(item.get("required_group", [])) | {symbol}
            for member in group:
                self.required_groups.setdefault(member, set()).update(group)
        self.blocked = {item["symbol"] for item in deferred}
        self.blocked.update(
            symbol
            for symbol, item in self.work.items()
            if item.get("required_sourced_facts") and not satisfied(item, index["symbols"].get(symbol), False)
        )
        self.failures = []
        self.command_failures = []
        self.history_approved = {}
        self.completed_decisions = {}
        self.restricted_identifiers = {
            identifier
            for symbol in self.blocked
            for identifier in work.get(symbol, {}).get("outcome", {}).get("request_identifiers", [])
        }
        self.current_group = set()

    def update_index(self, entries):
        self.index["symbols"].update(entries)
        self.graph.update(self._group_entries(entries))

    def satisfying_entry(self, symbol, entries, pending):
        """Consume published facts and this operation's producer approvals, without I/O."""
        item = self.work[symbol]
        if not self.allowed({symbol}):
            return None
        completed = self.completed_decisions.get(symbol)
        if completed is not None:
            obligation_id, owner, references = completed
            if (
                obligation_id == item["obligation_id"]
                and not pending.keys() & references.keys()
                and all((entries.get(s) or {}).get("sha256") == token for s, token in references.items())
                and (
                    not item.get("required_history_check")
                    or symbol in entries
                    and self.history_approved.get(symbol) == entries[symbol]["source_sha256"]
                )
            ):
                return entries[owner]
        entry = entries.get(symbol)
        # DO NOT restore approval-only republication. A successful check of the
        # selected pricing generation changes workflow state, not lineage data.
        # Selection already owns whether metadata needs refreshing; the index
        # owns coverage/group membership. Consume those handoffs in memory, not
        # by reopening receipts or repeating upstream checks. A crash before the
        # workflow acknowledgement leaves the obligation pending for a fresh check.
        approved = bool(
            entry
            and (
                item.get("required_history_check")
                or correction_satisfied(item, entry["source_sha256"])
                and item.get("correction_completion")
            )
            and not item.get("refresh_metadata")
            and (
                not item.get("required_history_check")
                or self.history_approved.get(symbol) == entry["source_sha256"]
            )
            and reusable_decision(entry)
            and set(item.get("required_group", ())) <= set(entry["group_members"])
            and not pending.keys() & set(entry["group_members"])
        )
        return satisfying_entry(symbol, item, entries, pending, approved=approved)

    @traced("lineage_completion")
    def accept_completion(self, receipts, entries, completed):
        """Keep the producer's input approval bound to its selected complete group."""
        # The caller already checked inputs under group locks and selected any
        # changed outputs. Keep that proof, including absent auxiliary members;
        # DO NOT reopen their receipts when a later live approval arrives.
        # This is operation-local: a crash before durable acknowledgement leaves
        # work pending, and no persisted cache is allowed to bypass a fresh check.
        unavailable = next(iter(receipts.values())).get("unavailable_pricing", {})
        references = {s: (entries.get(s) or {}).get("sha256") for s in receipts.keys() | unavailable.keys()}
        for symbol, owner in completed.items():
            self.completed_decisions[symbol] = (self.work[symbol]["obligation_id"], owner, references)

    def _group_entries(self, symbols):
        return {
            s: {
                "dependencies": set(self.index["symbols"].get(s, {}).get("dependencies", []))
                | self.required_groups.get(s, set()),
                "asset_ids": self.index["symbols"].get(s, {}).get("asset_ids", []),
                "group_members": self.index["symbols"].get(s, {}).get("group_members", []),
            }
            for s in symbols
        }

    def require(
        self,
        symbol,
        target,
        *,
        reason,
        acquire=False,
        refresh=False,
        group=(),
        history_check=False,
        start=None,
        _pending=None,
    ):
        """Persist an obligation before its first request, independently of registration."""
        from libs.alpaca_pricing_download.contract import timestamp
        from libs.alpaca_pricing_download.download import request_start

        request_start(start, timestamp(target + "T00:00:00Z"))
        prior = self.work.get(symbol, {})
        required = max(target, pricing_target(prior)) if prior else target
        item = obligation(
            {
                **prior,
                "reasons": sorted(set(prior.get("reasons", [])) | {reason}),
                "end_exclusive": max(prior.get("end_exclusive", target), target),
                "pricing_start": min(filter(None, (prior.get("pricing_start"), start)), default=None),
                "pricing_end_exclusive": required,
                "status": prior.get("status", "pending"),
                "attempts": prior.get("attempts", 0),
                "acquire": prior.get("acquire", False) or acquire,
                "invalidate_pricing": prior.get("invalidate_pricing", False),
                "refresh_metadata": prior.get("refresh_metadata", False) or refresh,
                "required_history_check": prior.get("required_history_check", False) or history_check,
                "required_group": sorted(set(prior.get("required_group", [])) | set(group)),
                "evidence_fingerprint": prior.get("evidence_fingerprint"),
            },
            prior,
        )
        if item != prior:
            if _pending is None:
                self._accept_requirements({symbol: item})
            else:
                _pending[symbol] = item
        return item

    def _accept_requirements(self, items):
        from . import universe_state

        if not items:
            return
        self.work.update(universe_state.put_work_group(self.store, items))
        changed = set()
        for symbol, item in items.items():
            linked = set(item["required_group"]) | {symbol}
            for member in linked:
                self.required_groups.setdefault(member, set()).update(linked)
            changed.update(linked)
            if pricing_target(item) > self.end:
                self.blocked.update(linked)
        self.graph.update(self._group_entries(changed))

    def require_dependencies(self, group, target, required=(), *, start=None):
        """Persist only selected acquisition while retaining its complete group."""
        group = self.graph.closure(set(group))
        required = set(required)
        selected = required or group
        pending = {}
        for symbol in sorted(selected):
            self.require(
                symbol,
                target,
                reason="dependency_reconciliation",
                acquire=symbol in required,
                refresh=symbol not in self.index["symbols"],
                group=group,
                start=start,
                _pending=pending,
            )
        self._accept_requirements(pending)

    def require_maintenance(self, entries, target):
        pending = {}
        for entry in entries:
            if target and entry["end_exclusive"][:10] < target:
                self.require(
                    entry["symbol"],
                    target,
                    reason="advance_summary_coverage",
                    history_check=True,
                    _pending=pending,
                )
        self._accept_requirements(pending)

    @traced("work_completion")
    def acknowledge(self):
        """Acknowledge selected publications or fresh producer approvals, never workflow status alone."""
        from libs.alpaca_pricing_download.update_checks import read_checks

        from . import storage, universe_state

        root = self.store / "end_of_day/lineage"
        self.pending_work = set(self.work)
        if not root.exists():
            return
        with storage.publication_lock(root):
            if (root / ".pending.json").exists():
                return
            index = (
                self.context.select_index(root)
                if self.context is not None
                else storage.refresh_index(root, self.index)
            )
            pending = read_checks(self.store)["checks"]
        acknowledged = set()
        with universe_state.outcome_batch(self.store) as batch:
            for symbol, item in self.work.items():
                entry = self.satisfying_entry(symbol, index["symbols"], pending)
                if symbol not in self.blocked and entry:
                    universe_state.record_outcome(
                        self.store,
                        symbol,
                        batch=batch,
                        expected=item,
                        status="complete",
                        outcome={
                            "lineage_generation": entry["sha256"],
                            "source_generation": entry["source_sha256"],
                        }
                        if symbol in index["symbols"]
                        else {"lineage_generation": entry["sha256"], "outcome": "unavailable_auxiliary_pricing"},
                    )
                    acknowledged.add(symbol)
        self.pending_work.difference_update(acknowledged)

    def allowed(self, symbols):
        visited = set()
        for symbol in symbols:
            if symbol in visited:
                continue
            group = self.graph.closure({symbol})
            if group.intersection(self.blocked):
                return False
            visited.update(group)
        return True

    def check(self, symbols):
        if not self.allowed(symbols):
            raise WorkDeferred("affected group has deferred or failed update work")

    def needs_refresh(self, group, previous):
        unavailable = {
            symbol: obligation_id
            for receipt in previous.values()
            if receipt
            for symbol, obligation_id in receipt.get("unavailable_obligations", {}).items()
        }
        evidence_requirements = {
            symbol: requirement
            for receipt in previous.values()
            if receipt
            for symbol, requirement in receipt.get("evidence_requirements", {}).items()
        }
        return any(
            symbol in self.work
            and self.work[symbol].get("refresh_metadata")
            and (previous.get(symbol) or {}).get("update_obligation_id") != self.work[symbol]["obligation_id"]
            and unavailable.get(symbol) != self.work[symbol]["obligation_id"]
            and evidence_requirements.get(symbol) != evidence_requirement(self.work[symbol])
            for symbol in group
        )

    def needs_full_metadata(self, group, previous):
        """Classify acquired endpoint changes before selecting the action window."""
        prior_lookups = symbol_lookups(
            {symbol: receipt["evidence"] for symbol, receipt in previous.items() if receipt}
        )
        for symbol in group:
            if not self.needs_refresh({symbol}, previous):
                continue
            item = self.work.get(symbol, {})
            if "explicit_lineage_refresh" in item.get("reasons", ()):
                return True
            for label, after in item.get("required_lookups", {}).items():
                before = prior_lookups.get(label)
                if (
                    before is None
                    or before.get("outcome") != after.get("outcome")
                    or any(
                        (before.get("asset") or {}).get(key) != (after.get("asset") or {}).get(key)
                        for key in ("id", "symbol", "class", "exchange")
                    )
                ):
                    return True
        return False

    def decision_inputs_satisfied(self, sources, selected=None):
        """Outstanding approval does not invalidate an otherwise completed decision."""
        for symbol, source in sources.items():
            item = self.work.get(symbol)
            if item is not None and not (
                covers_pricing_start(item, source)
                and source["latest_request"]["end_exclusive"][:10] >= pricing_target(item)
                and (
                    not item.get("invalidate_pricing")
                    or source.get("acquisition_evidence_sha256") == acquisition_fingerprint(item)
                )
                and (
                    correction_satisfied(item, (selected or {}).get(symbol), source.get("history_correction_id"))
                )
            ):
                return False
        return True

    def reconcile_evidence(self, evidence, sources, previous, *, group=None, target=None, partial=False):
        """Bind newly acquired evidence before pricing delegation or lineage publication."""
        from . import universe_state

        group = set(sources) if group is None else set(group)
        self.check(group)
        observed = symbol_lookups(evidence)
        lookups, changed, mapping = {}, set(), False
        for symbol in sorted(group):
            prior = self.work.get(symbol, {})
            old = symbol_lookups({symbol: previous[symbol]["evidence"]}) if previous.get(symbol) else {}
            old.update(prior.get("required_lookups", {}))
            own = (
                symbol_lookups({symbol: evidence[symbol]})
                if symbol in evidence
                else {symbol: observed[symbol]}
                if symbol in observed
                else {}
            )
            labels = own.keys() | prior.get("required_lookups", {}).keys()
            if labels - observed.keys():
                if partial:
                    continue  # Newly expanded members supply their evidence on the next group pass.
                raise WorkDeferred(f"{symbol}: accepted symbol evidence is absent from the selected group")
            lookups[symbol] = {label: observed[label] for label in sorted(labels)}
            for label, current in lookups[symbol].items():
                if label not in old or lookup_fact(old[label]) == lookup_fact(current):
                    continue
                changed.add(symbol)
                before, after = old[label].get("asset") or {}, current.get("asset") or {}
                mapping |= (before.get("id"), before.get("symbol")) != (after.get("id"), after.get("symbol"))
        updates = {}
        group_fingerprint = replacement_fingerprint(observed)
        for symbol in sorted(group):
            source = sources.get(symbol)
            prior = self.work.get(symbol, {})
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # Older pending work can lack a replacement token. Bind that existing
            # obligation to the accepted group once, without replacing siblings
            # whose completed receipts already satisfy their own requirements.
            unbound_replacement = prior.get("invalidate_pricing") and acquisition_fingerprint(prior) is None
            if symbol not in lookups:
                continue
            if not (
                mapping
                or unbound_replacement
                or symbol in changed
                or prior
                and lookups[symbol].keys() - prior.get("required_lookups", {}).keys()
            ):
                continue
            covered = source["latest_request"]["end_exclusive"][:10] if source else target
            fingerprint = document_digest({"lookups": lookups[symbol]})
            updates[symbol] = obligation(
                {
                    **prior,
                    "reasons": sorted(
                        set(prior.get("reasons", []))
                        | {"changed_identity_mapping" if mapping else "lineage_evidence"}
                    ),
                    "end_exclusive": max(prior.get("end_exclusive", covered), covered),
                    "pricing_end_exclusive": max(pricing_target(prior), covered) if prior else covered,
                    "status": prior.get("status", "pending"),
                    "attempts": prior.get("attempts", 0),
                    "required_lookups": lookups[symbol],
                    "evidence_fingerprint": fingerprint,
                    "acquisition_evidence_fingerprint": group_fingerprint
                    if mapping or unbound_replacement
                    else acquisition_fingerprint(prior) or (fingerprint if source is None else None),
                    "acquire": mapping or prior.get("acquire", False),
                    "invalidate_pricing": mapping or prior.get("invalidate_pricing", False),
                    "refresh_metadata": mapping or symbol in changed or prior.get("refresh_metadata", False),
                    "required_group": sorted(set(prior.get("required_group", [])) | group),
                },
                prior,
            )
        if updates:
            self.work.update(universe_state.put_work_group(self.store, updates))
            for symbol, item in updates.items():
                for member in item["required_group"]:
                    self.required_groups.setdefault(member, set()).update(item["required_group"])
                if pricing_target(item) > self.end:
                    self.blocked.add(symbol)
            self.graph.update(self._group_entries(group))
        self.check(group)
        return {
            symbol
            for symbol, source in sources.items()
            if self.work.get(symbol, {}).get("invalidate_pricing")
            and source.get("acquisition_evidence_sha256") != acquisition_fingerprint(self.work[symbol])
        }

    def stamp(self, receipts, sources):
        """Check new completion once under group locks; return its owning receipts."""
        self.check(receipts)
        unavailable = next(iter(receipts.values())).get("unavailable_pricing", {}) if receipts else {}
        observed = symbol_lookups(
            {s: receipt["evidence"] for s, receipt in receipts.items()}
            | {s: value["evidence"] for s, value in unavailable.items()}
        )
        group = receipts.keys() | unavailable.keys()
        for symbol in group:
            item = self.work.get(symbol)
            if item and any(
                label not in observed or lookup_fact(expected) != lookup_fact(observed[label])
                for label, expected in item.get("required_lookups", {}).items()
            ):
                raise WorkDeferred(f"{symbol}: lineage evidence differs from its accepted obligation")
        accepted = {}
        for symbol, receipt in receipts.items():
            item = self.work.get(symbol)
            if item is None:
                continue
            if set(item.get("required_group", [])) - group:
                continue
            source = sources[symbol]
            if item.get("invalidate_pricing") and source.get(
                "acquisition_evidence_sha256"
            ) != acquisition_fingerprint(item):
                raise WorkDeferred(f"{symbol}: required identity-related pricing replacement is incomplete")
            if not covers_pricing_start(item, source) or source["latest_request"]["end_exclusive"][
                :10
            ] < pricing_target(item):
                continue  # Maintenance still owes the remaining interval.
            if not correction_satisfied(item, receipt["source_sha256"], source.get("history_correction_id")):
                continue
            accepted[symbol] = symbol
            # Input acceptance is not history approval. The latter is a separate
            # source-bound handoff; keep the receipt's producer token, do not hash
            # the already accepted source document again to manufacture one.
            if (
                item.get("required_history_check")
                and self.history_approved.get(symbol) != receipt["source_sha256"]
            ):
                continue
            receipt["update_obligation_id"] = item["obligation_id"]
        # Record only accepted refresh requirements, independently of pricing and
        # approval obligations. Their existing fingerprints suffice; no new IDs or
        # work-state fields are needed. Generation/group guards remain mandatory.
        evidence_requirements = {
            member: evidence_requirement(self.work[member])
            for member in sorted(group)
            if self.work.get(member, {}).get("refresh_metadata")
        }
        for symbol, receipt in receipts.items():
            owned = {s: value for s, value in evidence_requirements.items() if s == symbol or s in unavailable}
            if owned:
                receipt["evidence_requirements"] = owned
            else:
                receipt.pop("evidence_requirements", None)
        # An unavailable auxiliary has no own pricing/lineage receipt. Its owning
        # group publishes the accepted outcome before orchestration acknowledges it.
        if any(
            s in self.work and receipt.get("update_obligation_id") != self.work[s]["obligation_id"]
            for s, receipt in receipts.items()
        ):
            return accepted
        completed = {}
        for symbol, value in unavailable.items():
            item = self.work.get(symbol)
            if not item or not auxiliary_work(item) or set(item.get("required_group", ())) - group:
                continue
            request = value["request"]
            if (
                value["outcome"] == "invalid_symbol"
                and request["end_exclusive"][:10] >= pricing_target(item)
                and (not item.get("pricing_start") or request["start"][:10] <= item["pricing_start"])
            ):
                completed[symbol] = item["obligation_id"]
        if completed:
            for receipt in receipts.values():
                receipt["unavailable_obligations"] = completed
            accepted.update({s: next(iter(receipts)) for s in completed})
        return accepted

    def require_correction(self, prepared, *, require_check=True):
        """Persist the completed correction scope and usable handoff before acquisition."""
        from libs.alpaca_pricing_download.correction import merge_observations, observations

        from . import universe_state

        changed = {}
        for request in prepared:
            symbol = request["symbol"]
            prior = self.work.get(symbol, {})
            if request.get("completed_correction") and request.get("required_correction_id") == prior.get(
                "correction_id"
            ):
                # The selected pricing publication completed this requirement.
                # Pending lineage/acknowledgement does not renew acquisition.
                continue
            target = request["end_exclusive"][:10]
            correction = prior.get("correction_id")
            if correction is None or request.get("history_correction_id") == correction:
                correction = uuid4().hex
            selection = request.get("endpoint_selection")
            contract = {
                "schema_version": 3,
                "store": str(self.store),
                "symbol": symbol,
                "scope": (
                    prior.get("pricing_correction", {}).get("scope", "full_history")
                    if request.get("correction_scope") == "unchanged"
                    else request.get("correction_scope", "full_history")
                ),
                "cause": request.get("correction_cause", "retained_replacement_required"),
                "source_sha256": request["source_sha256"],
                "asof": request["asof"],
                "start": request["start"],
                "end_exclusive": request["end_exclusive"],
                "selection": selection.document() if selection is not None else None,
                "comparison_end_exclusive": request.get("comparison_end_exclusive", request["end_exclusive"]),
                "comparison_asof": request.get("comparison_asof", request["asof"]),
                "observations": merge_observations(observations(request)),
            }
            previous_contract = prior.get("pricing_correction") or {}
            binding = (
                "store",
                "symbol",
                "source_sha256",
                "asof",
                "start",
                "end_exclusive",
                "comparison_end_exclusive",
                "comparison_asof",
            )
            if correction == prior.get("correction_id") and all(
                previous_contract.get(key) == contract.get(key) for key in binding
            ):
                contract["observations"] = merge_observations(
                    previous_contract.get("observations", {}), contract["observations"]
                )
                if previous_contract.get("scope") == "full_history":
                    contract["scope"] = "full_history"
                    if (
                        previous_contract.get("cause") != "provider_late_observations"
                        and contract["cause"] == "provider_late_observations"
                    ):
                        # A missing-session witness adds evidence; it cannot
                        # replace an already-selected historical correction.
                        contract["cause"] = previous_contract["cause"]
            # A correction ID names one exact requirement. Renewing evidence or
            # binding must invalidate both source stamps and equal-price receipts.
            requirement = (*binding, "scope", "observations")
            if correction == prior.get("correction_id") and any(
                contract.get(key) != previous_contract.get(key) for key in requirement
            ):
                correction = uuid4().hex
            item = obligation(
                {
                    **prior,
                    "reasons": sorted(set(prior.get("reasons", [])) | {"retained_history_correction"}),
                    "end_exclusive": max(prior.get("end_exclusive", target), target),
                    "pricing_end_exclusive": max(pricing_target(prior), target) if prior else target,
                    "status": prior.get("status", "pending"),
                    "required_history_check": require_check or prior.get("required_history_check", False),
                    "correction_id": correction,
                    "correction_completion": prior.get("correction_completion")
                    if correction == prior.get("correction_id")
                    else None,
                    "pricing_correction": contract,
                    "acquire": prior.get("acquire", False),
                    "refresh_metadata": prior.get("refresh_metadata", False),
                    "invalidate_pricing": prior.get("invalidate_pricing", False),
                    "evidence_fingerprint": prior.get("evidence_fingerprint"),
                },
                prior,
            )
            if item != prior:
                changed[symbol] = item
            request["required_correction_id"] = correction
            if correction == prior.get("correction_id"):
                request["recover_correction"] = True
                if prior.get("correction_completion"):
                    request["correction_completion"] = prior["correction_completion"]
            request["correction_observations"] = contract["observations"]
            request["correction_cause"] = contract["cause"]
            if request.get("correction_scope") != "unchanged":
                request["correction_scope"] = contract["scope"]
        if changed:
            self.work.update(universe_state.put_work_group(self.store, changed))
        return prepared

    def accept_correction_completions(self, results):
        from . import universe_state

        for result in results:
            completed = result.get("correction_completion")
            item = self.work.get(result["symbol"])
            if not completed or item is None:
                continue
            if (completed["correction_id"], completed["source_sha256"]) != (
                item.get("correction_id"),
                result["source_sha256"],
            ):
                raise LineageError("correction completion does not match its obligation")
            # Completion is workflow bookkeeping, not a new obligation or a data generation.
            self.work[result["symbol"]] = universe_state.accept_correction_completion(
                self.store, result["symbol"], item, completed
            )
            event(
                "correction_completion_accepted",
                symbol=result["symbol"],
                correction_id=completed["correction_id"],
                source_generation=completed["source_sha256"],
            )

    def correction_completion_for(self, symbol, generation, source):
        """Carry a completed correction across certified unchanged retained history.

        The pricing producer's ancestry is authoritative. A later append cannot
        revoke a completed correction of the earlier span. This also recovers an
        interruption after append publication but before workflow acknowledgement;
        no receipt, payload or provider observation is opened again here.
        """
        item = self.work.get(symbol, {})
        completed = item.get("correction_completion") or {}
        if completed.get("correction_id") != item.get("correction_id") or not completed:
            return None
        if completed.get("source_sha256") == generation:
            return completed
        contract = item.get("pricing_correction") or {}
        end = contract.get("end_exclusive")
        request = source.get("latest_request") or {}
        if not generation or not end or item.get("invalidate_pricing") or request.get("start", "") < end:
            return None
        for ancestor in source.get("history_changes", ()):
            if ancestor.get("source_sha256") != completed.get("source_sha256"):
                continue
            changed = ancestor.get("changed_from")
            if changed is None or changed >= end[:10]:
                return {**completed, "source_sha256": generation}
        return None

    def carry_correction_completions(self, results):
        """Accept already-published append ancestry before lineage consumes it."""
        carried = []
        for result in results:
            if result.get("mode") != "append" or not result.get("source_sha256"):
                continue
            completed = self.correction_completion_for(
                result["symbol"],
                result["source_sha256"],
                {"latest_request": result.get("request"), "history_changes": result.get("history_changes", ())},
            )
            if completed and completed != self.work[result["symbol"]].get("correction_completion"):
                carried.append({**result, "correction_completion": completed})
        self.accept_correction_completions(carried)

    def fail(self, symbols, outcome, *, stage):
        """Include maintenance and newly discovered dependencies in durable retries."""
        from . import universe_state

        if isinstance(outcome, WorkDeferred):
            return
        if isinstance(outcome, RegistrationIncomplete):
            self.blocked.update(symbols)
            self.failures.append({"symbols": sorted(symbols), "stage": stage, "error": str(outcome)})
            return
        if isinstance(outcome, Exception):
            outcome = {
                "error": str(outcome),
                "outcome": failure_outcome(outcome),
                "request_identifiers": getattr(outcome, "request_identifiers", []),
            }
        detail = outcome.get("outcome", {})
        classification = detail.get("outcome") if isinstance(detail, dict) else detail
        status = (
            "blocked"
            if classification in {"access_denied", "symbol_unavailable", "invalid_symbol"}
            or outcome.get("status") == "blocked"
            else "retry"
        )
        requirements = {}
        for symbol in sorted(set(symbols)):
            if symbol in self.blocked:
                continue
            item = self.work.get(symbol)
            if item is None:
                item = obligation(
                    {
                        "reasons": ["unfinished_" + stage],
                        "end_exclusive": self.end,
                        "status": "pending",
                        "attempts": 0,
                        "acquire": stage not in {"lineage", "selection", "late_bar_check"},
                        "invalidate_pricing": False,
                        "refresh_metadata": stage in {"lineage", "selection"},
                        "evidence_fingerprint": None,
                    }
                )
            if stage in {"history_check", "history_correction", "append"}:
                item = {**item, "required_history_check": True, "obligation_id": uuid4().hex}
            requirements[symbol] = item
        if requirements:
            accepted = universe_state.put_work_group(self.store, requirements)
            self.work.update(accepted)
            outcomes = {}
            with universe_state.outcome_batch(self.store) as batch:
                for symbol, item in accepted.items():
                    outcomes[symbol] = universe_state.record_outcome(
                        self.store,
                        symbol,
                        status=status,
                        outcome={**outcome, "stage": stage},
                        attempts=1,
                        batch=batch,
                        expected=item,
                    )
            self.work.update(outcomes)
        for symbol in requirements:
            self.blocked.add(symbol)
            self.failures.append({"symbol": symbol, "stage": stage, **outcome})
        self.restricted_identifiers.update(outcome.get("request_identifiers", []))

    def record_results(self, value, *, stage):
        for item in value.get("results", []):
            if item["status"] in {"failed", "blocked", "waiting_for_pricing"}:
                self.fail(item.get("affected", [item["symbol"]]), item, stage=stage)
