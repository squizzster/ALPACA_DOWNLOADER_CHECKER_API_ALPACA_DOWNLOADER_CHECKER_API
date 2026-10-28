"""Accepted update obligations and restrictions shared by every processing phase."""

from uuid import uuid4

from libs.alpaca_pricing_download.contract import HISTORY_START, failure_outcome
from libs.alpaca_security_lineage.lineage import compact_asset

from .contract import LineageError, document_digest, reusable_decision
from .selection import DependencyGraph


def obligation(item, previous=None):
    """An unfinished contract survives reselection, including catalogue absence."""
    previous = previous or {}
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
        "required_group",
    )
    if previous.get("obligation_id") and all(item.get(k) == previous.get(k) for k in keys):
        return {**item, "obligation_id": previous["obligation_id"]}
    return {**item, "obligation_id": uuid4().hex}


def acquisition_fingerprint(item):
    """Older accepted obligations used one fingerprint for both requirements."""
    return item.get("acquisition_evidence_fingerprint", item.get("evidence_fingerprint"))


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
        and (not item.get("correction_id") or projection.get("history_correction_id") == item["correction_id"])
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

    def __init__(self, store, index, work, deferred, end, *, context=None):
        self.store, self.end = store, end
        self.index = index
        self.context = context
        self.work = {s: item for s, item in work.items() if item.get("status") != "complete"}
        self.graph = work_graph(index["symbols"], self.work)
        self.required_groups = {}
        for symbol, item in self.work.items():
            group = set(item.get("required_group", [])) | {symbol}
            for member in group:
                self.required_groups.setdefault(member, set()).update(group)
        self.blocked = {item["symbol"] for item in deferred}
        self.failures = []
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
            and item.get("required_history_check")
            and not item.get("refresh_metadata")
            and self.history_approved.get(symbol) == entry["source_sha256"]
            and reusable_decision(entry)
            and set(item.get("required_group", ())) <= set(entry["group_members"])
            and not pending.keys() & set(entry["group_members"])
        )
        return satisfying_entry(symbol, item, entries, pending, approved=approved)

    def accept_completion(self, receipts, entries, completed):
        """Keep the producer's input approval bound to its selected complete group."""
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
        self, symbol, target, *, reason, acquire=False, refresh=False, group=(), history_check=False, start=None
    ):
        """Persist an obligation before its first request, independently of registration."""
        from libs.alpaca_pricing_download.contract import timestamp
        from libs.alpaca_pricing_download.download import request_start

        from . import universe_state

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
            self.work[symbol] = universe_state.put_work(self.store, symbol, item)
            linked = set(item["required_group"]) | {symbol}
            for member in linked:
                self.required_groups.setdefault(member, set()).update(linked)
            self.graph.update(self._group_entries(linked))
            if required > self.end:
                self.blocked.update(linked)
        return item

    def require_dependencies(self, group, target, required=(), *, start=None):
        """Keep acquisition and the complete group even if later evidence omits the link."""
        group = self.graph.closure(set(group))
        for symbol in sorted(group):
            self.require(
                symbol,
                target,
                reason="dependency_reconciliation",
                acquire=symbol in required,
                refresh=symbol not in self.index["symbols"],
                group=group,
                start=start,
            )

    def require_maintenance(self, entries, target):
        for entry in entries:
            if target and entry["end_exclusive"][:10] < target:
                self.require(entry["symbol"], target, reason="advance_summary_coverage", history_check=True)

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
        for symbol, item in self.work.items():
            entry = self.satisfying_entry(symbol, index["symbols"], pending)
            if symbol not in self.blocked and entry:
                universe_state.record_outcome(
                    self.store,
                    symbol,
                    status="complete",
                    outcome={"lineage_generation": entry["sha256"], "source_generation": entry["source_sha256"]}
                    if symbol in index["symbols"]
                    else {"lineage_generation": entry["sha256"], "outcome": "unavailable_auxiliary_pricing"},
                )
                self.pending_work.discard(symbol)

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

    def decision_inputs_satisfied(self, sources):
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
                    not item.get("correction_id") or source.get("history_correction_id") == item["correction_id"]
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
            if item.get("correction_id") and source.get("history_correction_id") != item["correction_id"]:
                continue
            accepted[symbol] = symbol
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
        """Persist a discovered retained-span correction before its acquisition."""
        from . import universe_state

        for request in prepared:
            symbol = request["symbol"]
            prior = self.work.get(symbol, {})
            target = request["end_exclusive"][:10]
            correction = prior.get("correction_id")
            if correction is None or request.get("history_correction_id") == correction:
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
                    "acquire": prior.get("acquire", False),
                    "refresh_metadata": prior.get("refresh_metadata", False),
                    "invalidate_pricing": prior.get("invalidate_pricing", False),
                    "evidence_fingerprint": prior.get("evidence_fingerprint"),
                },
                prior,
            )
            self.work[symbol] = universe_state.put_work(self.store, symbol, item)
            request["required_correction_id"] = correction

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
                        "acquire": stage not in {"lineage", "selection"},
                        "invalidate_pricing": False,
                        "refresh_metadata": stage in {"lineage", "selection"},
                        "evidence_fingerprint": None,
                    }
                )
            if stage in {"history_check", "history_correction", "append"}:
                item = {**item, "required_history_check": True, "obligation_id": uuid4().hex}
            universe_state.put_work(self.store, symbol, item)
            self.work[symbol] = universe_state.record_outcome(
                self.store, symbol, status=status, outcome={**outcome, "stage": stage}, attempts=1
            )
            self.blocked.add(symbol)
            self.failures.append({"symbol": symbol, "stage": stage, **outcome})
        self.restricted_identifiers.update(outcome.get("request_identifiers", []))

    def record_results(self, value, *, stage):
        for item in value.get("results", []):
            if item["status"] in {"failed", "blocked", "waiting_for_pricing"}:
                self.fail(item.get("affected", [item["symbol"]]), item, stage=stage)
