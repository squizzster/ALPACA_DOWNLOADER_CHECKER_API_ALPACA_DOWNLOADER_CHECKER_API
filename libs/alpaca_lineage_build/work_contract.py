"""Accepted update obligations and restrictions shared by every processing phase."""

from uuid import uuid4

from libs.alpaca_pricing_download.contract import failure_outcome

from .contract import LineageError, document_digest
from .selection import DependencyGraph


def obligation(item, previous=None):
    """An unfinished contract survives reselection, including catalogue absence."""
    previous = previous or {}
    keys = (
        "end_exclusive",
        "evidence_fingerprint",
        "acquisition_evidence_fingerprint",
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


def pricing_target(item):
    return item.get("pricing_end_exclusive", item["end_exclusive"])


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


def satisfied(item, entry, pending):
    if not entry or not item.get("obligation_id"):
        return False
    projection = entry.get("projection") or {}
    return (
        not pending
        and projection.get("processed_through", "")[:10] >= pricing_target(item)
        and projection.get("update_obligation_id") == item["obligation_id"]
        and (
            not item.get("invalidate_pricing")
            or projection.get("acquisition_evidence_sha256") == acquisition_fingerprint(item)
        )
        and (not item.get("correction_id") or projection.get("history_correction_id") == item["correction_id"])
    )


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

    def __init__(self, store, index, work, deferred, end):
        self.store, self.end = store, end
        self.index = {**index, "symbols": dict(index["symbols"])}
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
        self.restricted_identifiers = {
            identifier
            for symbol in self.blocked
            for identifier in work.get(symbol, {}).get("outcome", {}).get("request_identifiers", [])
        }
        self.current_group = set()

    def update_index(self, entries):
        self.index["symbols"].update(entries)
        self.graph.update(self._group_entries(entries))

    def _group_entries(self, symbols):
        return {
            s: {
                "dependencies": set(self.index["symbols"].get(s, {}).get("dependencies", []))
                | self.required_groups.get(s, set()),
                "asset_ids": self.index["symbols"].get(s, {}).get("asset_ids", []),
            }
            for s in symbols
        }

    def require(self, symbol, target, *, reason, acquire=False, refresh=False, group=(), history_check=False):
        """Persist an obligation before its first request, independently of registration."""
        from . import universe_state

        prior = self.work.get(symbol, {})
        required = max(target, pricing_target(prior)) if prior else target
        item = obligation(
            {
                **prior,
                "reasons": sorted(set(prior.get("reasons", [])) | {reason}),
                "end_exclusive": max(prior.get("end_exclusive", target), target),
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
            universe_state.put_work(self.store, symbol, item)
            self.work[symbol] = item
            linked = set(item["required_group"]) | {symbol}
            for member in linked:
                self.required_groups.setdefault(member, set()).update(linked)
            self.graph.update(self._group_entries(linked))
            if required > self.end:
                self.blocked.update(linked)
        return item

    def require_dependencies(self, group, target, required=()):
        """Keep acquisition and the complete group even if later evidence omits the link."""
        group = self.graph.closure(set(group))
        for symbol in sorted(group):
            self.require(
                symbol,
                target,
                reason="dependency_reconciliation",
                acquire=symbol in required,
                refresh=True,
                group=group,
            )

    def require_maintenance(self, entries, target):
        for entry in entries:
            if target and entry["asof"] < target:
                self.require(entry["symbol"], target, reason="advance_summary_coverage", history_check=True)

    def acknowledge(self):
        """Only the lineage publisher's stamped projection can satisfy intentions."""
        from libs.alpaca_pricing_download.update_checks import read_checks

        from . import storage, universe_state

        root = self.store / "end_of_day/lineage"
        self.pending_work = set(self.work)
        if not root.exists():
            return
        with storage.publication_lock(root):
            if (root / ".pending.json").exists():
                return
            index = storage.read_index(root)
            pending = read_checks(self.store)["checks"]
        for symbol, item in self.work.items():
            if symbol not in self.blocked and satisfied(item, index["symbols"].get(symbol), symbol in pending):
                entry = index["symbols"][symbol]
                universe_state.record_outcome(
                    self.store,
                    symbol,
                    status="complete",
                    outcome={"lineage_generation": entry["sha256"], "source_generation": entry["source_sha256"]},
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
        return any(
            symbol in self.work
            and self.work[symbol].get("refresh_metadata")
            and (previous.get(symbol) or {}).get("update_obligation_id") != self.work[symbol]["obligation_id"]
            for symbol in group
        )

    def stamp(self, receipts, sources):
        """Called under group pricing locks, before the lineage owner publishes."""
        self.check(receipts)
        for symbol, receipt in receipts.items():
            item = self.work.get(symbol)
            if item is None:
                continue
            if set(item.get("required_group", [])) - receipts.keys():
                continue
            source = sources[symbol]
            if item.get("invalidate_pricing") and source.get(
                "acquisition_evidence_sha256"
            ) != acquisition_fingerprint(item):
                raise WorkDeferred(f"{symbol}: required identity-related pricing replacement is incomplete")
            if source["latest_request"]["end_exclusive"][:10] < pricing_target(item):
                continue  # Maintenance still owes the remaining interval.
            if item.get("required_history_check") and self.history_approved.get(symbol) != document_digest(
                source
            ):
                continue
            if item.get("correction_id") and source.get("history_correction_id") != item["correction_id"]:
                continue
            receipt["update_obligation_id"] = item["obligation_id"]

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
                    "status": "pending",
                    "required_history_check": require_check or prior.get("required_history_check", False),
                    "correction_id": correction,
                    "acquire": prior.get("acquire", False),
                    "refresh_metadata": prior.get("refresh_metadata", False),
                    "invalidate_pricing": prior.get("invalidate_pricing", False),
                    "evidence_fingerprint": prior.get("evidence_fingerprint"),
                },
                prior,
            )
            self.work[symbol] = item
            universe_state.put_work(self.store, symbol, item)
            request["required_correction_id"] = correction

    def requires_publication(self, previous):
        return any(
            symbol in self.work
            and (receipt or {}).get("update_obligation_id") != self.work[symbol]["obligation_id"]
            for symbol, receipt in previous.items()
        )

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
            self.work[symbol] = item
            universe_state.put_work(self.store, symbol, item)
            universe_state.record_outcome(
                self.store, symbol, status=status, outcome={**outcome, "stage": stage}, attempts=1
            )
            self.blocked.add(symbol)
            self.failures.append({"symbol": symbol, "stage": stage, **outcome})
        self.restricted_identifiers.update(outcome.get("request_identifiers", []))

    def record_results(self, value, *, stage):
        for item in value.get("results", []):
            if item["status"] in {"failed", "blocked", "waiting_for_pricing"}:
                self.fail(item.get("affected", [item["symbol"]]), item, stage=stage)
