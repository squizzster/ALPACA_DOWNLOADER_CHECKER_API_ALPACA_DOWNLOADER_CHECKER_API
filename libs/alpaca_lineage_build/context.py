"""Accepted package contracts and coherent selectors for one operation."""

from pathlib import Path

from libs.alpaca_pricing_download.check_summary import refresh_snapshot
from libs.alpaca_pricing_download.context import accept_context as pricing_context
from libs.common.debug_trace import traced

from . import storage
from .contract import LineageError, validate_version


class OperationContext:
    # TRUST BOUNDARY: these are accepted producer selectors for one operation,
    # not a second source of truth. Refresh their compact revisions after a
    # publication; DO NOT authenticate all receipts/payloads to refresh a view.
    def __init__(self, store):
        validate_version()
        self.store = Path(store).absolute()
        self._accepted_store = None
        self._correlation_policy = None
        self._authorized_policy = None
        self._lineage_recovery_complete = False
        self.index = None
        self.summary = None
        self.serving = None
        self._pricing = None

    @traced("accept_store")
    def accept_store(self):
        # Store format and ancestor-path compatibility are immutable for this
        # operation. Lend that acceptance to each owning stage; ticker pricing
        # generations and publication selectors retain their own checks.
        if self._accepted_store is None:
            self._accepted_store = storage.accept_store(self.store)
        return self._accepted_store

    @traced("accept_policy")
    def accept_correlation_policy(self):
        # Import the YAML compiler only after the caller has completed frozen
        # journal recovery. Receipt readers and recovery stay independent of
        # PyYAML and the executable policy engine.
        from .correlation_policy import authorize_production_policy, load_authorized_policy

        if self._authorized_policy is None:
            if self._correlation_policy is None:
                # This producer returns an already authorized immutable policy.
                # Trust that completed handoff; do not authorize it a second time.
                self._correlation_policy = load_authorized_policy()
                self._authorized_policy = self._correlation_policy
            else:
                # Tests and explicit adapters may inject a compiled candidate.
                # It crosses the authorization boundary exactly once before use.
                self._authorized_policy = authorize_production_policy(self._correlation_policy)
        return self._authorized_policy

    @traced("lineage_recovery")
    def recover_lineage(self, root):
        # Recovery belongs to the journal owner and precedes new policy work.
        # Reusing this completed handoff must not reopen the journal per group.
        if self._lineage_recovery_complete:
            return False
        if Path(root).absolute() != self.accept_store().root:
            raise LineageError("lineage context belongs to another store")
        recovered = storage.recover(self.store, root)
        self._lineage_recovery_complete = True
        if recovered:
            self.index = None
        return recovered

    def pricing(self):
        self._pricing = pricing_context(self.store, self._pricing)
        return self._pricing

    @traced("select_index")
    def select_index(self, root):
        # The caller holds the existing publication gate.
        if Path(root).absolute() != self.accept_store().root:
            raise LineageError("lineage context belongs to another store")
        self.index = storage.refresh_index(root, self.index)
        return self.index

    @traced("select_summary")
    def select_summary(self):
        # Pricing baselines and lineage membership are already joined by their
        # owners; downstream code must not rediscover eligible ticker paths.
        self.accept_store()
        self.summary = refresh_snapshot(self.store, self.summary)
        return self.summary

    @traced("select_serving")
    def select_serving(self, *, recover=False):
        from libs.alpaca_serving_catalogue.storage import refresh_controls

        self.accept_store()
        self.serving = refresh_controls(self.store, self.serving, recover=recover)
        return self.serving


def accept_context(store, context=None, work_policy=None):
    # A caller-supplied context carries completed acceptance, not an invitation
    # to rerun it. The store binding prevents lending facts across stores.
    if context is None and work_policy is not None:
        context = work_policy.context
    if context is None:
        context = OperationContext(store)
    if context.store != Path(store).absolute():
        raise LineageError("lineage context belongs to another store")
    return context


def recover_and_accept_correlation_policy(store, context):
    """Finish a frozen lineage journal, then authorize all new live work."""
    if not context._lineage_recovery_complete:
        with storage.writer_lock(Path(store).absolute(), accepted_store=context.accept_store()) as root:
            context.recover_lineage(root)
    return context.accept_correlation_policy()
