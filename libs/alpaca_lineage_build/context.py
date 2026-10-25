"""Accepted package contracts and coherent selectors for one operation."""

from pathlib import Path

from libs.alpaca_pricing_download.check_summary import refresh_snapshot
from libs.alpaca_pricing_download.context import accept_context as pricing_context

from . import storage
from .contract import LineageError, validate_version


class OperationContext:
    def __init__(self, store):
        validate_version()
        self.store = Path(store).absolute()
        self._correlation_policy = None
        self._lineage_recovery_complete = False
        self.index = None
        self.summary = None
        self.serving = None
        self._pricing = None

    def accept_correlation_policy(self):
        # Import the YAML compiler only after the caller has completed frozen
        # journal recovery. Receipt readers and recovery stay independent of
        # PyYAML and the executable policy engine.
        from .correlation_policy import authorize_production_policy, load_authorized_policy

        if self._correlation_policy is None:
            self._correlation_policy = load_authorized_policy()
        return authorize_production_policy(self._correlation_policy)

    def recover_lineage(self, root):
        if self._lineage_recovery_complete:
            return False
        recovered = storage.recover(self.store, root)
        self._lineage_recovery_complete = True
        if recovered:
            self.index = None
        return recovered

    def pricing(self):
        self._pricing = pricing_context(self.store, self._pricing)
        return self._pricing

    def select_index(self, root):
        # The caller holds the existing publication gate.
        self.index = storage.refresh_index(root, self.index)
        return self.index

    def select_summary(self):
        self.summary = refresh_snapshot(self.store, self.summary)
        return self.summary

    def select_serving(self, *, recover=False):
        from libs.alpaca_serving_catalogue.storage import refresh_controls

        self.serving = refresh_controls(self.store, self.serving, recover=recover)
        return self.serving


def accept_context(store, context=None, work_policy=None):
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
        with storage.writer_lock(Path(store).absolute()) as root:
            context.recover_lineage(root)
    return context.accept_correlation_policy()
