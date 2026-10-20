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
        self.index = None
        self.summary = None
        self._pricing = None

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


def accept_context(store, context=None, work_policy=None):
    if context is None and work_policy is not None:
        context = work_policy.context
    if context is None:
        context = OperationContext(store)
    if context.store != Path(store).absolute():
        raise LineageError("lineage context belongs to another store")
    return context
