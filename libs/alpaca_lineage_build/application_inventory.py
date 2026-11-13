"""Retired fresh application-inventory publication and acknowledgement.

A frozen journal remains installable by storage.recover under its recorded
provenance. None of these names can prepare new fact-based work or reopen that
completed decision.
"""

from .contract import LineageError


def _reject():
    raise LineageError("fact-application inventory is retired; use explicit generic lineage recovery")


def prepare(store, index, receipts, receipt_generations, pricing_generations, required_applications):
    _reject()


def publish_locked(store, root, index, plan, pricing_generations, required_applications):
    _reject()


def accept_metadata_completion(policy, index, plan, expected_work, pricing_generations):
    _reject()
