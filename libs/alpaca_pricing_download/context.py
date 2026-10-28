"""Store and package compatibility accepted for one pricing operation."""

from dataclasses import dataclass
from pathlib import Path

from .contract import DownloadError, validate_version
from .storage import initialize_store


@dataclass(frozen=True)
class PricingContext:
    store: Path


def accept_context(store, context=None):
    store = Path(store).absolute()
    if context is not None:
        if context.store != store:
            raise DownloadError("pricing context belongs to another store")
        # This operation already accepted this store and package contract.
        # DO NOT rerun initialization or ancestor-path checks in each worker;
        # publication generation checks remain with the selected ticker owner.
        return context
    validate_version()
    initialize_store(store)
    return PricingContext(store)
