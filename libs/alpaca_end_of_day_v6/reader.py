"""V6 consumer adapter over the established public history semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_reader.history import (
    _read_alpaca_end_of_day_security_history_from_master,
)
from libs.alpaca_end_of_day_security_correlation import AuthenticatedSecurityMaster

from .common import assert_publication_complete, publication_lock, read_json_object
from .correlation import load_correlation
from .errors import V6Error
from .paths import current_store_root
from .source import read_monthly_adjustment_rows
from .warnings import load_warnings


def _correlation_loader(store_root: Path, symbol: str):
    return load_correlation(store_root, symbol)


def _warning_loader(store_root: Path, symbol: str, correlation):
    return load_warnings(store_root, symbol)


def _source_reader(
    store_root: Path,
    symbol: str,
    source_descriptor: dict[str, Any],
    payload_cache: dict[Path, tuple[tuple[str, ...], ...]] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    adjustment = source_descriptor.get("adjustment")
    rows = read_monthly_adjustment_rows(
        store_root,
        symbol,
        adjustment,
        payload_cache=payload_cache,
    )
    filename = source_descriptor.get("data_file")
    manifest = {
        "file": filename,
        "sha256": source_descriptor.get("download_sha256"),
        "rows": source_descriptor.get("rows"),
        "symbol": symbol,
        "adjustment": adjustment,
        **{
            field: source_descriptor.get(field)
            for field in ("feed", "timeframe", "request_from", "request_to", "asof")
        },
    }
    return [dict(row) for row in rows], manifest, source_descriptor.get("pipeline_state")


def _integrity_failure_reader(
    _symbol_directory: Path,
    _symbol: str,
    _source: dict[str, Any],
) -> None:
    return None


@dataclass
class PublishedHistoryInputs:
    """Run-scoped cache of already-published reader inputs."""

    correlations: dict[str, Any] = field(default_factory=dict)
    warnings: dict[str, Any] = field(default_factory=dict)
    sources: dict[tuple[str, str], tuple[list[dict[str, str]], dict[str, Any], str]] = field(
        default_factory=dict
    )
    payloads: dict[Path, tuple[tuple[str, ...], ...]] = field(default_factory=dict)

    def seed_pricing_input(self, pricing_input: Any) -> None:
        """Reuse a pricing view already consumed by an earlier pipeline boundary."""
        for filename, download in pricing_input.downloads_by_filename.items():
            self.sources[(pricing_input.symbol, filename)] = (
                download.rows,
                download.manifest,
                download.pipeline_state,
            )

    def correlation(self, store_root: Path, symbol: str):
        if symbol not in self.correlations:
            self.correlations[symbol] = _correlation_loader(store_root, symbol)
        return self.correlations[symbol]

    def warning(self, store_root: Path, symbol: str, correlation):
        if symbol not in self.warnings:
            self.warnings[symbol] = _warning_loader(store_root, symbol, correlation)
        return self.warnings[symbol]

    def source(self, store_root: Path, symbol: str, descriptor: dict[str, Any]):
        key = (symbol, descriptor.get("data_file"))
        if key not in self.sources:
            self.sources[key] = _source_reader(
                store_root,
                symbol,
                descriptor,
                self.payloads,
            )
        return self.sources[key]


def read_history_from_master(
    store_root: Path | str,
    master: AuthenticatedSecurityMaster,
    master_id: str,
    adjustment: str,
    *,
    inputs: PublishedHistoryInputs | None = None,
) -> dict[str, Any]:
    inputs = PublishedHistoryInputs() if inputs is None else inputs
    return _read_alpaca_end_of_day_security_history_from_master(
        master_id,
        adjustment,
        repository_root=Path(store_root),
        master=master,
        correlation_authenticator=inputs.correlation,
        warning_authenticator=inputs.warning,
        source_reader=inputs.source,
        integrity_failure_reader=_integrity_failure_reader,
        trust_published_inputs=True,
    )


def read_v6_history(
    master_id: str,
    adjustment: str,
    *,
    repository_root: Path | str | None = None,
    master: AuthenticatedSecurityMaster | None = None,
    inputs: PublishedHistoryInputs | None = None,
) -> dict[str, Any]:
    from .catalogue import _load_catalogue_store

    repository = Path.cwd() if repository_root is None else Path(repository_root)
    root = current_store_root(repository)
    with publication_lock(root, exclusive=False):
        assert_publication_complete(root)
        if master is None:
            master = _load_catalogue_store(root)
        else:
            catalogue_receipt = read_json_object(
                root / "catalogue" / "catalogue.manifest.json",
                "v6 catalogue receipt",
            )
            current = catalogue_receipt.get("security_master")
            if not isinstance(current, dict) or current.get("generation_sha256") != master.receipt.get(
                "security_master_generation_sha256"
            ):
                raise V6Error("the published v6 catalogue changed after the reader opened")
        return read_history_from_master(
            root,
            master,
            master_id,
            adjustment,
            inputs=inputs,
        )


__all__ = ["PublishedHistoryInputs", "read_history_from_master", "read_v6_history"]
