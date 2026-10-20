"""Build and audit a generation without consulting another runtime data store."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import tempfile
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, TextIO

from alpaca_end_of_day import ADJUSTMENTS, DataLibrary
from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_end_of_day_security_correlation import CORRELATION_VERSION
from libs.alpaca_end_of_day_ticker_pipeline.orchestration import (
    resolve_authoritative_alpaca_symbol,
)
from libs.alpaca_end_of_day_v6.catalogue import authenticate_catalogue_store
from libs.alpaca_end_of_day_v6.common import (
    assert_publication_complete,
    canonical_sha256,
    publication_lock,
    read_json_object,
    replace_json,
    sha256_bytes,
    sha256_file,
)
from libs.alpaca_end_of_day_v6.correlation import authenticate_correlation
from libs.alpaca_end_of_day_v6.lineage import authenticate_lineage
from libs.alpaca_end_of_day_v6.pipeline import _request_source_view_batch, run_v6_pipeline
from libs.alpaca_end_of_day_v6.source import (
    SOURCE_MANIFEST,
    authenticate_monthly_source,
    initialize_v6_store,
)
from libs.alpaca_end_of_day_v6.validation import authenticate_validation
from libs.alpaca_end_of_day_v6.versions import (
    AVAILABILITY_VERSION,
    CATALOGUE_PUBLICATION_VERSION,
    CORRELATION_ENVELOPE_VERSION,
    IDENTITY_CANDIDATE_VERSION,
    LINEAGE_SOURCE_VERSION,
    SOURCE_VERSION,
    VALIDATION_VERSION,
    WARNING_ENVELOPE_VERSION,
)
from libs.alpaca_end_of_day_v6.warnings import authenticate_warnings
from libs.alpaca_end_of_day_warning_pipeline import WARNING_PIPELINE_VERSION
from libs.alpaca_recovery_registry import atomic_bytes
from libs.alpaca_security_lineage import AlpacaIdentityRequestRateLimiter
from libs.alpaca_security_lineage.settings import REPORT_VERSION
from libs.download_alpaca_end_of_day_bars import AlpacaRequestRateLimiter
from libs.download_alpaca_end_of_day_bars.settings import EARLIEST_REQUEST_TIMESTAMP

CLEAN_ROOM_RUN_VERSION = 3
SUPPORTED_RUN_VERSIONS = (1, 2, CLEAN_ROOM_RUN_VERSION)
CLEAN_ROOM_RUN_BOUNDARY = "alpaca_end_of_day_clean_room_run"
RUN_STATE_DIRECTORY = "clean_room_run"
RUN_MANIFEST = "manifest.json"
AUDIT_REPORT = "audit.json"
COMPLETION_RECEIPT = "completion.json"
UNIVERSE_SNAPSHOT = "universe.snapshot.csv"
INITIALIZATION_PENDING = "initialization.pending.json"
SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]*")
_RESOURCE_ROOT = Path(__file__).resolve().parents[2] / "resources"
_CHANGE_LOG = _RESOURCE_ROOT / "alpaca_clean_room_run_change_log.json"
SOURCE_PREFETCH_LEAD = 24
RESOLUTION_PREFETCH_LEAD = 128
SOURCE_REQUEST_BATCH_SIZE = 3


class CleanRoomError(RuntimeError):
    """Raised when clean-room construction or its accounting is incoherent."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _validate_request_end(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise CleanRoomError(f"invalid exclusive request end: {value!r}") from error
    if (
        not value.endswith("T00:00:00Z")
        or parsed.tzinfo is None
        or parsed.hour
        or parsed.minute
        or parsed.second
        or parsed.microsecond
    ):
        raise CleanRoomError("clean-room request end must be an exclusive UTC midnight")


def _emit(output: TextIO | None, message: str) -> None:
    if output is not None:
        print(message, file=output, flush=True)


def _validate_change_log() -> tuple[dict[str, Any], ...]:
    try:
        entries = json.loads(_CHANGE_LOG.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CleanRoomError(f"could not load clean-room changelog: {error}") from error
    if not isinstance(entries, list) or len(entries) != CLEAN_ROOM_RUN_VERSION:
        raise CleanRoomError("clean-room changelog length must equal its owning version")
    for position, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise CleanRoomError(f"invalid clean-room changelog entry {position}")
    return tuple(entries)


def _contract_versions() -> dict[str, int]:
    return {
        "source": SOURCE_VERSION,
        "validation": VALIDATION_VERSION,
        "lineage_source": LINEAGE_SOURCE_VERSION,
        "lineage_report": REPORT_VERSION,
        "correlation_envelope": CORRELATION_ENVELOPE_VERSION,
        "correlation_transform": CORRELATION_VERSION,
        "warning_envelope": WARNING_ENVELOPE_VERSION,
        "warning_transform": WARNING_PIPELINE_VERSION,
        "validation_transform": PIPELINE_VERSION,
        "identity_candidate": IDENTITY_CANDIDATE_VERSION,
        "availability": AVAILABILITY_VERSION,
        "catalogue_publication": CATALOGUE_PUBLICATION_VERSION,
    }


def load_universe(path: Path | str) -> tuple[str, ...]:
    """Load the one-record tracked universe without interpreting another data store."""
    return _capture_universe(Path(path))[1]


def _capture_universe(path: Path) -> tuple[bytes, tuple[str, ...]]:
    try:
        content = path.read_bytes()
    except OSError as error:
        raise CleanRoomError(f"could not read universe {path}: {error}") from error
    return content, _universe_labels(content)


def _universe_labels(content: bytes) -> tuple[str, ...]:
    try:
        with io.StringIO(content.decode("utf-8"), newline="") as stream:
            rows = list(csv.reader(stream))
    except (UnicodeError, csv.Error) as error:
        raise CleanRoomError(f"could not decode universe: {error}") from error
    if len(rows) != 1 or not rows[0]:
        raise CleanRoomError("clean-room universe must contain exactly one nonempty CSV record")
    symbols = tuple(value.strip().upper() for value in rows[0])
    invalid = [symbol for symbol in symbols if SYMBOL_PATTERN.fullmatch(symbol) is None]
    if invalid:
        raise CleanRoomError(f"invalid universe label: {invalid[0]!r}")
    if len(symbols) != len(set(symbols)):
        duplicate = next(symbol for symbol, count in Counter(symbols).items() if count > 1)
        raise CleanRoomError(f"duplicate universe label: {duplicate}")
    if symbols != tuple(sorted(symbols)):
        raise CleanRoomError("clean-room universe must be sorted exactly")
    return symbols


def _check_snapshot(manifest, content, labels):
    expected = manifest["universe"]
    if (
        sha256_bytes(content) != expected["source_sha256"]
        or canonical_sha256(list(labels)) != expected["ordered_labels_sha256"]
        or len(labels) != expected["label_count"]
    ):
        raise CleanRoomError("original universe is required to pin this build; registry has changed")


def pin_universe(store_root, universe_path):
    """Preserve original bytes and run identity, including for incompatible old runs.

    This explicit input migration does not make old producer contracts compatible.
    It reads the run manifest and its universe only, never runtime payloads.
    """
    store = Path(store_root)
    state = _state_root(store)
    with publication_lock(store, exclusive=True):
        _recover_initialization(state)
        manifest = read_json_object(state / RUN_MANIFEST, "clean-room run manifest")
        snapshot = state / UNIVERSE_SNAPSHOT
        source = snapshot if snapshot.exists() else Path(universe_path)
        content, labels = _capture_universe(source)
        _check_snapshot(manifest, content, labels)
        if not snapshot.exists():
            _discard_initialization_staging(state)
            _publish_run_input(state, manifest, content)
        return {"run_id": manifest["run_id"], "snapshot": str(snapshot), "label_count": len(labels)}


def resume_clean_room_generation(store_root, **kwargs):
    store = Path(store_root)
    with publication_lock(store, exclusive=True):
        _recover_initialization(_state_root(store))
        manifest = read_json_object(_state_root(store) / RUN_MANIFEST, "clean-room run manifest")
        snapshot = _state_root(store) / UNIVERSE_SNAPSHOT
        if not snapshot.exists():
            raise CleanRoomError("pin the original universe before resuming this older build")
    return run_clean_room_generation(snapshot, store, request_end_timestamp=manifest["request"]["to"], **kwargs)


def _state_root(store_root: Path) -> Path:
    return store_root / RUN_STATE_DIRECTORY


def _state_file(store_root: Path, kind: str, position: int) -> Path:
    return _state_root(store_root) / kind / f"{position:06d}.json"


def _manifest_identity(
    universe_path: Path,
    symbols: Sequence[str],
    request_end_timestamp: str,
    universe_bytes: bytes,
) -> dict[str, Any]:
    return {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": CLEAN_ROOM_RUN_BOUNDARY,
        "mode": "provider_only_clean_room",
        "request": {
            "from": EARLIEST_REQUEST_TIMESTAMP,
            "to": request_end_timestamp,
            "to_is_exclusive": True,
        },
        "universe": {
            "source_file": universe_path.name,
            "source_sha256": sha256_bytes(universe_bytes),
            "ordered_labels_sha256": canonical_sha256(list(symbols)),
            "label_count": len(symbols),
        },
        "contracts": _contract_versions(),
        "input_policy": {
            "allowed": ["ordered_universe", "provider_responses", "repository_contracts"],
            "other_runtime_stores": "forbidden",
            "migration": "forbidden",
            "repair_from_prior_generation": "forbidden",
        },
    }


def _finish_initialization(state: Path, manifest: dict, content: bytes) -> None:
    """Caller holds the store lock; the manifest selects the captured input last."""
    path, snapshot = state / RUN_MANIFEST, state / UNIVERSE_SNAPSHOT
    if path.exists() and read_json_object(path, "clean-room run manifest") != manifest:
        raise CleanRoomError("initialization conflicts with the published clean-room identity")
    if snapshot.exists():
        if snapshot.read_bytes() != content:
            raise CleanRoomError("initialization conflicts with the immutable universe snapshot")
    else:
        atomic_bytes(snapshot, content)
    if not path.exists():
        replace_json(path, manifest)
    (state / INITIALIZATION_PENDING).unlink()


def _publish_run_input(state: Path, manifest: dict, content: bytes) -> None:
    replace_json(
        state / INITIALIZATION_PENDING,
        {
            "schema_version": CLEAN_ROOM_RUN_VERSION,
            "boundary": "alpaca_end_of_day_clean_room_initialization",
            "manifest": manifest,
            "universe_csv_utf8": content.decode("utf-8"),
        },
    )
    _finish_initialization(state, manifest, content)


def _discard_initialization_staging(state: Path) -> None:
    """Retire only this owner's abandoned atomic-write files under the store lock."""
    prefixes = tuple(f".{name}.tmp." for name in (INITIALIZATION_PENDING, UNIVERSE_SNAPSHOT, RUN_MANIFEST))
    for entry in state.iterdir():
        if entry.name.startswith(prefixes):
            if entry.is_symlink() or not entry.is_file():
                raise CleanRoomError(f"invalid initialization staging file: {entry}")
            entry.unlink()


def _recover_initialization(state: Path) -> None:
    pending = state / INITIALIZATION_PENDING
    if not pending.exists():
        return
    _discard_initialization_staging(state)
    intent = read_json_object(pending, "clean-room initialization")
    if (
        intent.get("schema_version") != 3
        or intent.get("boundary") != "alpaca_end_of_day_clean_room_initialization"
        or not isinstance(intent.get("manifest"), dict)
        or not isinstance(intent.get("universe_csv_utf8"), str)
    ):
        raise CleanRoomError("invalid clean-room initialization journal")
    manifest = intent["manifest"]
    content = intent["universe_csv_utf8"].encode("utf-8")
    labels = _universe_labels(content)
    _check_snapshot(manifest, content, labels)
    identity = {key: value for key, value in manifest.items() if key not in {"run_id", "created_at"}}
    if canonical_sha256(identity) != manifest.get("run_id"):
        raise CleanRoomError("invalid clean-room initialization identity")
    # Finish the accepted inputs even if a later invocation has other contracts
    # or a different target. Ordinary resume checks compatibility afterwards.
    _finish_initialization(state, manifest, content)


def _initialize_run(
    store_root: Path,
    universe_path: Path,
    request_end_timestamp: str,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    _validate_change_log()
    initialize_v6_store(store_root)
    with publication_lock(store_root, exclusive=True):
        state = _state_root(store_root)
        if state.is_symlink():
            raise CleanRoomError(f"clean-room state cannot be a symlink: {state}")
        _recover_initialization(state)
        path, snapshot = state / RUN_MANIFEST, state / UNIVERSE_SNAPSHOT
        document = read_json_object(path, "clean-room run manifest") if path.exists() else None
        source = snapshot if document is not None and snapshot.exists() else universe_path
        content, symbols = _capture_universe(source)
        expected_identity = _manifest_identity(universe_path, symbols, request_end_timestamp, content)
        if document is not None:
            # Pinning preserves the original run's schema and source name. This
            # compatibility does not permit a change to its producer contracts.
            if document.get("schema_version") in SUPPORTED_RUN_VERSIONS:
                expected_identity["schema_version"] = document["schema_version"]
                expected_identity["universe"]["source_file"] = document["universe"]["source_file"]
            expected_run_id = canonical_sha256(expected_identity)
            observed_identity = {key: document.get(key) for key in expected_identity}
            if observed_identity != expected_identity or document.get("run_id") != expected_run_id:
                raise CleanRoomError("existing clean-room run has a different immutable identity")
            if not isinstance(document.get("created_at"), str):
                raise CleanRoomError("existing clean-room run has no creation time")
            if not snapshot.exists():
                _discard_initialization_staging(state)
                _publish_run_input(state, document, content)
        else:
            allowed = {"store.manifest.json", ".publication.lock", "end_of_day", RUN_STATE_DIRECTORY}
            unexpected_root_entries = {entry.name for entry in store_root.iterdir() if entry.name not in allowed}
            source_entries = tuple((store_root / "end_of_day").iterdir())
            if unexpected_root_entries or source_entries:
                raise CleanRoomError(
                    "a new clean-room run requires a virgin monthly store; "
                    f"unexpected={sorted(unexpected_root_entries)!r}, source_count={len(source_entries)}"
                )
            state.mkdir(exist_ok=True)
            _discard_initialization_staging(state)
            unexpected = [entry.name for entry in state.iterdir() if entry.name != UNIVERSE_SNAPSHOT]
            if unexpected:
                raise CleanRoomError(f"unowned files precede the clean-room manifest: {sorted(unexpected)!r}")
            if snapshot.exists() and snapshot.read_bytes() != content:
                raise CleanRoomError("original input is required for an interrupted unjournaled snapshot")
            document = {
                **expected_identity,
                "run_id": canonical_sha256(expected_identity),
                "created_at": _utc_now(),
            }
            _publish_run_input(state, document, content)
        for kind in ("resolutions", "outcomes", "attempts"):
            directory = state / kind
            directory.mkdir(exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise CleanRoomError(f"clean-room {kind} path is not a directory")
        return document, symbols


def _validate_resolution(
    document: dict[str, Any],
    *,
    run_id: str,
    position: int,
    label: str,
) -> dict[str, Any]:
    resolution = document.get("resolution")
    if (
        document.get("schema_version") not in SUPPORTED_RUN_VERSIONS
        or document.get("boundary") != "alpaca_end_of_day_clean_room_resolution"
        or document.get("run_id") != run_id
        or document.get("position") != position
        or document.get("submitted_label") != label
        or not isinstance(resolution, dict)
        or resolution.get("submitted_symbol") != label
        or SYMBOL_PATTERN.fullmatch(str(resolution.get("authoritative_symbol"))) is None
        or document.get("resolution_sha256") != canonical_sha256(resolution)
    ):
        raise CleanRoomError(f"clean-room resolution is malformed: {label}")
    return resolution


def _load_or_resolve(
    store_root: Path,
    run_id: str,
    position: int,
    label: str,
    api_key_id: str,
    api_secret_key: str,
    identity_rate_limiter: AlpacaIdentityRequestRateLimiter,
    resolver: Callable[..., dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    path = _state_file(store_root, "resolutions", position)
    if path.exists():
        document = read_json_object(path, "clean-room resolution")
        resolution = _validate_resolution(
            document,
            run_id=run_id,
            position=position,
            label=label,
        )
        return resolution, document["resolution_sha256"]
    resolution = resolver(
        label,
        api_key_id,
        api_secret_key,
        request_rate_limiter=identity_rate_limiter,
    )
    if not isinstance(resolution, dict):
        raise CleanRoomError(f"provider returned a non-object resolution for {label}")
    document = {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_resolution",
        "run_id": run_id,
        "position": position,
        "submitted_label": label,
        "resolved_at": _utc_now(),
        "resolution_sha256": canonical_sha256(resolution),
        "resolution": resolution,
    }
    _validate_resolution(
        document,
        run_id=run_id,
        position=position,
        label=label,
    )
    replace_json(path, document)
    return resolution, document["resolution_sha256"]


def _outcome_content(document: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in document.items() if key != "outcome_sha256"}


def _validate_outcome(
    document: dict[str, Any],
    *,
    run_id: str,
    position: int,
    label: str,
) -> dict[str, Any]:
    status = document.get("status")
    if (
        document.get("schema_version") not in SUPPORTED_RUN_VERSIONS
        or document.get("boundary") != "alpaca_end_of_day_clean_room_outcome"
        or document.get("run_id") != run_id
        or document.get("position") != position
        or document.get("submitted_label") != label
        or status not in {"published", "rejected_validation", "canonical_reused"}
        or SYMBOL_PATTERN.fullmatch(str(document.get("canonical_symbol"))) is None
        or not isinstance(document.get("resolution_sha256"), str)
        or not isinstance(document.get("completed_at"), str)
        or document.get("outcome_sha256") != canonical_sha256(_outcome_content(document))
    ):
        raise CleanRoomError(f"clean-room outcome is malformed: {label}")
    if status == "canonical_reused" and (
        type(document.get("canonical_owner_position")) is not int
        or document.get("canonical_owner_status") not in {"published", "rejected_validation"}
    ):
        raise CleanRoomError(f"clean-room reused outcome is malformed: {label}")
    return document


def _write_outcome(store_root: Path, document: dict[str, Any]) -> dict[str, Any]:
    document = {
        **document,
        "completed_at": _utc_now(),
    }
    document["outcome_sha256"] = canonical_sha256(_outcome_content(document))
    _validate_outcome(
        document,
        run_id=document["run_id"],
        position=document["position"],
        label=document["submitted_label"],
    )
    replace_json(_state_file(store_root, "outcomes", document["position"]), document)
    return document


def _pipeline_outcome(
    *,
    run_id: str,
    position: int,
    label: str,
    canonical_symbol: str,
    resolution_sha256: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    source = result.get("source", {}).get(canonical_symbol)
    validation = result.get("validation", {}).get(canonical_symbol)
    if not isinstance(source, dict) or not isinstance(validation, dict):
        raise CleanRoomError(f"pipeline returned no source/validation receipt for {label}")
    admitted = result.get("admitted")
    status = "published" if admitted is True else "rejected_validation"
    if admitted not in {True, False}:
        raise CleanRoomError(f"pipeline returned no admission decision for {label}")
    generations: dict[str, Any] = {
        "source": source.get("source_generation_sha256"),
        "validation": validation.get("validation_generation_sha256"),
    }
    if admitted:
        lineage = result.get("lineage", {}).get(canonical_symbol)
        correlation = result.get("correlation", {}).get(canonical_symbol)
        warning = result.get("warnings", {}).get(canonical_symbol)
        catalogue = result.get("catalogue")
        if not all(isinstance(item, dict) for item in (lineage, correlation, warning, catalogue)):
            raise CleanRoomError(f"pipeline returned incomplete published boundaries for {label}")
        generations.update(
            {
                "lineage": lineage.get("lineage_generation_sha256"),
                "correlation": correlation.get("correlation_generation_sha256"),
                "warnings": warning.get("warning_pipeline_generation_sha256"),
                "candidate": result.get("identity_candidate", {}).get("candidate_generation_sha256"),
                "availability": result.get("availability", {}).get("availability_generation_sha256"),
                "catalogue": catalogue.get("catalogue_generation_sha256"),
                "public_catalogue": catalogue.get("security_master", {}).get("generation_sha256"),
            }
        )
    if any(not isinstance(value, str) or not value for value in generations.values()):
        raise CleanRoomError(f"pipeline returned an invalid generation identity for {label}")
    return {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_outcome",
        "run_id": run_id,
        "position": position,
        "submitted_label": label,
        "canonical_symbol": canonical_symbol,
        "resolution_sha256": resolution_sha256,
        "status": status,
        "generations": generations,
        "rejection": result.get("rejection"),
    }


def _attempt_count(store_root: Path, position: int) -> int:
    directory = _state_root(store_root) / "attempts"
    return len(tuple(directory.glob(f"{position:06d}.*.json")))


def _record_attempt_failure(
    store_root: Path,
    *,
    run_id: str,
    position: int,
    label: str,
    attempt: int,
    stage: str,
    error: Exception,
) -> None:
    document = {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_attempt",
        "run_id": run_id,
        "position": position,
        "submitted_label": label,
        "attempt": attempt,
        "stage": stage,
        "failed_at": _utc_now(),
        "error_type": type(error).__name__,
        "diagnostic": str(error),
    }
    path = _state_root(store_root) / "attempts" / f"{position:06d}.{attempt:03d}.json"
    replace_json(path, document)


def _resolve_with_retries(
    store_root: Path,
    *,
    run_id: str,
    position: int,
    label: str,
    api_key_id: str,
    api_secret_key: str,
    identity_rate_limiter: AlpacaIdentityRequestRateLimiter,
    resolver: Callable[..., dict[str, Any]],
    maximum_attempts: int,
    output: TextIO | None,
) -> tuple[dict[str, Any], str]:
    prior_attempts = _attempt_count(store_root, position)
    for local_attempt in range(1, maximum_attempts + 1):
        attempt = prior_attempts + local_attempt
        try:
            return _load_or_resolve(
                store_root,
                run_id,
                position,
                label,
                api_key_id,
                api_secret_key,
                identity_rate_limiter,
                resolver,
            )
        except Exception as error:
            _record_attempt_failure(
                store_root,
                run_id=run_id,
                position=position,
                label=label,
                attempt=attempt,
                stage="resolution",
                error=error,
            )
            if local_attempt == maximum_attempts:
                raise CleanRoomError(
                    f"{label}: resolution failed after {maximum_attempts} attempts: "
                    f"{type(error).__name__}: {error}"
                ) from error
            delay = min(2 ** (local_attempt - 1), 30)
            _emit(
                output,
                f"{label}: resolution attempt {attempt} failed "
                f"({type(error).__name__}: {error}); retrying in {delay}s",
            )
            time.sleep(delay)
    raise AssertionError("unreachable resolution retry state")


def _load_existing_outcomes(
    store_root: Path,
    run_id: str,
    symbols: Sequence[str],
) -> tuple[dict[int, dict[str, Any]], dict[str, tuple[int, dict[str, Any]]]]:
    outcomes = {}
    owners: dict[str, tuple[int, dict[str, Any]]] = {}
    for position, label in enumerate(symbols):
        path = _state_file(store_root, "outcomes", position)
        if not path.exists():
            continue
        outcome = _validate_outcome(
            read_json_object(path, "clean-room outcome"),
            run_id=run_id,
            position=position,
            label=label,
        )
        outcomes[position] = outcome
        if outcome["status"] in {"published", "rejected_validation"}:
            canonical = outcome["canonical_symbol"]
            if canonical in owners:
                raise CleanRoomError(f"canonical symbol was independently calculated twice: {canonical}")
            owners[canonical] = (position, outcome)
    return outcomes, owners


def _load_completed_run(store_root: Path, run_id: str) -> dict[str, Any] | None:
    completion_path = _state_root(store_root) / COMPLETION_RECEIPT
    if not completion_path.exists():
        return None
    completion = read_json_object(completion_path, "clean-room completion receipt")
    if (
        completion.get("schema_version") not in SUPPORTED_RUN_VERSIONS
        or completion.get("boundary") != "alpaca_end_of_day_clean_room_completion"
        or completion.get("run_id") != run_id
        or completion.get("status") != "passed"
        or re.fullmatch(r"[a-f0-9]{64}", str(completion.get("audit_sha256"))) is None
    ):
        raise CleanRoomError("clean-room completion receipt is malformed")
    return completion


def _directory_symbols(path: Path) -> set[str]:
    if not path.is_dir() or path.is_symlink():
        raise CleanRoomError(f"expected a regular boundary directory: {path}")
    symbols = set()
    for child in path.iterdir():
        if child.name.startswith("."):
            continue
        if child.is_symlink() or not child.is_dir() or SYMBOL_PATTERN.fullmatch(child.name) is None:
            raise CleanRoomError(f"unowned entry in boundary directory: {child}")
        symbols.add(child.name)
    return symbols


def audit_clean_room_generation(
    store_root: Path | str,
    symbols: Sequence[str],
    *,
    output: TextIO | None = None,
) -> dict[str, Any]:
    """Authenticate every boundary, account for every label, and exercise every online history."""
    store = Path(store_root)
    manifest = read_json_object(_state_root(store) / RUN_MANIFEST, "clean-room run manifest")
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str):
        raise CleanRoomError("clean-room run has no identity")
    outcomes, owners = _load_existing_outcomes(store, run_id, symbols)
    if set(outcomes) != set(range(len(symbols))):
        missing = next(position for position in range(len(symbols)) if position not in outcomes)
        raise CleanRoomError(f"unaccounted universe label at position {missing}: {symbols[missing]}")
    for position, label in enumerate(symbols):
        resolution_document = read_json_object(
            _state_file(store, "resolutions", position),
            "clean-room resolution",
        )
        resolution = _validate_resolution(
            resolution_document,
            run_id=run_id,
            position=position,
            label=label,
        )
        outcome = outcomes[position]
        if (
            outcome["resolution_sha256"] != resolution_document["resolution_sha256"]
            or outcome["canonical_symbol"] != resolution["authoritative_symbol"]
        ):
            raise CleanRoomError(f"resolution/outcome binding differs: {label}")
        if outcome["status"] == "canonical_reused":
            owner_position = outcome["canonical_owner_position"]
            owner = outcomes.get(owner_position)
            if (
                owner is None
                or owner["status"] not in {"published", "rejected_validation"}
                or owner["canonical_symbol"] != outcome["canonical_symbol"]
                or owner["status"] != outcome["canonical_owner_status"]
            ):
                raise CleanRoomError(f"reused canonical owner differs: {label}")

    published = {
        canonical for canonical, (_position, outcome) in owners.items() if outcome["status"] == "published"
    }
    rejected = {
        canonical
        for canonical, (_position, outcome) in owners.items()
        if outcome["status"] == "rejected_validation"
    }
    all_sources = published | rejected
    if not published:
        raise CleanRoomError("clean-room generation produced no publishable canonical symbols")

    audit_counts = Counter()
    with publication_lock(store, exclusive=False):
        assert_publication_complete(store)
        expected_directories = {
            "end_of_day": all_sources,
            "validation": all_sources,
            "lineage": published,
            "correlation": published,
            "warnings": published,
        }
        for directory_name, expected in expected_directories.items():
            observed = _directory_symbols(store / directory_name)
            if observed != expected:
                missing = sorted(expected - observed)[:5]
                unexpected = sorted(observed - expected)[:5]
                raise CleanRoomError(
                    f"{directory_name} membership differs; missing={missing!r}, unexpected={unexpected!r}"
                )

        for number, canonical in enumerate(sorted(all_sources), start=1):
            source = authenticate_monthly_source(store, canonical)
            validation_receipt, _validation_report = authenticate_validation(
                store,
                canonical,
                require_passed=canonical in published,
            )
            if validation_receipt.get("source_generation_sha256") != source.source_generation_sha256:
                raise CleanRoomError(f"validation/source generation differs: {canonical}")
            if canonical in rejected:
                if validation_receipt.get("passed") is not False:
                    raise CleanRoomError(f"rejected source unexpectedly passed validation: {canonical}")
            else:
                lineage_receipt, _lineage_report = authenticate_lineage(store, canonical)
                correlation = authenticate_correlation(store, canonical)
                warnings = authenticate_warnings(store, canonical)
                if (
                    lineage_receipt.get("source_generation_sha256") != source.source_generation_sha256
                    or lineage_receipt.get("validation_generation_sha256")
                    != validation_receipt.get("validation_generation_sha256")
                    or correlation.receipt.get("source_generation_sha256") != source.source_generation_sha256
                    or correlation.receipt.get("validation_generation_sha256")
                    != validation_receipt.get("validation_generation_sha256")
                    or correlation.receipt.get("lineage_generation_sha256")
                    != lineage_receipt.get("lineage_generation_sha256")
                    or warnings.receipt.get("source_generation_sha256") != source.source_generation_sha256
                    or warnings.receipt.get("validation_generation_sha256")
                    != validation_receipt.get("validation_generation_sha256")
                    or warnings.receipt.get("correlation_generation_sha256")
                    != correlation.receipt.get("correlation_generation_sha256")
                ):
                    raise CleanRoomError(f"boundary generation chain differs: {canonical}")
            audit_counts["canonical_boundaries"] += 1
            audit_counts["source_rows"] += sum(len(view.rows) for view in source.adjustments)
            if number % 25 == 0 or number == len(all_sources):
                _emit(output, f"audit: authenticated {number}/{len(all_sources)} canonical sources")

        master = authenticate_catalogue_store(store)
        catalogue_symbols = set(master.sources_by_symbol)
        if catalogue_symbols != published:
            raise CleanRoomError("authenticated catalogue source membership differs from admitted symbols")
        audit_counts["catalogue_sources"] = len(catalogue_symbols)
        audit_counts["catalogue_identities"] = len(master.identities_by_master_id)

    # Exercise the exact public package against a read-only facade for this store.
    with tempfile.TemporaryDirectory(prefix="alpaca-clean-room-public-") as directory:
        facade = Path(directory)
        os.symlink(store.resolve(), facade / "data_vtest", target_is_directory=True)
        library = DataLibrary(facade)
        securities = library.list_securities()
        if library.catalog_generation() != master.receipt["security_master_generation_sha256"]:
            raise CleanRoomError("public reader selected another catalogue generation")
        public_bar_reads = 0
        for number, security in enumerate(securities, start=1):
            if len(security.adjustments) != len(ADJUSTMENTS) or set(security.adjustments) != set(ADJUSTMENTS):
                raise CleanRoomError(f"online security advertises incomplete adjustments: {security.public_id}")
            for adjustment in ADJUSTMENTS:
                history = library.get_history(security.public_id, adjustment)
                if (
                    history.security.public_id != security.public_id
                    or history.adjustment != adjustment
                    or history.generation != library.catalog_generation()
                    or not history.bars
                ):
                    raise CleanRoomError(f"public history contract failed: {security.public_id}/{adjustment}")
                public_bar_reads += len(history.bars)
            # The public object deliberately caches within a normal consumer snapshot.
            # A universe audit bounds memory by dropping that cache after each identity.
            library._history_inputs = type(library._history_inputs)()  # noqa: SLF001
            if number % 10 == 0 or number == len(securities):
                _emit(output, f"audit: exercised {number}/{len(securities)} online public identities")
        if DataLibrary(facade).catalog_generation() != library.catalog_generation():
            raise CleanRoomError("catalogue generation changed during public audit")
        audit_counts["online_identities"] = len(securities)
        audit_counts["public_history_reads"] = len(securities) * len(ADJUSTMENTS)
        audit_counts["public_bar_observations_read"] = public_bar_reads

    return {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_audit",
        "run_id": run_id,
        "status": "passed",
        "completed_at": _utc_now(),
        "request_end": manifest["request"]["to"],
        "universe_label_count": len(symbols),
        "outcome_counts": dict(sorted(Counter(item["status"] for item in outcomes.values()).items())),
        "unique_canonical_count": len(owners),
        "published_canonical_count": len(published),
        "rejected_canonical_count": len(rejected),
        "catalogue_generation_sha256": master.receipt["security_master_generation_sha256"],
        "counts": dict(sorted(audit_counts.items())),
    }


def run_clean_room_generation(
    universe_path: Path | str,
    store_root: Path | str,
    *,
    request_end_timestamp: str,
    environment: Mapping[str, str] | None = None,
    output: TextIO | None = None,
    resolver: Callable[..., dict[str, Any]] = resolve_authoritative_alpaca_symbol,
    pipeline_runner: Callable[..., dict[str, Any]] = run_v6_pipeline,
    final_auditor: Callable[..., dict[str, Any]] = audit_clean_room_generation,
    maximum_attempts_per_label: int = 4,
) -> dict[str, Any]:
    """Construct, checkpoint, and fully audit one provider-only generation."""
    if maximum_attempts_per_label < 1:
        raise ValueError("maximum attempts per label must be positive")
    universe = Path(universe_path)
    store = Path(store_root)
    _validate_request_end(request_end_timestamp)
    if store.name in {"data", "data_vtest"}:
        raise CleanRoomError("clean-room construction refuses an existing public or legacy store path")
    environment = os.environ if environment is None else environment
    api_key_id = environment.get("ALPACA_API_KEY_ID")
    api_secret_key = environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise CleanRoomError("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")
    manifest, symbols = _initialize_run(store, universe, request_end_timestamp)
    run_id = manifest["run_id"]
    completed = _load_completed_run(store, run_id)
    if completed is not None:
        _emit(output, "clean-room generation already completed and audited; retaining it unchanged")
        return completed

    outcomes, canonical_owners = _load_existing_outcomes(store, run_id, symbols)
    bar_rate_limiter = AlpacaRequestRateLimiter()
    identity_rate_limiter = AlpacaIdentityRequestRateLimiter()
    _emit(
        output,
        f"clean-room run {run_id}: {len(symbols)} labels, "
        f"{len(outcomes)} already complete, request end {request_end_timestamp}",
    )
    prefetch_enabled = pipeline_runner is run_v6_pipeline
    source_executor = ThreadPoolExecutor(max_workers=8) if prefetch_enabled else None
    resolution_executor = ThreadPoolExecutor(max_workers=16) if prefetch_enabled else None
    resolved: dict[int, tuple[dict[str, Any], str]] = {}
    source_futures: dict[
        int,
        tuple[
            Future[dict[str, Mapping[str, Sequence[object]]]],
            str,
        ],
    ] = {}
    resolution_futures: dict[int, Future[tuple[dict[str, Any], str]]] = {}
    claimed_canonicals = set(canonical_owners)
    pending_source_batch: list[tuple[int, str]] = []
    next_resolution_submit = 0
    next_prepare = 0

    def submit_source_batch() -> None:
        nonlocal pending_source_batch
        if not pending_source_batch:
            return
        if source_executor is None:
            raise AssertionError("source prefetch batch has no executor")
        future = source_executor.submit(
            _request_source_view_batch,
            [item[1] for item in pending_source_batch],
            api_key_id,
            api_secret_key,
            request_end_timestamp,
            bar_rate_limiter,
        )
        for batch_position, batch_symbol in pending_source_batch:
            source_futures[batch_position] = (future, batch_symbol)
        pending_source_batch = []

    def submit_resolutions(exclusive_end: int) -> None:
        nonlocal next_resolution_submit
        while next_resolution_submit < exclusive_end:
            position = next_resolution_submit
            next_resolution_submit += 1
            if position in outcomes or resolution_executor is None:
                continue
            resolution_futures[position] = resolution_executor.submit(
                _resolve_with_retries,
                store,
                run_id=run_id,
                position=position,
                label=symbols[position],
                api_key_id=api_key_id,
                api_secret_key=api_secret_key,
                identity_rate_limiter=identity_rate_limiter,
                resolver=resolver,
                maximum_attempts=maximum_attempts_per_label,
                output=output,
            )

    def prepare_sources(exclusive_end: int) -> None:
        nonlocal next_prepare
        submit_resolutions(
            min(len(symbols), exclusive_end + (RESOLUTION_PREFETCH_LEAD if prefetch_enabled else 0))
        )
        while next_prepare < exclusive_end:
            position = next_prepare
            next_prepare += 1
            if position in outcomes:
                continue
            label = symbols[position]
            resolution_future = resolution_futures.pop(position, None)
            resolution = (
                resolution_future.result()
                if resolution_future is not None
                else _resolve_with_retries(
                    store,
                    run_id=run_id,
                    position=position,
                    label=label,
                    api_key_id=api_key_id,
                    api_secret_key=api_secret_key,
                    identity_rate_limiter=identity_rate_limiter,
                    resolver=resolver,
                    maximum_attempts=maximum_attempts_per_label,
                    output=output,
                )
            )
            resolved[position] = resolution
            canonical = resolution[0]["authoritative_symbol"]
            if canonical in claimed_canonicals:
                continue
            claimed_canonicals.add(canonical)
            source_manifest = store / "end_of_day" / canonical / SOURCE_MANIFEST
            if source_executor is not None and not source_manifest.is_file():
                pending_source_batch.append((position, canonical))
                if len(pending_source_batch) == SOURCE_REQUEST_BATCH_SIZE:
                    submit_source_batch()
        if exclusive_end == len(symbols):
            submit_source_batch()

    try:
        for position, label in enumerate(symbols):
            if position in outcomes:
                continue
            prepare_sources(
                min(
                    len(symbols),
                    position + (SOURCE_PREFETCH_LEAD if prefetch_enabled else 1),
                )
            )
            if any(batch_position == position for batch_position, _symbol in pending_source_batch):
                submit_source_batch()
            resolution, resolution_sha256 = resolved.pop(position)
            canonical = resolution["authoritative_symbol"]
            prior = canonical_owners.get(canonical)
            if prior is not None:
                owner_position, owner = prior
                outcome = _write_outcome(
                    store,
                    {
                        "schema_version": CLEAN_ROOM_RUN_VERSION,
                        "boundary": "alpaca_end_of_day_clean_room_outcome",
                        "run_id": run_id,
                        "position": position,
                        "submitted_label": label,
                        "canonical_symbol": canonical,
                        "resolution_sha256": resolution_sha256,
                        "status": "canonical_reused",
                        "canonical_owner_position": owner_position,
                        "canonical_owner_status": owner["status"],
                        "canonical_owner_outcome_sha256": owner["outcome_sha256"],
                    },
                )
                outcomes[position] = outcome
            else:
                prior_attempts = _attempt_count(store, position)
                for local_attempt in range(1, maximum_attempts_per_label + 1):
                    attempt = prior_attempts + local_attempt
                    try:
                        source_views = None
                        if local_attempt == 1 and position in source_futures:
                            source_future, future_symbol = source_futures[position]
                            source_views = {canonical: source_future.result()[future_symbol]}
                        runner_arguments = {
                            "store_root": store,
                            "environment": environment,
                            "request_start_timestamp": EARLIEST_REQUEST_TIMESTAMP,
                            "request_end_timestamp": request_end_timestamp,
                            "bar_rate_limiter": bar_rate_limiter,
                            "identity_rate_limiter": identity_rate_limiter,
                            "symbol_resolutions": [resolution],
                            "accept_failed_validation": True,
                            "reuse_acquired_boundaries": True,
                            "output": None,
                        }
                        if source_views is not None:
                            runner_arguments["source_views_by_symbol"] = source_views
                        result = pipeline_runner([label], **runner_arguments)
                        outcome = _write_outcome(
                            store,
                            _pipeline_outcome(
                                run_id=run_id,
                                position=position,
                                label=label,
                                canonical_symbol=canonical,
                                resolution_sha256=resolution_sha256,
                                result=result,
                            ),
                        )
                        outcomes[position] = outcome
                        canonical_owners[canonical] = (position, outcome)
                        break
                    except Exception as error:
                        _record_attempt_failure(
                            store,
                            run_id=run_id,
                            position=position,
                            label=label,
                            attempt=attempt,
                            stage="pipeline",
                            error=error,
                        )
                        if local_attempt == maximum_attempts_per_label:
                            raise CleanRoomError(
                                f"{label}: failed after {maximum_attempts_per_label} attempts: "
                                f"{type(error).__name__}: {error}"
                            ) from error
                        delay = min(2 ** (local_attempt - 1), 30)
                        _emit(
                            output,
                            f"{label}: attempt {attempt} failed "
                            f"({type(error).__name__}: {error}); retrying in {delay}s",
                        )
                        time.sleep(delay)
            source_futures.pop(position, None)
            if (position + 1) % 25 == 0 or position + 1 == len(symbols):
                counts = Counter(item["status"] for item in outcomes.values())
                _emit(
                    output,
                    f"acquisition: {position + 1}/{len(symbols)} labels accounted; "
                    f"published={counts['published']}, "
                    f"rejected={counts['rejected_validation']}, "
                    f"canonical-reused={counts['canonical_reused']}",
                )
    finally:
        if source_executor is not None:
            source_executor.shutdown(wait=True, cancel_futures=True)
        if resolution_executor is not None:
            resolution_executor.shutdown(wait=True, cancel_futures=True)

    audit = final_auditor(store, symbols, output=output)
    if audit.get("status") != "passed" or audit.get("run_id") != run_id:
        raise CleanRoomError("final clean-room auditor did not return a passing bound report")
    audit_path = _state_root(store) / AUDIT_REPORT
    replace_json(audit_path, audit)
    completion = {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_completion",
        "run_id": run_id,
        "status": "passed",
        "completed_at": _utc_now(),
        "audit_sha256": sha256_file(audit_path),
        "catalogue_generation_sha256": audit.get("catalogue_generation_sha256"),
        "universe_label_count": len(symbols),
    }
    replace_json(_state_root(store) / COMPLETION_RECEIPT, completion)
    _emit(output, f"clean-room generation passed complete audit: {run_id}")
    return completion


def _move_generation_into_place(
    *,
    repository_root: Path,
    staging_root: Path,
    live_root: Path,
    backup_root: Path | None,
) -> None:
    """Rename a fully closed staging directory into place with rollback."""
    if live_root.exists():
        if backup_root is None:
            raise CleanRoomError("activation backup path is required for an existing public store")
        os.replace(live_root, backup_root)
    try:
        os.replace(staging_root, live_root)
    except BaseException:
        if backup_root is not None and backup_root.exists() and not live_root.exists():
            os.replace(backup_root, live_root)
        raise


def _rollback_activation(
    *,
    repository_root: Path,
    staging_root: Path,
    live_root: Path,
    backup_root: Path | None,
) -> None:
    if staging_root.exists():
        raise CleanRoomError(f"cannot roll back activation over an existing path: {staging_root}")
    os.replace(live_root, staging_root)
    if backup_root is not None:
        os.replace(backup_root, live_root)


def activate_clean_room_generation(
    repository_root: Path | str,
    store_root: Path | str,
    *,
    output: TextIO | None = None,
) -> dict[str, Any]:
    """Atomically select one completed clean-room store while retaining the prior store."""
    repository = Path(repository_root).resolve()
    supplied_store = Path(store_root)
    if not supplied_store.is_absolute():
        supplied_store = repository / supplied_store
    if supplied_store.is_symlink() or not supplied_store.is_dir():
        raise CleanRoomError(f"activation source is not a regular directory: {supplied_store}")
    staging = supplied_store.resolve()
    live = repository / "data_vtest"
    if staging == live:
        completion = read_json_object(
            live / RUN_STATE_DIRECTORY / COMPLETION_RECEIPT,
            "clean-room completion receipt",
        )
        return {
            "schema_version": CLEAN_ROOM_RUN_VERSION,
            "boundary": "alpaca_end_of_day_clean_room_activation",
            "status": "already_active",
            "run_id": completion.get("run_id"),
            "catalogue_generation_sha256": completion.get("catalogue_generation_sha256"),
            "active_store": live.name,
            "previous_store": None,
        }
    if staging.parent != repository:
        raise CleanRoomError("clean-room activation requires a staging store directly under the repository root")
    if live.is_symlink() or (live.exists() and not live.is_dir()):
        raise CleanRoomError(f"public store is not a regular directory: {live}")

    manifest = read_json_object(staging / RUN_STATE_DIRECTORY / RUN_MANIFEST, "clean-room run manifest")
    completion = _load_completed_run(staging, manifest.get("run_id"))
    if completion is None:
        raise CleanRoomError("only a completely audited clean-room generation can be activated")
    audit = read_json_object(staging / RUN_STATE_DIRECTORY / AUDIT_REPORT, "clean-room audit report")
    if (
        audit.get("status") != "passed"
        or audit.get("run_id") != completion.get("run_id")
        or audit.get("catalogue_generation_sha256") != completion.get("catalogue_generation_sha256")
    ):
        raise CleanRoomError("clean-room audit and completion identities differ")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = (
        repository / f"data_previous_public_{timestamp}_{completion['run_id'][:12]}" if live.exists() else None
    )
    if backup is not None and backup.exists():
        raise CleanRoomError(f"activation backup path already exists: {backup}")

    # Existing readers finish under the old shared lock. The new generation is
    # immutable and separately locked before either directory name changes.
    if live.exists():
        with publication_lock(live, exclusive=True):
            with publication_lock(staging, exclusive=True):
                _move_generation_into_place(
                    repository_root=repository,
                    staging_root=staging,
                    live_root=live,
                    backup_root=backup,
                )
    else:
        with publication_lock(staging, exclusive=True):
            _move_generation_into_place(
                repository_root=repository,
                staging_root=staging,
                live_root=live,
                backup_root=None,
            )

    try:
        library = DataLibrary(repository)
        public_generation = library.catalog_generation()
        online_count = library.online_count()
        if public_generation != completion["catalogue_generation_sha256"]:
            raise CleanRoomError("activated public reader selected another generation")
        expected_online = audit.get("counts", {}).get("online_identities")
        if expected_online != online_count:
            raise CleanRoomError("activated public reader returned another online identity count")
    except BaseException:
        if backup is not None:
            with publication_lock(live, exclusive=True):
                with publication_lock(backup, exclusive=True):
                    _rollback_activation(
                        repository_root=repository,
                        staging_root=staging,
                        live_root=live,
                        backup_root=backup,
                    )
        else:
            with publication_lock(live, exclusive=True):
                _rollback_activation(
                    repository_root=repository,
                    staging_root=staging,
                    live_root=live,
                    backup_root=None,
                )
        raise

    activation = {
        "schema_version": CLEAN_ROOM_RUN_VERSION,
        "boundary": "alpaca_end_of_day_clean_room_activation",
        "status": "active",
        "activated_at": _utc_now(),
        "run_id": completion["run_id"],
        "catalogue_generation_sha256": public_generation,
        "online_identity_count": online_count,
        "active_store": live.name,
        "previous_store": backup.name if backup is not None else None,
    }
    replace_json(
        repository / "alpaca_run_logs" / f"clean_room_activation_{completion['run_id']}.json",
        activation,
    )
    _emit(
        output,
        f"activated clean-room generation {public_generation}; "
        f"previous store retained at {backup.name if backup is not None else 'none'}",
    )
    return activation


__all__ = [
    "CLEAN_ROOM_RUN_VERSION",
    "CleanRoomError",
    "activate_clean_room_generation",
    "audit_clean_room_generation",
    "load_universe",
    "run_clean_room_generation",
]
