from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_security_correlation import (
    OFFLINE_AVAILABILITY_STATUS,
    SECURITY_AVAILABILITY_STATUSES,
    SecurityCorrelationError,
    authenticate_security_master,
    is_master_id,
)

from .errors import EndOfDayReaderError
from .reader_version import validate_reader_change_log


def _canonical_master_id(master_id: str) -> str:
    if not isinstance(master_id, str):
        raise ValueError("master ID must be a string")
    canonical_id = master_id.lower()
    if not is_master_id(canonical_id):
        raise ValueError(f"invalid master ID: {master_id!r}")
    return canonical_id


def _authenticated_master(repository_root: Path | str | None):
    validate_reader_change_log()
    root = Path.cwd() if repository_root is None else Path(repository_root)
    try:
        return authenticate_security_master(root)
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(f"cannot authenticate security master: {error}") from error


def get_alpaca_end_of_day_security_availability(
    master_id: str,
    *,
    repository_root: Path | str | None = None,
) -> dict[str, Any]:
    """Return the persisted provider-neutral online state for one master ID."""
    canonical_id = _canonical_master_id(master_id)
    master = _authenticated_master(repository_root)
    try:
        identity = master.identities_by_master_id.get(canonical_id)
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(
            f"cannot authenticate security-master identity {canonical_id}: {error}"
        ) from error
    return {
        "master_id": canonical_id,
        "online": identity["online"] if identity is not None else False,
        "availability_status": (
            identity["availability_status"] if identity is not None else OFFLINE_AVAILABILITY_STATUS
        ),
    }


def is_alpaca_end_of_day_security_online(
    master_id: str,
    *,
    repository_root: Path | str | None = None,
) -> bool:
    """Return the persisted online boolean for one master ID."""
    return get_alpaca_end_of_day_security_availability(
        master_id,
        repository_root=repository_root,
    )["online"]


def list_online_alpaca_end_of_day_master_ids(
    *,
    repository_root: Path | str | None = None,
) -> list[str]:
    """Return every master ID whose authenticated identity shard stores online=true."""
    master = _authenticated_master(repository_root)
    try:
        return sorted(
            master_id
            for master_id in master.identities_by_master_id
            if master.identities_by_master_id[master_id]["online"]
        )
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(f"cannot authenticate security-master online inventory: {error}") from error


def list_alpaca_end_of_day_master_ids_by_availability_status(
    availability_status: str,
    *,
    repository_root: Path | str | None = None,
) -> list[str]:
    """Return published master IDs with one exact persisted availability classification."""
    if availability_status not in SECURITY_AVAILABILITY_STATUSES:
        raise ValueError(f"invalid availability status: {availability_status!r}")
    master = _authenticated_master(repository_root)
    try:
        return sorted(
            master_id
            for master_id in master.identities_by_master_id
            if master.identities_by_master_id[master_id]["availability_status"] == availability_status
        )
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(
            f"cannot authenticate security-master availability inventory: {error}"
        ) from error
