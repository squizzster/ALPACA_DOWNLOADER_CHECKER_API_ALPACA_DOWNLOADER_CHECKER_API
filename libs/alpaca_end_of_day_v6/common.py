from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import V6Error

PUBLICATION_LOCK = ".publication.lock"
PUBLICATION_PENDING = ".publication.pending.json"


def canonical_json_bytes(document: object) -> bytes:
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def canonical_sha256(document: object) -> str:
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def document_bytes(document: object) -> bytes:
    return json.dumps(document, ensure_ascii=True, separators=(",", ":")).encode("utf-8") + b"\n"


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_child(root: Path, name: str, description: str) -> Path:
    if not isinstance(name, str) or not name or name in {".", ".."}:
        raise V6Error(f"unsafe {description}: {name!r}")
    if Path(name).name != name or "/" in name or "\\" in name:
        raise V6Error(f"unsafe {description}: {name!r}")
    return root / name


def read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise V6Error(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise V6Error(f"{description} is not a JSON object: {path}")
    return document


def replace_bytes(path: Path, content: bytes) -> bool:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise V6Error(f"publication target is not a regular file: {path}")
    if path.exists() and path.stat().st_size == len(content) and sha256_file(path) == sha256_bytes(content):
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.tmp.")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
        os.replace(temporary, path)
        return True
    finally:
        temporary.unlink(missing_ok=True)


def replace_json(path: Path, document: object) -> bool:
    return replace_bytes(path, document_bytes(document))


def ensure_publication_lock(store_root: Path | str) -> Path:
    """Create the stable lock inode once without ever replacing it."""
    root = Path(store_root)
    path = root / PUBLICATION_LOCK
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise V6Error(f"v6 publication lock is not a regular file: {path}")
    if not path.exists():
        try:
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
    if path.is_symlink() or not path.is_file():
        raise V6Error(f"v6 publication lock is unavailable: {path}")
    return path


@contextmanager
def publication_lock(store_root: Path | str, *, exclusive: bool) -> Iterator[None]:
    """Hold the stable v6 publication lock for one complete read or write."""
    path = ensure_publication_lock(store_root) if exclusive else Path(store_root) / PUBLICATION_LOCK
    if path.is_symlink() or not path.is_file():
        raise V6Error(f"v6 publication lock is unavailable: {path}")
    descriptor = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def assert_publication_complete(store_root: Path | str) -> None:
    marker = Path(store_root) / PUBLICATION_PENDING
    if marker.is_symlink() or (marker.exists() and not marker.is_file()):
        raise V6Error(f"v6 publication recovery marker is invalid: {marker}")
    if marker.exists():
        raise V6Error("v6 publication is incomplete; rerun the v6 pipeline to recover it")


def begin_publication(store_root: Path | str, document: object) -> None:
    """Publish the transient recovery marker while holding the exclusive lock."""
    replace_json(Path(store_root) / PUBLICATION_PENDING, document)


def finish_publication(store_root: Path | str) -> None:
    """Remove the recovery marker only after the final catalogue receipt exists."""
    root = Path(store_root)
    marker = root / PUBLICATION_PENDING
    marker.unlink(missing_ok=True)


@contextmanager
def publication_transaction(store_root: Path | str, marker: object, *, before_publish=None) -> Iterator[None]:
    """Guard a multi-file v6 publication and retain its marker on failure."""
    with publication_lock(store_root, exclusive=True):
        if before_publish is not None:
            before_publish()
        begin_publication(store_root, marker)
        yield
        finish_publication(store_root)
