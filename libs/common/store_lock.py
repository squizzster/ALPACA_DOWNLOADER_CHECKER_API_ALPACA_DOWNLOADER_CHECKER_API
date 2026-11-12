"""One operation-lifetime read/write gate for a completed pricing store."""

import fcntl
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from threading import get_ident


@dataclass
class _Ownership:
    store: Path
    exclusive: bool
    process: int
    thread: int
    active: bool = True


_ownership = ContextVar("store_gate_ownership", default=())


def exclusive_owned(store):
    """Whether this execution actually holds the exclusive kernel gate.

    This is execution infrastructure only. Revocation and process/thread binding
    prevent a copied context from lending ownership after release or after fork.
    Ordinary operation_lock calls remain non-reentrant.
    """
    store = Path(store).resolve()
    return any(
        owner.active
        and owner.exclusive
        and owner.store == store
        and owner.process == os.getpid()
        and owner.thread == get_ident()
        for owner in _ownership.get()
    )


class StoreBusyError(RuntimeError):
    """Raised when an export or store writer already owns the store gate."""


@contextmanager
def operation_lock(store, *, exclusive, wait=False, on_wait=None):
    """Take the kernel gate, optionally waiting after one nonblocking attempt."""
    # The kernel lock is authority, not the persistent pathname. DO NOT remove
    # a supposedly stale lock file: replacing its inode lets two owners proceed.
    # This outer gate orders writers/exports; selected owner locks still protect
    # their publications and must not be replaced by per-ticker existence probes.
    store = Path(store).resolve()
    lock = store.parent / f".{store.name}.operation.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock, os.O_RDONLY | os.O_CREAT, 0o666)
    try:
        try:
            fcntl.flock(
                descriptor,
                (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB,
            )
        except BlockingIOError as error:
            if not wait:
                raise StoreBusyError("store is busy with an export or update; retry later") from error
            if on_wait is not None:
                on_wait()
            # Keep this descriptor/inode and sleep in the kernel. Repeated
            # probes or pathname checks establish no new scheduling fact.
            # KeyboardInterrupt still crosses the outer descriptor cleanup.
            fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        owner = _Ownership(store, exclusive, os.getpid(), get_ident())
        token = _ownership.set((*_ownership.get(), owner))
        try:
            yield
        finally:
            owner.active = False
            _ownership.reset(token)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass  # Closing the descriptor still releases the kernel lock.
    finally:
        os.close(descriptor)
