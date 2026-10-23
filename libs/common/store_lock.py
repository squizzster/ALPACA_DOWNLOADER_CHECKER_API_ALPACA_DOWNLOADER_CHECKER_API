"""One operation-lifetime read/write gate for a completed pricing store."""

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path


class StoreBusyError(RuntimeError):
    """Raised when an export or store writer already owns the store gate."""


@contextmanager
def operation_lock(store, *, exclusive):
    """Take the store-wide gate without waiting or interpreting path existence as state."""
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
            raise StoreBusyError("store is busy with an export or update; retry later") from error
        try:
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass  # Closing the descriptor still releases the kernel lock.
    finally:
        os.close(descriptor)
