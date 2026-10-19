"""SQLite connections for process-recoverable publications without forced writeback."""

import sqlite3
from pathlib import Path


def connect_sqlite(path, *, readonly=False, create=False, timeout=60):
    mode = "ro" if readonly else "rwc" if create else "rw"
    connection = sqlite3.connect(Path(path).absolute().as_uri() + "?mode=" + mode, uri=True, timeout=timeout)
    if not readonly:
        try:
            # Keep disk journals and transactions. SQLite owns interrupted-transaction
            # recovery, including any engine-internal synchronization during opening.
            connection.execute("PRAGMA synchronous=OFF")
        except BaseException:
            connection.close()
            raise
    return connection
