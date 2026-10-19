"""Create an interrupted SQLite transaction in a disposable test database."""

import subprocess
import sys


def leave_hot_journal(path, statement):
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            """
import os
import sys
from libs.common.sqlite_connections import connect_sqlite

db = connect_sqlite(sys.argv[1])
db.execute("PRAGMA cache_size=1")
db.execute("BEGIN IMMEDIATE")
db.execute(sys.argv[2])
db.execute("CREATE TABLE interrupted_write (payload BLOB)")
db.executemany("INSERT INTO interrupted_write VALUES (?)", [(b'x' * 4096,)] * 64)
os._exit(91)
""",
            str(path),
            statement,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode != 91 or not path.with_name(path.name + "-journal").is_file():
        raise AssertionError(f"failed to leave an interrupted transaction: {result.stderr}")
