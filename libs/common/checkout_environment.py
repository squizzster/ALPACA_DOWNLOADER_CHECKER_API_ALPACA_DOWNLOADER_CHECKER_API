"""Interpreter selection for executable checkout adapters."""

import os
import sys
from pathlib import Path


def reexec_in_checkout_venv(script: Path) -> None:
    """Prefer this checkout's Python 3.11 venv unless one is already active."""
    if sys.prefix != sys.base_prefix:
        return
    checkout_python = script.parent / ".venv" / "bin" / "python3.11"
    if not checkout_python.is_file() or not os.access(checkout_python, os.X_OK):
        return
    if Path(os.path.abspath(sys.executable)) == checkout_python:
        return
    os.execv(str(checkout_python), [str(checkout_python), *sys.orig_argv[1:]])
