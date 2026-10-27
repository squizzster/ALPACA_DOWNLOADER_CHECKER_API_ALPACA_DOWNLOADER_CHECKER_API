"""Commit the tracked recovery registry after a completed CLI update."""

import subprocess
from pathlib import Path

from libs.alpaca_recovery_registry import registry_lock

REGISTRY_PATH = "resources/ticker.csv"


def _git(repository, *arguments, check=True):
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def commit_registry(repository, registry, reporter):
    """Keep Git outcomes on stderr and leave the data result authoritative.

    The existing registry lock pins the file while Git reads it. A path-only
    commit leaves unrelated staged and unstaged work alone. No push occurs.
    """
    repository, registry = Path(repository).resolve(), Path(registry).absolute()
    if registry != repository / REGISTRY_PATH:
        return

    def report(status, message, *, commit=None, level="info"):
        context = {"status": status, "path": REGISTRY_PATH, "estimated_remaining_seconds": "TBD"}
        if commit is not None:
            context["commit"] = commit
        reporter.update("COMMIT", message=message, scope=REGISTRY_PATH, context=context)
        reporter.diagnostic(message, level=level)

    report("checking", "Checking the recovery registry for an automatic Git commit.")
    try:
        with registry_lock(registry):
            top = _git(repository, "rev-parse", "--show-toplevel").stdout.strip()
            if Path(top).resolve() != repository:
                raise ValueError("recovery registry is not in the expected Git checkout")
            _git(repository, "ls-files", "--error-unmatch", "--", REGISTRY_PATH)
            difference = _git(repository, "diff", "--quiet", "HEAD", "--", REGISTRY_PATH, check=False)
            if difference.returncode == 0:
                report("unchanged", "Recovery registry unchanged; no Git commit needed.")
                return
            if difference.returncode != 1:
                raise subprocess.CalledProcessError(
                    difference.returncode, difference.args, output=difference.stdout
                )
            report("committing", "Committing changes to resources/ticker.csv.")
            _git(repository, "commit", "--only", "-m", "Update recovery ticker registry", "--", REGISTRY_PATH)
            commit = _git(repository, "rev-parse", "HEAD").stdout.strip()
        report("committed", f"Recovery registry committed as {commit}.", commit=commit)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        details = (
            (error.output or str(error)).strip()
            if isinstance(error, subprocess.CalledProcessError)
            else str(error)
        )
        report(
            "failed", f"Data update succeeded; recovery registry Git commit failed: {details}", level="warning"
        )
