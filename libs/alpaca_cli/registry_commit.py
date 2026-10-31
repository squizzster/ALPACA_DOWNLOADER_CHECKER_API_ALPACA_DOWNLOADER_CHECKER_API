"""Commit and push only the tracked recovery registry after a completed update."""

import subprocess
from pathlib import Path

from libs.alpaca_recovery_registry import registry_lock
from libs.common.debug_trace import traced

REGISTRY_PATH = "resources/ticker.csv"


def _git(repository, *arguments, check=True):
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _failure(error):
    if isinstance(error, subprocess.CalledProcessError):
        return (error.output or f"Git exited with status {error.returncode}").strip()
    return str(error)


@traced("registry_push")
def _push_registry(repository, commit, report):
    # Git publication is a separate, narrowly scoped external side effect. Only
    # the verified registry-only commit may advance its configured upstream;
    # DO NOT broaden this to push unrelated work, force, merge or change branches.
    """An exact source commit and one explicit refspec cannot publish later work."""
    remote, push_url = "upstream", None
    report("checking", "Checking the registry commit against its upstream branch.", phase="PUSH", commit=commit)
    try:
        branch_result = _git(repository, "symbolic-ref", "--quiet", "--short", "HEAD", check=False)
        if branch_result.returncode != 0:
            raise ValueError("automatic registry push requires a current branch with a configured upstream")
        branch = branch_result.stdout.strip()
        remote_result = _git(repository, "config", "--get", f"branch.{branch}.remote", check=False)
        target_result = _git(repository, "config", "--get-all", f"branch.{branch}.merge", check=False)
        remote, targets = remote_result.stdout.strip(), target_result.stdout.splitlines()
        if (
            remote_result.returncode != 0
            or target_result.returncode != 0
            or remote == "."
            or len(targets) != 1
            or not targets[0].startswith("refs/heads/")
        ):
            raise ValueError("automatic registry push requires one configured remote branch")
        target = targets[0]
        urls = _git(repository, "remote", "get-url", "--push", "--all", remote).stdout.splitlines()
        if len(urls) != 1:
            raise ValueError("automatic registry push requires exactly one push destination")
        push_url = urls[0]
        advertised = _git(repository, "ls-remote", "--refs", "--", push_url, target).stdout.splitlines()
        if len(advertised) != 1 or advertised[0].split()[1] != target:
            raise ValueError("the upstream branch is missing; synchronize it explicitly")
        remote_commit = advertised[0].split()[0]
        if remote_commit == commit:
            report("current", "Recovery registry is already current on the remote.", phase="PUSH", commit=commit)
            return
        parents = _git(repository, "rev-list", "--parents", "-n", "1", commit).stdout.split()
        paths = _git(repository, "diff-tree", "--no-commit-id", "--name-only", "-r", commit).stdout.splitlines()
        if len(parents) != 2 or parents[1] != remote_commit or paths != [REGISTRY_PATH]:
            raise ValueError(
                "push deferred: only one resources/ticker.csv commit directly above the remote is allowed; "
                "synchronize other commits or divergent history explicitly"
            )
        report(
            "pushing",
            f"Pushing the registry commit to {remote}/{target.removeprefix('refs/heads/')}.",
            phase="PUSH",
            commit=commit,
        )
        _git(
            repository,
            "push",
            "--porcelain",
            "--no-follow-tags",
            "--recurse-submodules=no",
            "--",
            remote,
            f"{commit}:{target}",
        )
        report("pushed", f"Recovery registry commit {commit} pushed successfully.", phase="PUSH", commit=commit)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        details = _failure(error)
        if push_url:
            details = details.replace(push_url, remote)
        report(
            "failed",
            f"Data update succeeded; recovery registry push failed: {details}",
            phase="PUSH",
            commit=commit,
            level="warning",
        )


@traced("registry_commit")
def commit_registry(repository, registry, reporter):
    """Keep Git outcomes on stderr and leave the data result authoritative.

    The existing registry lock pins the file while Git reads it. A path-only
    commit leaves unrelated staged and unstaged work alone. Push selects only
    that commit when its sole parent is the advertised upstream tip.
    """
    repository, registry = Path(repository).resolve(), Path(registry).absolute()
    if registry != repository / REGISTRY_PATH:
        return

    def report(status, message, *, phase="COMMIT", commit=None, level="info"):
        context = {"status": status, "path": REGISTRY_PATH, "estimated_remaining_seconds": "TBD"}
        if commit is not None:
            context["commit"] = commit
        reporter.update(phase, message=message, scope=REGISTRY_PATH, context=context)
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
            elif difference.returncode != 1:
                raise subprocess.CalledProcessError(
                    difference.returncode, difference.args, output=difference.stdout
                )
            else:
                report("committing", "Committing changes to resources/ticker.csv.")
                _git(
                    repository, "commit", "--only", "-m", "Update recovery ticker registry", "--", REGISTRY_PATH
                )
            commit = _git(repository, "rev-parse", "HEAD").stdout.strip()
        if difference.returncode == 1:
            report("committed", f"Recovery registry committed as {commit}.", commit=commit)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report(
            "failed",
            f"Data update succeeded; recovery registry Git commit failed: {_failure(error)}",
            level="warning",
        )
        return
    _push_registry(repository, commit, report)
