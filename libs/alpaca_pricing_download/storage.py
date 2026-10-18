"""Small, fixed-name boundaries with a roll-forward publication journal.

Only changed files are staged. Cooperating readers hold the ticker lock and
reject an interrupted transaction. Old partitions are removed only after the
replacement files and final source receipt have been installed and verified.
"""

from __future__ import annotations

import calendar
import fcntl
import json
import os
import re
from contextlib import contextmanager
from datetime import date
from pathlib import Path, PurePosixPath

from .contract import (
    ADJUSTMENTS,
    COLUMNS,
    HISTORY_START,
    SUPPORTED_VERSIONS,
    VALIDATOR_FILE,
    VERSION,
    DownloadError,
    csv_bytes,
    decode_csv,
    digest,
    json_bytes,
    timestamp,
)
from .endpoints import merge_ranges, validator_endpoints

STORE_DOCUMENT = {
    "schema_version": 3,
    "boundary": "alpaca_standalone_pricing_store",
    "columns": list(COLUMNS),
    "adjustments": list(ADJUSTMENTS),
    "overlap": "current_month_replaces_same_month_in_current_year",
    "pricing": "end_of_day/pricing",
    "updates": "end_of_day/manifests",
}


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def regular_path(root: Path, relative: str) -> Path:
    parts = PurePosixPath(relative).parts
    if (
        not parts
        or len(parts) > 3
        or PurePosixPath(relative).is_absolute()
        or any(p in (".", "..") for p in parts)
    ):
        raise DownloadError(f"unsafe pricing path: {relative!r}")
    path = root
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise DownloadError(f"pricing paths cannot be symlinks: {path}")
    return path


def write_file(path: Path, data: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def read_document(path: Path) -> dict:
    if path.is_symlink():
        raise DownloadError(f"manifest cannot be a symlink: {path}")
    try:
        document = json.loads(path.read_bytes())
    except (ValueError, OSError) as error:
        raise DownloadError(f"cannot read manifest: {path}") from error
    if not isinstance(document, dict):
        raise DownloadError(f"manifest is not an object: {path}")
    return document


def initialize_store(root: Path) -> None:
    for path in (root, *root.parents):
        if path.is_symlink():
            raise DownloadError(f"store cannot traverse a symlink: {path}")
    root.mkdir(parents=True, exist_ok=True)
    # Lock the stable directory inode: no additional persistent lock file.
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        manifest = regular_path(root, "store.manifest.json")
        staged = regular_path(root, ".store.manifest.pending")
        if manifest.exists():
            if read_document(manifest) != STORE_DOCUMENT:
                raise DownloadError("destination belongs to another format; existing stores are not migrated")
        else:
            if any(path != staged for path in root.iterdir()):
                raise DownloadError("destination is nonempty without a pricing-download store manifest")
            if staged.exists():
                staged.unlink()  # Reserved, incomplete initialization; no published data exists.
            write_file(staged, json_bytes(STORE_DOCUMENT))
            os.replace(staged, manifest)
            fsync_directory(root)
    finally:
        os.close(descriptor)


@contextmanager
def ticker_lock(root: Path, *, exclusive: bool):
    # Downloader and update consumer both take EX. Never unlink the stable inode.
    lock = regular_path(root, ".download.lock")
    with lock.open("a+b" if exclusive else "rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def update_path(root: Path) -> Path:
    if root.parent.name != "pricing" or root.parent.parent.name != "end_of_day":
        raise DownloadError("update manifests require the split pricing/manifests layout")
    return regular_path(root.parent.parent, f"manifests/{root.name}/update.json")


def validate_update(value: dict) -> dict:
    if not isinstance(value, dict) or set(value) != {"from", "to"}:
        raise DownloadError("update manifest must contain only from/to dates")
    try:
        first, last = (date.fromisoformat(value[key]) for key in ("from", "to"))
    except (TypeError, ValueError) as error:
        raise DownloadError("invalid update-manifest dates") from error
    if first > last or value != {"from": first.isoformat(), "to": last.isoformat()}:
        raise DownloadError("invalid update-manifest range")
    return value


def read_update(root: Path) -> dict | None:
    """Caller must hold the ticker lock; presence means unprocessed work."""
    path = update_path(root)
    if not path.parent.exists():
        return None
    if any(entry.name != "update.json" for entry in path.parent.iterdir()):
        raise DownloadError("unexpected files in ticker update directory")
    return validate_update(read_document(path)) if path.exists() else None


def acknowledge_update(root: Path, expected: dict) -> None:
    """Remove only a successfully consumed range, under the same exclusive lock."""
    if read_update(root) != expected:
        raise DownloadError("pending update changed during consumption")
    path = update_path(root)
    path.unlink()
    fsync_directory(path.parent)
    path.parent.rmdir()
    fsync_directory(path.parent.parent)


def boundary_paths(anchor: str) -> dict[str, str]:
    if not isinstance(anchor, str) or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", anchor):
        raise DownloadError("invalid source anchor month")
    year, month = anchor.split("-")
    return {
        "completed_years": "completed_years.manifest.json",
        "current_year": f"{year}/year.manifest.json",
        "current_month": f"{year}/{calendar.month_abbr[int(month)]}/month.manifest.json",
    }


def load_source(root: Path, symbol: str) -> dict | None:
    path = regular_path(root, "source.manifest.json")
    if not path.exists():
        return None
    source = read_document(path)
    if (
        type(source.get("schema_version")) is not int
        or source.get("schema_version") not in SUPPORTED_VERSIONS
        or source.get("boundary") != "alpaca_standalone_pricing_source"
        or source.get("symbol") != symbol
    ):
        raise DownloadError("source receipt has a different owner or version")
    request = source.get("latest_request")
    if not isinstance(request, dict) or not isinstance(request.get("end_exclusive"), str):
        raise DownloadError("invalid pricing source request receipt")
    end = timestamp(request["end_exclusive"])
    expected = boundary_paths(source.get("anchor_month", ""))
    boundaries = source.get("boundaries")
    if not isinstance(boundaries, dict) or set(boundaries) != set(expected):
        raise DownloadError("source must select exactly three pricing boundaries")
    for kind, relative in expected.items():
        entry = boundaries[kind]
        if (
            not isinstance(entry, dict)
            or entry.get("manifest") != relative
            or not isinstance(entry.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
        ):
            raise DownloadError("invalid pricing boundary reference")
    if source["schema_version"] >= 4 or "validator" in source:
        reference = source.get("validator")
        if (
            not isinstance(reference, dict)
            or reference.get("file") != VALIDATOR_FILE
            or not isinstance(reference.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]) is None
        ):
            raise DownloadError("invalid pricing validator reference or history start")
    if source["schema_version"] >= 4:
        if (
            not isinstance(source.get("history_start"), str)
            or not timestamp(HISTORY_START) <= timestamp(source["history_start"]) < end
        ):
            raise DownloadError("invalid pricing history start")
    return source


def read_validator(root: Path, source: dict) -> dict | None:
    reference = source.get("validator")
    if reference is None:
        return None
    path = regular_path(root, VALIDATOR_FILE)
    if reference.get("file") != VALIDATOR_FILE or digest(path.read_bytes()) != reference.get("sha256"):
        raise DownloadError("first/last-session validator checksum differs")
    return validator_endpoints(read_document(path), source["symbol"])


def boundary_document(root: Path, reference: dict, symbol: str, kind: str) -> dict:
    path = regular_path(root, reference["manifest"])
    data = path.read_bytes()
    if digest(data) != reference["sha256"]:
        raise DownloadError(f"boundary manifest changed: {path}")
    document = read_document(path)
    if (
        document.get("schema_version") not in SUPPORTED_VERSIONS
        or document.get("boundary") != "alpaca_pricing_" + kind
        or document.get("symbol") != symbol
    ):
        raise DownloadError("pricing boundary owner differs")
    entries = document.get("adjustments")
    if (
        not isinstance(entries, list)
        or any(not isinstance(entry, dict) for entry in entries)
        or tuple(entry.get("adjustment") for entry in entries) != ADJUSTMENTS
    ):
        raise DownloadError("pricing boundary adjustment inventory differs")
    prefix = "completed_years." if kind == "completed_years" else ""
    allowed = {prefix + name.replace(",", "_") + ".csv.gz" for name in ADJUSTMENTS}
    previous = {}
    for entry in entries:
        if (
            type(entry.get("rows")) is not int
            or entry["rows"] < 0
            or not isinstance(entry.get("logical_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", entry["logical_sha256"]) is None
        ):
            raise DownloadError("invalid pricing view row count or digest")
        storage = entry.get("storage")
        if storage == "empty":
            if entry.get("rows") != 0 or "file" in entry:
                raise DownloadError("invalid empty pricing view")
        elif (
            storage in ("file", "exact_copy") and isinstance(entry.get("file"), str) and entry["file"] in allowed
        ):
            if (
                entry["rows"] == 0
                or type(entry.get("bytes")) is not int
                or entry["bytes"] < 0
                or not isinstance(entry.get("sha256"), str)
                or re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is None
                or not isinstance(entry.get("first"), str)
                or not isinstance(entry.get("last"), str)
                or timestamp(entry["first"]) > timestamp(entry["last"])
            ):
                raise DownloadError("invalid pricing payload metadata")
            if storage == "file":
                if entry["file"] != prefix + entry["adjustment"].replace(",", "_") + ".csv.gz":
                    raise DownloadError("pricing file name does not match its adjustment")
            else:
                equivalent = entry.get("equivalent_to")
                original = previous.get(equivalent) if isinstance(equivalent, str) else None
                fields = ("file", "sha256", "bytes", "rows", "logical_sha256", "first", "last")
                if (
                    original is None
                    or original["storage"] != "file"
                    or any(entry.get(key) != original.get(key) for key in fields)
                ):
                    raise DownloadError("invalid exact-copy pricing alias")
        else:
            raise DownloadError("invalid pricing storage entry")
        previous[entry["adjustment"]] = entry
    return document


def read_boundary(root: Path, source: dict, kind: str) -> dict:
    reference = source["boundaries"][kind]
    document = boundary_document(root, reference, source["symbol"], kind)
    directory = PurePosixPath(reference["manifest"]).parent
    decoded = {}
    views = {}
    for entry in document["adjustments"]:
        if entry["storage"] == "empty":
            rows = ()
        else:
            filename = entry["file"]
            if filename not in decoded:
                packed = regular_path(root, str(directory / filename)).read_bytes()
                if len(packed) != entry["bytes"] or digest(packed) != entry["sha256"]:
                    raise DownloadError("pricing payload checksum differs")
                decoded[filename] = decode_csv(packed)
            rows = decoded[filename]
        if len(rows) != entry["rows"] or digest(csv_bytes(rows)) != entry["logical_sha256"]:
            raise DownloadError("pricing logical data differs from manifest")
        views[entry["adjustment"]] = rows
    return views


def owned_files(root: Path, source: dict) -> dict[str, str]:
    result = {}
    if source.get("validator") is not None:
        result[VALIDATOR_FILE] = source["validator"]["sha256"]
    for kind, reference in source["boundaries"].items():
        document = boundary_document(root, reference, source["symbol"], kind)
        result[reference["manifest"]] = reference["sha256"]
        directory = PurePosixPath(reference["manifest"]).parent
        for entry in document["adjustments"]:
            if entry["storage"] == "file":
                result[str(directory / entry["file"])] = entry["sha256"]
    return result


def _discard_staging(root: Path) -> None:
    staging = regular_path(root, ".staging")
    if not staging.exists():
        return
    entries = list(staging.iterdir())
    if len(entries) > 64 or any(
        not re.fullmatch(r"write-\d{3}", entry.name) or entry.is_symlink() or not entry.is_file()
        for entry in entries
    ):
        raise DownloadError("unexpected files in reserved pricing staging directory; refusing cleanup")
    for entry in entries:
        entry.unlink()
    staging.rmdir()
    fsync_directory(root)


def _finish_publication(root: Path, journal: dict, *, on_check=None, check_event=None) -> None:
    if journal.get("schema_version") not in SUPPORTED_VERSIONS or not isinstance(journal.get("writes"), list):
        raise DownloadError("invalid pricing publication journal")
    writes = journal["writes"]
    if (
        not writes
        or len(writes) > 64
        or not isinstance(writes[-1], dict)
        or writes[-1].get("path") != "source.manifest.json"
    ):
        raise DownloadError("publication must select its source receipt last")
    removals = journal.get("remove")
    if not isinstance(removals, dict) or len(removals) > 64:
        raise DownloadError("invalid pricing retirement inventory")
    update = journal.get("update")
    if update is not None:
        if (
            not isinstance(update, dict)
            or set(update) != {"sha256"}
            or not re.fullmatch(r"[0-9a-f]{64}", update.get("sha256", ""))
        ):
            raise DownloadError("invalid update-manifest journal entry")
        update_path(root)  # Check the destination before changing any pricing files.
    targets = set()
    for entry in writes:
        if not isinstance(entry, dict):
            raise DownloadError("invalid pricing publication entry")
        relative, checksum = entry.get("path"), entry.get("sha256")
        _check_publication_entry(relative, checksum)
        regular_path(root, relative)
        if relative in targets:
            raise DownloadError("duplicate pricing publication target")
        targets.add(relative)
    for relative, checksum in removals.items():
        _check_publication_entry(relative, checksum)
        regular_path(root, relative)
        if relative in targets or relative == "source.manifest.json":
            raise DownloadError("cannot retire a selected publication target")
    # Recovery queues too: a deleted cache must not hide a resumed publication.
    from .update_checks import queue_check, read_checks

    store = root.parents[2]
    event = check_event
    if event is None:
        event = queue_check(store, root.name)
        if on_check is not None:
            on_check(event)
    for index, entry in enumerate(writes):
        target = regular_path(root, entry["path"])
        staged = regular_path(root, f".staging/write-{index:03d}")
        if staged.exists():
            if digest(staged.read_bytes()) != entry["sha256"]:
                raise DownloadError("staged pricing output failed verification")
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
            fsync_directory(target.parent)
            fsync_directory(target.parent.parent)
        if digest(target.read_bytes()) != entry["sha256"]:
            raise DownloadError("installed pricing output failed verification")
    # No prior month/year is retired until the complete replacement is selected.
    parents = set()
    for relative, expected in removals.items():
        path = regular_path(root, relative)
        if path.exists():
            if digest(path.read_bytes()) != expected:
                raise DownloadError(f"retired pricing file changed; refusing deletion: {relative}")
            path.unlink()
            fsync_directory(path.parent)
        parents.update(parent for parent in path.parents if parent != root and root in parent.parents)
    for parent in sorted(parents, key=lambda path: len(path.parts), reverse=True):
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
            fsync_directory(parent.parent)
    if update is not None:
        target = update_path(root)
        staged = regular_path(root, f".staging/write-{len(writes):03d}")
        if staged.exists():
            if digest(staged.read_bytes()) != update["sha256"]:
                raise DownloadError("staged update manifest failed verification")
            validate_update(read_document(staged))
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
            fsync_directory(target.parent)
            fsync_directory(target.parent.parent)
            fsync_directory(target.parent.parent.parent)
        if digest(target.read_bytes()) != update["sha256"]:
            raise DownloadError("installed update manifest failed verification")
    _discard_staging(root)
    regular_path(root, ".pending.json").unlink()
    fsync_directory(root)
    if read_checks(store, ())["epoch"] != event[0]:
        # Deletion/recreation during publication cannot lose this change.
        event = queue_check(store, root.name)
        if on_check is not None:
            on_check(event)


def _check_publication_entry(relative, checksum) -> None:
    payload = "(?:" + "|".join(re.escape(name.replace(",", "_")) for name in ADJUSTMENTS) + r")\.csv\.gz"
    month = "(?:" + "|".join(calendar.month_abbr[1:]) + ")"
    pattern = (
        r"(?:validator_first_last_sessions\.json|source\.manifest\.json|completed_years\.manifest\.json|completed_years\."
        + payload
        + r"|\d{4}/(?:year\.manifest\.json|"
        + payload
        + "|"
        + month
        + r"/(?:month\.manifest\.json|"
        + payload
        + ")))"
    )
    if (
        not isinstance(relative, str)
        or re.fullmatch(pattern, relative) is None
        or not isinstance(checksum, str)
        or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
    ):
        raise DownloadError("unsafe pricing publication or retirement entry")


def recover(root: Path, *, on_check=None) -> bool:
    pending = regular_path(root, ".pending.json")
    if pending.exists():
        _finish_publication(root, read_document(pending), on_check=on_check)
        return True
    _discard_staging(root)
    return False


def publish(
    root: Path,
    previous: dict | None,
    source: dict,
    documents: dict,
    payloads: dict,
    *,
    update: dict | None = None,
    repair_payloads: bool = False,
    on_check=None,
) -> int:
    old_files = owned_files(root, previous) if previous is not None else {}
    retained = {VALIDATOR_FILE: source["validator"]["sha256"]} if "validator" in source else {}
    for kind, reference in source["boundaries"].items():
        relative = reference["manifest"]
        document = documents.get(relative)
        if document is None:
            document = boundary_document(root, reference, source["symbol"], kind)
        retained[relative] = reference["sha256"]
        directory = PurePosixPath(relative).parent
        for entry in document["adjustments"]:
            if entry["storage"] == "file":
                retained[str(directory / entry["file"])] = entry["sha256"]
    outputs = dict(payloads)
    outputs.update((path, json_bytes(document)) for path, document in documents.items())
    outputs["source.manifest.json"] = json_bytes(source)
    changed = []
    for relative, content in outputs.items():
        path = regular_path(root, relative)
        if not path.exists() or path.read_bytes() != content:
            changed.append((relative, content))
    notice = None
    existing = read_update(root)
    if update is not None:
        update = validate_update(update)
        update = merge_ranges(existing, update)
        if update != existing:
            notice = json_bytes(update)
    if not changed and notice is None:
        return 0
    # Source receipt is also staged for an otherwise byte-identical selection.
    if not changed or changed[-1][0] != "source.manifest.json":
        changed.append(("source.manifest.json", json_bytes(source)))
    staging = regular_path(root, ".staging")
    staging.mkdir()
    pending = regular_path(root, ".pending.json")
    removals = {path: sha for path, sha in old_files.items() if path not in retained}
    if repair_payloads:
        # Intact receipts establish ownership. Explicit force may retire a damaged owned gzip.
        for path in removals:
            target = regular_path(root, path)
            if path.endswith(".csv.gz") and target.exists():
                removals[path] = digest(target.read_bytes())
    journal = {
        "schema_version": VERSION,
        "writes": [{"path": path, "sha256": digest(data)} for path, data in changed],
        "remove": removals,
        "update": {"sha256": digest(notice)} if notice is not None else None,
    }
    try:
        for index, (_relative, content) in enumerate(changed):
            write_file(staging / f"write-{index:03d}", content)
        if notice is not None:
            write_file(staging / f"write-{len(changed):03d}", notice)
        fsync_directory(staging)
        # Create journal through an atomic rename so partial JSON is never recovery authority.
        journal_stage = staging / f"write-{len(changed) + (notice is not None):03d}"
        write_file(journal_stage, json_bytes(journal))
        # Invalidate completion before the journal is visible, so a stopped
        # publication cannot be skipped by an older completed-batch memo.
        from .update_checks import queue_check

        event = queue_check(root.parents[2], source["symbol"])
        if on_check is not None:
            on_check(event)
        os.replace(journal_stage, pending)
        fsync_directory(root)
        _finish_publication(root, journal, on_check=on_check, check_event=event)
    except BaseException:
        if not pending.exists():
            _discard_staging(root)
        raise
    return len(changed) + (notice is not None)
