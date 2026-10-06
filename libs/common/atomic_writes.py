import json
import os
import tempfile
from pathlib import Path


def create_temporary_sibling_path(destination_path: Path | str) -> Path:
    """Create an empty randomized temporary file beside its destination."""
    destination_path = Path(destination_path)
    file_descriptor, temporary_filename = tempfile.mkstemp(
        dir=destination_path.parent,
        prefix=f".{destination_path.name}.tmp.",
    )
    os.close(file_descriptor)
    return Path(temporary_filename)


def write_json_document_atomically(destination_path: Path, document: dict, *, compact: bool) -> None:
    """Fsync JSON into a sibling temporary file, then atomically replace the destination."""
    temporary_path = create_temporary_sibling_path(destination_path)
    try:
        with temporary_path.open("w", encoding="utf-8") as json_file:
            if compact:
                json.dump(document, json_file, separators=(",", ":"))
            else:
                json.dump(document, json_file, indent=2)
            json_file.write("\n")
            json_file.flush()
            os.fsync(json_file.fileno())
        os.replace(temporary_path, destination_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
