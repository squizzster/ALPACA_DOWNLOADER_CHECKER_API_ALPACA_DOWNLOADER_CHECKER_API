import hashlib
from pathlib import Path


def calculate_file_sha256(file_path: Path | str) -> str:
    """Return the SHA-256 digest of a file without loading it all into memory."""
    file_digest = hashlib.sha256()
    with Path(file_path).open("rb") as binary_file:
        for file_chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            file_digest.update(file_chunk)
    return file_digest.hexdigest()
