"""Metadata recording and tracking for ingested raw files."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def calculate_sha256(file_path: Path) -> str:
    """Computes the SHA-256 hexadecimal digest of a file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_file_metadata(
    file_path: Path,
    source_name: str,
    url: str,
) -> Path:
    """Writes a metadata JSON file alongside the downloaded raw file.

    Format: <filename>.meta.json
    Contains: source_name, url, downloaded_at (UTC ISO 8601), file_size_bytes, sha256.
    """
    meta_path = file_path.with_name(f"{file_path.name}.meta.json")
    stat = file_path.stat()

    metadata: dict[str, Any] = {
        "file_name": file_path.name,
        "source_name": source_name,
        "url": url,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "file_size_bytes": stat.st_size,
        "sha256": calculate_sha256(file_path),
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return meta_path


def read_file_metadata(file_path: Path) -> dict[str, Any] | None:
    """Reads metadata JSON if present for a file."""
    meta_path = file_path.with_name(f"{file_path.name}.meta.json")
    if meta_path.is_file():
        try:
            with open(meta_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None
