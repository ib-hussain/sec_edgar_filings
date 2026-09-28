"""Safe path and ZIP extraction helpers for filing attachments."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import zipfile
from pathlib import Path, PurePosixPath


class StorageReserveError(RuntimeError):
    """Raised before extraction consumes the configured free-space reserve."""


def ensure_free_space(path: Path, reserve_bytes: int) -> None:
    if reserve_bytes <= 0:
        return
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    free = shutil.disk_usage(probe).free
    if free < reserve_bytes:
        raise StorageReserveError(f"Free space {free:,} bytes is below configured reserve {reserve_bytes:,} bytes")


def safe_filename(name: str, fallback: str = "document.txt") -> str:
    name = name.replace("\\", "/").split("/")[-1].strip()
    name = re.sub(r"[^A-Za-z0-9._() -]+", "_", name).strip(" .")
    if name in {"", ".", ".."}:
        name = fallback
    return name[:180]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_zip_safely(archive: Path, destination: Path, max_uncompressed_bytes: int,
                       max_members: int, min_free_bytes: int = 0) -> tuple[int, int, list[dict[str, object]]]:
    """Extract a ZIP without traversal, symlink, or excessive-expansion writes."""
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    total = 0
    written = 0
    extracted: list[dict[str, object]] = []
    seen_targets: set[str] = set()
    with zipfile.ZipFile(archive) as zf:
        members = zf.infolist()
        if len(members) > max_members:
            raise ValueError(f"ZIP has {len(members)} members; limit is {max_members}")
        if sum(max(0, x.file_size) for x in members) > max_uncompressed_bytes:
            raise ValueError("ZIP uncompressed size exceeds configured limit")
        for info in members:
            raw_name = info.filename.replace("\\", "/")
            member = PurePosixPath(raw_name)
            if (member.is_absolute() or re.match(r"^[A-Za-z]:", raw_name) or
                    any(part in {"..", ""} or ":" in part for part in member.parts)):
                raise ValueError(f"Unsafe ZIP member path: {info.filename!r}")
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError(f"ZIP symlink is not extracted: {info.filename!r}")
            target = (root / Path(*member.parts)).resolve()
            if target != root and root not in target.parents:
                raise ValueError(f"ZIP member escapes extraction directory: {info.filename!r}")
            target_key = target.relative_to(root).as_posix().casefold()
            if not info.is_dir():
                if target_key in seen_targets:
                    raise ValueError(f"ZIP contains duplicate/case-colliding member path: {info.filename!r}")
                seen_targets.add(target_key)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            size = 0
            digest = hashlib.sha256()
            with zf.open(info) as src, target.open("wb") as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    total += len(chunk)
                    if total > max_uncompressed_bytes:
                        raise ValueError("ZIP exceeded configured extraction limit while expanding")
                    ensure_free_space(destination, min_free_bytes)
                    dst.write(chunk)
                    digest.update(chunk)
            written += 1
            extracted.append({"member_name": raw_name, "relative_path": target.relative_to(root).as_posix(),
                              "size_bytes": size, "sha256": digest.hexdigest()})
    return written, total, extracted
