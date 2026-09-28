"""Resumable, bounded-thread, first-stage SEC filing extraction orchestrator."""

from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterator

from .database import connect_database, record_exists, write_filing
from .extract_document import canonical_form, load_filing_record, process_source_file
from .making_file import ensure_free_space, sha256_file

FORM_FOLDERS = ("8-K", "10-K", "10-Q", "DEF_14A")


def auto_workers() -> int:
    return max(1, os.cpu_count() or 1)


def scan_sources(raw_root: Path, form_folder: str) -> Iterator[Path]:
    if not raw_root.is_dir():
        raise FileNotFoundError(f"Raw filing root does not exist: {raw_root}")
    for year_dir in sorted(raw_root.iterdir()):
        if year_dir.is_symlink() or not year_dir.is_dir() or not (year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue
        form_dir = year_dir / form_folder
        if form_dir.is_symlink() or not form_dir.is_dir():
            continue
        for source in sorted(form_dir.iterdir()):
            if source.is_symlink() or not source.is_file() or source.suffix.lower() != ".txt" or source.name.endswith(".part"):
                continue
            yield source


def _jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as log:
        log.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        log.flush()
        os.fsync(log.fileno())


def _move_stage_to_target(stage: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"Filing output folder already exists: {target}")
    os.replace(stage, target)
    return target


def _same_source_stat(payload: dict[str, Any], stat_result: os.stat_result) -> bool:
    source = payload["filing"]
    return (int(source.get("source_size_bytes", -1)) == stat_result.st_size and
            int(source.get("source_mtime_ns", -1)) == stat_result.st_mtime_ns)


def _recover_orphan(target: Path, source: Path, connection: sqlite3.Connection,
                    source_relpath: str, source_stat: os.stat_result) -> bool:
    """Recover a crash after folder publication but before the SQLite commit."""
    marker = target / "_filing_record.json"
    if not marker.is_file():
        return False
    payload = load_filing_record(target)
    if not _same_source_stat(payload, source_stat):
        return False
    if payload["filing"].get("source_relpath") != source_relpath:
        return False
    if sha256_file(source) != payload["filing"].get("source_sha256"):
        return False
    write_filing(connection, payload)
    return True


def _process_one(source: Path, raw_root: Path, form_folder: str, form_root: Path,
                 max_zip_bytes: int, max_zip_members: int, min_free_bytes: int) -> dict[str, Any]:
    stage_root = form_root / ".staging"
    stage_root.mkdir(parents=True, exist_ok=True)
    return process_source_file(source, raw_root, form_folder, stage_root,
                               max_zip_bytes=max_zip_bytes, max_zip_members=max_zip_members,
                               min_free_bytes=min_free_bytes)


def _handle_result(result: dict[str, Any], raw_root: Path, form_root: Path,
                   connection: sqlite3.Connection, event_log: Path,
                   delete_raw_after_success: bool) -> str:
    payload = result["payload"]
    filing = payload["filing"]
    source = Path(result["source"])
    rel_path = filing["source_relpath"]
    target = form_root / filing["filing_folder"]
    stage = Path(result["stage_dir"])
    existing = record_exists(connection, rel_path)

    if target.exists():
        marker = target / "_filing_record.json"
        if not marker.is_file():
            raise FileExistsError(f"Existing filing directory has no recovery marker; refusing to overwrite: {target}")
        old_payload = load_filing_record(target)
        old_filing = old_payload["filing"]
        if old_filing.get("source_sha256") != filing.get("source_sha256"):
            raise FileExistsError(f"Source changed but a different processed filing folder already exists: {target}")
        shutil.rmtree(stage, ignore_errors=True)
        # Keep the marker aligned with current source stat fields after a harmless touch.
        marker_tmp = marker.with_suffix(".json.part")
        marker_tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(marker_tmp, marker)
    else:
        _move_stage_to_target(stage, target)
    write_filing(connection, payload)

    if delete_raw_after_success and payload.get("delete_safe", False):
        source.unlink()
        status = "processed_and_raw_deleted"
    elif delete_raw_after_success:
        status = "processed_with_warnings_raw_retained"
    else:
        status = "processed_raw_retained"
    _jsonl(event_log, {"event": status, "source": rel_path, "filing_id": filing.get("accession_number"),
                       "sha256": filing.get("source_sha256"), "output": str(target)})
    return status


def run_form(raw_root: Path, output_root: Path, form_folder: str, workers: int,
             delete_raw_after_success: bool = False, max_zip_bytes: int = 4 * 1024**3,
             max_zip_members: int = 100_000, progress_every: int = 1000,
             verify_hashes: bool = False, min_free_bytes: int = 10 * 1024**3) -> dict[str, int]:
    canonical_form(form_folder)
    form_root = output_root / form_folder
    form_root.mkdir(parents=True, exist_ok=True)
    db_path = form_root / f"{form_folder}_forms.db"
    schema_path = Path(__file__).with_name("schema.sql")
    logs_root = output_root / "logs"
    event_log = logs_root / "phase1_events.jsonl"
    error_log = logs_root / "phase1_errors.jsonl"
    connection = connect_database(db_path, schema_path)
    stats = {"scanned": 0, "processed": 0, "skipped": 0, "recovered": 0, "failed": 0, "deleted": 0}
    start_time = time.monotonic()
    pending: dict[concurrent.futures.Future[dict[str, Any]], tuple[Path, str]] = {}
    limit = max(1, workers * 2)

    def consume(future: concurrent.futures.Future[dict[str, Any]], source: Path, rel_path: str) -> None:
        result: dict[str, Any] | None = None
        try:
            result = future.result()
            status = _handle_result(result, raw_root, form_root, connection, event_log, delete_raw_after_success)
            stats["processed"] += 1
            if status == "processed_and_raw_deleted":
                stats["deleted"] += 1
        except Exception as exc:
            if result and Path(result.get("stage_dir", "")).is_dir():
                shutil.rmtree(result["stage_dir"], ignore_errors=True)
            stats["failed"] += 1
            _jsonl(error_log, {"event": "processing_failed", "source": rel_path,
                               "error_type": type(exc).__name__, "error": str(exc),
                               "timestamp_epoch": time.time()})
            print(f"[phase1:{form_folder}] ERROR {rel_path}: {type(exc).__name__}: {exc}", flush=True)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"sec-{form_folder}") as executor:
            for source in scan_sources(raw_root, form_folder):
                stats["scanned"] += 1
                rel_path = source.relative_to(raw_root).as_posix()
                source_stat = source.stat()
                existing = record_exists(connection, rel_path)
                if existing and int(existing["source_size_bytes"]) == source_stat.st_size and int(existing["source_mtime_ns"]) == source_stat.st_mtime_ns:
                    target = form_root / existing["filing_folder"]
                    hash_matches = True
                    if verify_hashes or delete_raw_after_success:
                        hash_matches = sha256_file(source) == existing["source_sha256"]
                    if hash_matches and target.is_dir() and (target / "_filing_record.json").is_file():
                        stats["skipped"] += 1
                        if delete_raw_after_success and existing["processing_status"] == "complete":
                            source.unlink()
                            stats["deleted"] += 1
                        continue
                target_name = source.stem
                target = form_root / target_name
                if target.is_dir() and existing is None:
                    try:
                        if _recover_orphan(target, source, connection, rel_path, source_stat):
                            stats["recovered"] += 1
                            if delete_raw_after_success and load_filing_record(target).get("delete_safe", False):
                                source.unlink()
                                stats["deleted"] += 1
                            continue
                    except Exception as exc:
                        stats["failed"] += 1
                        _jsonl(error_log, {"event": "orphan_recovery_failed", "source": rel_path, "error": str(exc)})
                        continue
                while len(pending) >= limit:
                    done, _ = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                    for future in done:
                        source_done, rel_done = pending.pop(future)
                        consume(future, source_done, rel_done)
                future = executor.submit(_process_one, source, raw_root, form_folder, form_root,
                                         max_zip_bytes, max_zip_members, min_free_bytes)
                pending[future] = (source, rel_path)
                if stats["scanned"] % progress_every == 0:
                    print(f"[phase1:{form_folder}] scanned={stats['scanned']:,} submitted={stats['scanned'] - stats['skipped'] - stats['recovered']:,} "
                          f"processed={stats['processed']:,} failed={stats['failed']:,} workers={workers}", flush=True)
            while pending:
                done, _ = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    source_done, rel_done = pending.pop(future)
                    consume(future, source_done, rel_done)
    except KeyboardInterrupt:
        print(f"[phase1:{form_folder}] interrupted; committed filings and raw sources are preserved. Rerun to resume.", flush=True)
        raise
    finally:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.close()
    elapsed = time.monotonic() - start_time
    print(f"[phase1:{form_folder}] complete scanned={stats['scanned']:,} processed={stats['processed']:,} "
          f"skipped={stats['skipped']:,} recovered={stats['recovered']:,} failed={stats['failed']:,} "
          f"raw_deleted={stats['deleted']:,} elapsed={elapsed:.1f}s db={db_path}", flush=True)
    return stats


def run_pipeline(raw_root: Path, output_root: Path, forms: tuple[str, ...] = FORM_FOLDERS,
                 workers: int | None = None, delete_raw_after_success: bool = False,
                 max_zip_bytes: int = 4 * 1024**3, max_zip_members: int = 100_000,
                 progress_every: int = 1000, verify_hashes: bool = False,
                 min_free_gb: float = 10.0) -> dict[str, dict[str, int]]:
    raw_root, output_root = raw_root.resolve(), output_root.resolve()
    if raw_root == output_root or raw_root in output_root.parents or output_root in raw_root.parents:
        raise ValueError("Raw and processed roots must be separate, non-nested directories")
    actual_workers = workers or auto_workers()
    if actual_workers < 1:
        raise ValueError("workers must be at least 1")
    if min_free_gb < 0:
        raise ValueError("min_free_gb cannot be negative")
    min_free_bytes = int(min_free_gb * 1024**3)
    ensure_free_space(output_root, min_free_bytes)
    print(f"[phase1] raw={raw_root} output={output_root} workers={actual_workers} "
          f"delete_raw_after_success={delete_raw_after_success} min_free_gb={min_free_gb}", flush=True)
    return {form: run_form(raw_root, output_root, form, actual_workers, delete_raw_after_success,
                           max_zip_bytes, max_zip_members, progress_every, verify_hashes,
                           min_free_bytes) for form in forms}
