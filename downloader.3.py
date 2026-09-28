#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path, PurePosixPath
import shutil
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

import requests


# PATHS / CONFIG
MANIFEST_DIR = Path(os.getenv("MANIFEST_DIR", "manifests"))
FILTERED_MANIFEST = MANIFEST_DIR / "filtered_manifest_2000_2026.csv"

RAW_FILINGS_DIR = Path(os.getenv("FILING_DIR", "raw"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
DOWNLOAD_LOG = LOG_DIR / "core_filings_companyscope_download_log.csv"

SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "IbrahimHussain ibrahimbeaconarion@gmail.com",
).strip()
SEC_REQUEST_INTERVAL_SECONDS = float(os.getenv("SEC_REQUEST_INTERVAL_SECONDS", "0.15"))

REQUEST_TIMEOUT = (15, 120)
TRANSIENT_RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
PROGRESS_EVERY = 1000

_thread_local = threading.local()
_request_rate_lock = threading.Lock()
_last_request_started = 0.0


# HELPERS
def require_user_agent() -> None:
    if not SEC_USER_AGENT:
        raise SystemExit(
            "SEC_USER_AGENT is not set.\n"
            "Example:\n"
            'export SEC_USER_AGENT="Your Name your.email@example.com"'
        )


def human_bytes(n: int | float) -> str:
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if n < 1024.0 or unit == units[-1]:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} B"


def fmt_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60.0
    if minutes < 60:
        return f"{minutes:.1f}m"
    return f"{minutes / 60.0:.2f}h"


def free_bytes(path: Path) -> int:
    return int(shutil.disk_usage(path).free)


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        })
        _thread_local.session = session
    return session


def wait_for_request_slot() -> None:
    """Throttle request starts globally across all worker threads."""
    global _last_request_started

    with _request_rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_started
        if elapsed < SEC_REQUEST_INTERVAL_SECONDS:
            time.sleep(SEC_REQUEST_INTERVAL_SECONDS - elapsed)
        _last_request_started = time.monotonic()


def retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
    return float(min(60, 2**attempt))


def normalize_manifest_output_path(value: str) -> tuple[str, ...]:
    """Normalise old Windows and new POSIX manifest paths into relative parts."""
    raw_value = (value or "").strip()
    if not raw_value:
        raise ValueError("Manifest row has an empty output_path")

    normalized = raw_value.replace("\\", "/")
    logical = PurePosixPath(normalized)

    if logical.is_absolute():
        raise ValueError(f"Manifest output_path must be relative: {value!r}")

    parts = list(logical.parts)
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Unsafe manifest output_path: {value!r}")
    if ":" in parts[0]:
        raise ValueError(f"Absolute/drive-qualified output_path is not allowed: {value!r}")

    # Existing manifests use raw/<year>/<form>/<file>.  FILING_DIR is the
    # physical raw root, so remove that logical prefix before joining.
    if parts[0].lower() == "raw":
        parts = parts[1:]

    if len(parts) < 3:
        raise ValueError(f"Unexpected filing output_path structure: {value!r}")

    return tuple(parts)


def resolve_output_path(value: str) -> Path:
    return RAW_FILINGS_DIR.joinpath(*normalize_manifest_output_path(value))


def existing_valid_file_size(path: Path) -> int | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise IsADirectoryError(f"Expected filing file but found non-file path: {path}")

    size = path.stat().st_size
    return size if size > 0 else None


def log_csv_header_if_needed(path: Path, header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as output:
            csv.writer(output).writerow(header)


def load_permanent_failures(log_path: Path) -> set[str]:
    permanent: set[str] = set()
    if not log_path.exists():
        return permanent

    with log_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        for row in reader:
            status = (row.get("status") or "").strip()
            url = (row.get("filing_url") or "").strip()
            if status in {"404", "410"} and url:
                permanent.add(url)
    return permanent


def append_log_row(log_path: Path, row: dict[str, Any]) -> None:
    log_csv_header_if_needed(
        log_path,
        ["timestamp", "filing_url", "output_path", "status", "bytes_written", "error_message"],
    )
    with log_path.open("a", newline="", encoding="utf-8") as output:
        csv.writer(output).writerow([
            row.get("timestamp", ""),
            row.get("filing_url", ""),
            row.get("output_path", ""),
            row.get("status", ""),
            row.get("bytes_written", ""),
            row.get("error_message", ""),
        ])


def safe_mkdir_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_response_to_file(response: requests.Response, dest: Path) -> int:
    safe_mkdir_parent(dest)
    temporary = dest.with_suffix(dest.suffix + ".part")
    written = 0

    try:
        with temporary.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
                    written += len(chunk)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    if written <= 0:
        temporary.unlink(missing_ok=True)
        raise IOError(f"Downloaded empty response for {response.url}")

    temporary.replace(dest)
    return written


# DOWNLOAD
def pre_scan_manifest(
    manifest_path: Path,
    permanent_failures: set[str],
) -> dict[str, Any]:
    total_rows = 0
    skip_existing = 0
    skip_permanent = 0
    pending = 0
    zero_byte_existing = 0
    sample_sizes: list[int] = []

    with manifest_path.open("r", newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        required = {"filing_url", "output_path"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            missing = required - set(reader.fieldnames or [])
            raise ValueError(f"Manifest missing required columns: {sorted(missing)}")

        for row in reader:
            total_rows += 1
            filing_url = row["filing_url"].strip()
            output_path = resolve_output_path(row["output_path"])

            if filing_url in permanent_failures:
                skip_permanent += 1
                continue

            existing_size = existing_valid_file_size(output_path)
            if existing_size is not None:
                skip_existing += 1
                if len(sample_sizes) < 5000:
                    sample_sizes.append(existing_size)
                continue

            if output_path.exists():
                zero_byte_existing += 1
            pending += 1

            if total_rows % 250_000 == 0:
                print(
                    f"[prescan] scanned={total_rows:,} pending={pending:,} "
                    f"skip_existing={skip_existing:,} skip_permanent={skip_permanent:,} "
                    f"zero_byte={zero_byte_existing:,}",
                    flush=True,
                )

    avg_existing_size = (sum(sample_sizes) / len(sample_sizes)) if sample_sizes else None
    estimated_required_bytes = int(avg_existing_size * pending) if avg_existing_size else None
    return {
        "total_rows": total_rows,
        "skip_existing": skip_existing,
        "skip_permanent": skip_permanent,
        "pending": pending,
        "zero_byte_existing": zero_byte_existing,
        "avg_existing_size": avg_existing_size,
        "estimated_required_bytes": estimated_required_bytes,
    }


def download_one(row: dict[str, str], max_retries: int) -> dict[str, Any]:
    filing_url = row["filing_url"].strip()
    output_path = resolve_output_path(row["output_path"])

    existing_size = existing_valid_file_size(output_path)
    if existing_size is not None:
        return {
            "status": "exists",
            "filing_url": filing_url,
            "output_path": str(output_path),
            "bytes_written": 0,
            "error_message": "",
        }

    session = get_session()
    last_error = ""

    for attempt in range(max_retries + 1):
        response: requests.Response | None = None
        try:
            wait_for_request_slot()
            response = session.get(filing_url, timeout=REQUEST_TIMEOUT, stream=True)

            if response.status_code in {404, 410}:
                return {
                    "status": str(response.status_code),
                    "filing_url": filing_url,
                    "output_path": str(output_path),
                    "bytes_written": 0,
                    "error_message": f"{response.status_code} Client Error",
                }

            if response.status_code in TRANSIENT_RETRY_STATUSES:
                last_error = f"HTTP {response.status_code}"
                if attempt < max_retries:
                    time.sleep(retry_delay(response, attempt + 1))
                    continue
                return {
                    "status": "failed",
                    "filing_url": filing_url,
                    "output_path": str(output_path),
                    "bytes_written": 0,
                    "error_message": last_error,
                }

            response.raise_for_status()
            written = save_response_to_file(response, output_path)
            return {
                "status": "downloaded",
                "filing_url": filing_url,
                "output_path": str(output_path),
                "bytes_written": written,
                "error_message": "",
            }

        except requests.HTTPError as exc:
            code = getattr(exc.response, "status_code", None)
            if code in {404, 410}:
                return {
                    "status": str(code),
                    "filing_url": filing_url,
                    "output_path": str(output_path),
                    "bytes_written": 0,
                    "error_message": str(exc),
                }

            last_error = str(exc)
            if code in TRANSIENT_RETRY_STATUSES and attempt < max_retries:
                time.sleep(retry_delay(exc.response, attempt + 1))
                continue
            break

        except (requests.ConnectionError, requests.Timeout, requests.RequestException) as exc:
            last_error = str(exc)
            if attempt < max_retries:
                time.sleep(retry_delay(response, attempt + 1))
                continue
            break

        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            break

        finally:
            if response is not None:
                response.close()

    return {
        "status": "failed",
        "filing_url": filing_url,
        "output_path": str(output_path),
        "bytes_written": 0,
        "error_message": last_error,
    }


def download_manifest(
    manifest_path: Path,
    workers: int,
    max_pending_futures: int,
    min_free_gb: float,
    max_retries: int,
    force: bool,
) -> None:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    if max_pending_futures < workers:
        raise ValueError("max_pending_futures must be >= workers")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if min_free_gb < 0:
        raise ValueError("min_free_gb must be >= 0")
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Filtered manifest not found: {manifest_path}")

    min_free_bytes = int(min_free_gb * (1024**3))

    current_free = free_bytes(RAW_FILINGS_DIR)
    print(f"[download] current free disk: {human_bytes(current_free)}", flush=True)
    if current_free < min_free_bytes:
        raise SystemExit(
            f"Refusing to start: free disk space {human_bytes(current_free)} "
            f"is below the required minimum of {human_bytes(min_free_bytes)}"
        )

    permanent_failures = load_permanent_failures(DOWNLOAD_LOG)
    print(
        f"[download] loaded {len(permanent_failures):,} permanent 404/410 failures from log",
        flush=True,
    )

    pre = pre_scan_manifest(manifest_path, permanent_failures)
    print(
        f"[download] manifest rows={pre['total_rows']:,} "
        f"pending={pre['pending']:,} "
        f"skip_existing={pre['skip_existing']:,} "
        f"skip_permanent={pre['skip_permanent']:,} "
        f"zero_byte_existing={pre['zero_byte_existing']:,}",
        flush=True,
    )

    estimated = pre["estimated_required_bytes"]
    if estimated is not None:
        print(
            f"[download] avg existing file size ≈ {human_bytes(pre['avg_existing_size'])} "
            f"| estimated bytes needed for pending ≈ {human_bytes(estimated)}",
            flush=True,
        )
        safe_available = max(0, current_free - min_free_bytes)
        if estimated > safe_available and not force:
            raise SystemExit(
                f"Estimated required space {human_bytes(estimated)} exceeds safe available space "
                f"{human_bytes(safe_available)}.\n"
                f"Free disk={human_bytes(current_free)}, min reserve={human_bytes(min_free_bytes)}.\n"
                "Use --force to proceed anyway, or free space first."
            )

    start = time.time()
    submitted = 0
    completed = 0
    downloaded = 0
    skipped_existing = 0
    skipped_permanent = 0
    failed = 0
    downloaded_bytes = 0

    pending_futures: dict[Any, dict[str, str]] = {}

    def handle_result(result: dict[str, Any]) -> None:
        nonlocal completed, downloaded, skipped_existing, skipped_permanent, failed, downloaded_bytes

        completed += 1
        status = result["status"]
        if status == "downloaded":
            downloaded += 1
            downloaded_bytes += int(result["bytes_written"] or 0)
            append_log_row(DOWNLOAD_LOG, {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                **result,
            })
        elif status == "exists":
            skipped_existing += 1
        elif status in {"404", "410"}:
            skipped_permanent += 1
            append_log_row(DOWNLOAD_LOG, {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                **result,
            })
        else:
            failed += 1
            append_log_row(DOWNLOAD_LOG, {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                **result,
            })

        if completed % PROGRESS_EVERY == 0:
            elapsed = time.time() - start
            rate = completed / max(elapsed, 1e-9)
            print(
                f"[download] completed={completed:,} submitted={submitted:,} "
                f"downloaded={downloaded:,} exists={skipped_existing:,} "
                f"perm_skip={skipped_permanent:,} failed={failed:,} "
                f"bytes={human_bytes(downloaded_bytes)} | rate={rate:,.1f} items/s "
                f"| elapsed={fmt_elapsed(elapsed)}",
                flush=True,
            )

    try:
        with manifest_path.open("r", newline="", encoding="utf-8-sig") as source, \
             ThreadPoolExecutor(max_workers=workers) as executor:
            reader = csv.DictReader(source)

            for row in reader:
                filing_url = row["filing_url"].strip()
                output_path = resolve_output_path(row["output_path"])

                if filing_url in permanent_failures:
                    skipped_permanent += 1
                    continue

                if existing_valid_file_size(output_path) is not None:
                    skipped_existing += 1
                    continue

                if submitted % 500 == 0:
                    current_free = free_bytes(RAW_FILINGS_DIR)
                    if current_free < min_free_bytes:
                        print(
                            "[download] stopping due to low disk space. "
                            f"current_free={human_bytes(current_free)} < "
                            f"reserve={human_bytes(min_free_bytes)}",
                            flush=True,
                        )
                        break

                while len(pending_futures) >= max_pending_futures:
                    done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        pending_futures.pop(future, None)
                        handle_result(future.result())

                future = executor.submit(download_one, row, max_retries)
                pending_futures[future] = row
                submitted += 1

            while pending_futures:
                done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    pending_futures.pop(future, None)
                    handle_result(future.result())

    except KeyboardInterrupt:
        print(
            "\n[download] interrupted. Already-downloaded files are preserved.\n"
            "[download] Re-run the same command to resume.",
            flush=True,
        )
        raise

    elapsed = time.time() - start
    print(
        f"[download] done. submitted={submitted:,} completed={completed:,} "
        f"downloaded={downloaded:,} exists={skipped_existing:,} "
        f"perm_skip={skipped_permanent:,} failed={failed:,} "
        f"bytes={human_bytes(downloaded_bytes)} | elapsed={fmt_elapsed(elapsed)}",
        flush=True,
    )


# CLI
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download filings only from manifests/filtered_manifest_2000_2026.csv "
            "with safe resume, global SEC request throttling, and disk-space checks."
        )
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-pending-futures", type=int, default=128)
    parser.add_argument("--min-free-gb", type=float, default=12.0)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass only the estimated-space warning; minimum free-space reserve still applies.",
    )
    return parser.parse_args()


def main() -> None:
    require_user_agent()
    args = parse_args()

    RAW_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[download] manifest={FILTERED_MANIFEST}", flush=True)
    print(f"[download] filing_root={RAW_FILINGS_DIR}", flush=True)
    print(
        f"[download] global request interval={SEC_REQUEST_INTERVAL_SECONDS:.3f}s",
        flush=True,
    )

    download_manifest(
        manifest_path=FILTERED_MANIFEST,
        workers=args.workers,
        max_pending_futures=args.max_pending_futures,
        min_free_gb=args.min_free_gb,
        max_retries=args.max_retries,
        force=args.force,
    )


if __name__ == "__main__":
    main()

# Usage:
# python downloader.3.py
# python downloader.3.py --workers 8 --max-pending-futures 256 --min-free-gb 12 --max-retries 8 --force
