#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shutil
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import requests


# ============================================================
# PATHS / CONFIG
# ============================================================

dataPath = Path(os.getenv("dataPathGlobal", "data"))

DEFAULT_INPUT_MANIFEST = (
    dataPath / "sec_edgar" / "processed" / "manifests" / "filings_manifest_2000_2024.csv"
)

DEFAULT_FILTERED_MANIFEST = (
    dataPath / "sec_edgar" / "processed" / "manifests" / "filings_manifest_core_companyscope_2000_2024.csv"
)

DEFAULT_CIK_MAP = (
    dataPath / "sec_edgar" / "processed" / "cleaned" / "cik_ticker_map_cleaned.csv"
)

DEFAULT_ISSUER_TICKERS = (
    dataPath / "sec_edgar" / "processed" / "cleaned" / "issuer_master_onlyTickers.csv"
)

RAW_FILINGS_DIR = dataPath / "sec_edgar" / "raw" / "filings_txt"
LOG_DIR = dataPath / "sec_edgar" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DOWNLOAD_LOG = LOG_DIR / "core_filings_companyscope_download_log.csv"
SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "IbrahimHussain ibrahimbeaconarion@gmail.com"
).strip()

CORE_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}

REQUEST_TIMEOUT = (15, 120)
TRANSIENT_RETRY_STATUSES = {429, 500, 502, 503, 504}
PROGRESS_EVERY = 1000

_thread_local = threading.local()


# ============================================================
# HELPERS
# ============================================================

def require_user_agent() -> None:
    if not SEC_USER_AGENT:
        raise SystemExit(
            "SEC_USER_AGENT is not set.\n"
            "Example:\n"
            'export SEC_USER_AGENT="IbrahimHussain ibrahimbeaconarion@gmail.com"'
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
    hours = minutes / 60.0
    return f"{hours:.2f}h"


def free_bytes(path: Path) -> int:
    usage = shutil.disk_usage(path)
    return int(usage.free)


def form_is_core(form_type: str) -> bool:
    return form_type.strip().upper() in CORE_FORMS


def year_in_range(date_filed: str, start_year: int, end_year: int) -> bool:
    if not date_filed or len(date_filed) < 4:
        return False
    try:
        y = int(date_filed[:4])
        return start_year <= y <= end_year
    except Exception:
        return False


def get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        })
        _thread_local.session = sess
    return sess


def log_csv_header_if_needed(path: Path, header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)


def load_permanent_failures(log_path: Path) -> set[str]:
    permanent: set[str] = set()
    if not log_path.exists():
        return permanent

    with log_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
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
    with log_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            row.get("timestamp", ""),
            row.get("filing_url", ""),
            row.get("output_path", ""),
            row.get("status", ""),
            row.get("bytes_written", ""),
            row.get("error_message", ""),
        ])


def safe_mkdir_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_response_to_file(resp: requests.Response, dest: Path) -> int:
    safe_mkdir_parent(dest)
    tmp = dest.with_suffix(dest.suffix + ".part")

    written = 0
    with tmp.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                written += len(chunk)

    tmp.replace(dest)
    return written


def normalize_cik(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return ""
    # manifest CIKs are usually unpadded numerics
    s = s.lstrip("0")
    return s if s else "0"


def detect_cik_column(fieldnames: list[str]) -> str:
    lowered = {c.lower(): c for c in fieldnames}
    candidates = [
        "cik",
        "padded_cik",
        "sec_cik",
        "issuer_cik",
    ]
    for cand in candidates:
        if cand in lowered:
            return lowered[cand]
    raise RuntimeError(f"Could not find a CIK column in: {fieldnames}")


def load_cik_union(csv_paths: list[Path]) -> set[str]:
    """
    Load union of CIKs into RAM.
    This is tiny enough to keep in memory.
    """
    cik_set: set[str] = set()

    for path in csv_paths:
        if not path.exists():
            raise FileNotFoundError(f"Required CIK source file not found: {path}")

        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            if not fieldnames:
                raise RuntimeError(f"CSV has no header: {path}")

            cik_col = detect_cik_column(fieldnames)

            loaded = 0
            for row in reader:
                cik = normalize_cik(row.get(cik_col, ""))
                if cik:
                    cik_set.add(cik)
                    loaded += 1

            print(
                f"[cik-union] loaded {loaded:,} rows from {path} | cumulative unique CIKs={len(cik_set):,}",
                flush=True,
            )

    return cik_set


# ============================================================
# STAGE 1 — FILTER MANIFEST
# ============================================================

def filter_manifest_by_cik_and_form(
    input_manifest: Path,
    output_manifest: Path,
    cik_union: set[str],
    start_year: int,
    end_year: int,
    overwrite: bool,
) -> None:
    if output_manifest.exists() and not overwrite:
        print(f"[filter] Output already exists: {output_manifest}")
        print("[filter] Use --overwrite to rebuild it.")
        return

    output_manifest.parent.mkdir(parents=True, exist_ok=True)

    total_in = 0
    kept_form_year = 0
    kept_final = 0
    dropped_non_core = 0
    dropped_year = 0
    dropped_cik = 0

    start = time.time()

    with input_manifest.open("r", newline="", encoding="utf-8") as in_f, \
         output_manifest.open("w", newline="", encoding="utf-8") as out_f:
        reader = csv.DictReader(in_f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise RuntimeError(f"Manifest has no header: {input_manifest}")

        required = {"cik", "form_type", "date_filed", "filing_url", "output_path"}
        missing = required - set(fieldnames)
        if missing:
            raise RuntimeError(f"Manifest missing required columns: {sorted(missing)}")

        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total_in += 1

            form_type = row["form_type"]
            date_filed = row["date_filed"]
            cik = normalize_cik(row["cik"])

            if not form_is_core(form_type):
                dropped_non_core += 1
                continue

            if not year_in_range(date_filed, start_year, end_year):
                dropped_year += 1
                continue

            kept_form_year += 1

            if cik not in cik_union:
                dropped_cik += 1
                continue

            writer.writerow(row)
            kept_final += 1

            if total_in % 500_000 == 0:
                elapsed = time.time() - start
                rate = total_in / max(elapsed, 1e-9)
                print(
                    f"[filter] scanned={total_in:,} kept={kept_final:,} "
                    f"| kept_form_year={kept_form_year:,} dropped_cik={dropped_cik:,} "
                    f"| rate={rate:,.0f} rows/s | elapsed={fmt_elapsed(elapsed)}",
                    flush=True,
                )

    elapsed = time.time() - start
    print(
        f"[filter] done. scanned={total_in:,} kept={kept_final:,} "
        f"| dropped_non_core={dropped_non_core:,} dropped_year={dropped_year:,} dropped_cik={dropped_cik:,} "
        f"| elapsed={fmt_elapsed(elapsed)} | output={output_manifest}",
        flush=True,
    )


# ============================================================
# STAGE 2 — DOWNLOAD
# ============================================================

def pre_scan_manifest(
    manifest_path: Path,
    permanent_failures: set[str],
) -> dict[str, Any]:
    total_rows = 0
    skip_existing = 0
    skip_permanent = 0
    pending = 0
    sample_sizes: list[int] = []

    with manifest_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            filing_url = row["filing_url"]
            output_path = Path(row["output_path"])

            if filing_url in permanent_failures:
                skip_permanent += 1
                continue

            if output_path.exists():
                skip_existing += 1
                try:
                    sz = output_path.stat().st_size
                    if sz > 0 and len(sample_sizes) < 5000:
                        sample_sizes.append(sz)
                except Exception:
                    pass
                continue

            pending += 1

            if total_rows % 250_000 == 0:
                print(
                    f"[prescan] scanned={total_rows:,} pending={pending:,} "
                    f"skip_existing={skip_existing:,} skip_permanent={skip_permanent:,}",
                    flush=True,
                )

    avg_existing_size = (sum(sample_sizes) / len(sample_sizes)) if sample_sizes else None
    estimated_required_bytes = int(avg_existing_size * pending) if avg_existing_size else None

    return {
        "total_rows": total_rows,
        "skip_existing": skip_existing,
        "skip_permanent": skip_permanent,
        "pending": pending,
        "avg_existing_size": avg_existing_size,
        "estimated_required_bytes": estimated_required_bytes,
    }


def download_one(row: dict[str, str], max_retries: int) -> dict[str, Any]:
    filing_url = row["filing_url"]
    output_path = Path(row["output_path"])

    if output_path.exists():
        return {
            "status": "exists",
            "filing_url": filing_url,
            "output_path": str(output_path),
            "bytes_written": 0,
            "error_message": "",
        }

    sess = get_session()
    last_error = ""

    for attempt in range(max_retries + 1):
        try:
            resp = sess.get(filing_url, timeout=REQUEST_TIMEOUT, stream=True)

            if resp.status_code in {404, 410}:
                return {
                    "status": str(resp.status_code),
                    "filing_url": filing_url,
                    "output_path": str(output_path),
                    "bytes_written": 0,
                    "error_message": f"{resp.status_code} Client Error",
                }

            if resp.status_code in TRANSIENT_RETRY_STATUSES:
                last_error = f"HTTP {resp.status_code}"
                if attempt < max_retries:
                    wait_time = min(8, 2 ** attempt)
                    time.sleep(wait_time)
                    continue
                return {
                    "status": "failed",
                    "filing_url": filing_url,
                    "output_path": str(output_path),
                    "bytes_written": 0,
                    "error_message": last_error,
                }

            resp.raise_for_status()
            written = save_response_to_file(resp, output_path)

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
                wait_time = min(8, 2 ** attempt)
                time.sleep(wait_time)
                continue
            break

        except (requests.ConnectionError, requests.Timeout, requests.RequestException) as exc:
            last_error = str(exc)
            if attempt < max_retries:
                wait_time = min(8, 2 ** attempt)
                time.sleep(wait_time)
                continue
            break

        except Exception as exc:
            last_error = str(exc)
            break

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
    min_free_bytes = int(min_free_gb * (1024 ** 3))

    current_free = free_bytes(RAW_FILINGS_DIR)
    print(f"[download] current free disk: {human_bytes(current_free)}", flush=True)
    if current_free < min_free_bytes:
        raise SystemExit(
            f"Refusing to start: free disk space {human_bytes(current_free)} "
            f"is below the required minimum of {human_bytes(min_free_bytes)}"
        )

    permanent_failures = load_permanent_failures(DOWNLOAD_LOG)
    print(f"[download] loaded {len(permanent_failures):,} permanent 404/410 failures from log", flush=True)

    pre = pre_scan_manifest(manifest_path, permanent_failures)
    print(
        f"[download] manifest rows={pre['total_rows']:,} "
        f"pending={pre['pending']:,} "
        f"skip_existing={pre['skip_existing']:,} "
        f"skip_permanent={pre['skip_permanent']:,}",
        flush=True,
    )

    est = pre["estimated_required_bytes"]
    if est is not None:
        print(
            f"[download] avg existing file size ≈ {human_bytes(pre['avg_existing_size'])} "
            f"| estimated bytes needed for pending ≈ {human_bytes(est)}",
            flush=True,
        )
        safe_available = max(0, current_free - min_free_bytes)
        if est > safe_available and not force:
            raise SystemExit(
                f"Estimated required space {human_bytes(est)} exceeds safe available space "
                f"{human_bytes(safe_available)}.\n"
                f"Free disk={human_bytes(current_free)}, min reserve={human_bytes(min_free_bytes)}.\n"
                f"Use --force to proceed anyway, or free space first."
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
        with manifest_path.open("r", newline="", encoding="utf-8") as f, \
             ThreadPoolExecutor(max_workers=workers) as executor:
            reader = csv.DictReader(f)

            for row in reader:
                filing_url = row["filing_url"]
                output_path = Path(row["output_path"])

                if filing_url in permanent_failures:
                    skipped_permanent += 1
                    continue

                if output_path.exists():
                    skipped_existing += 1
                    continue

                if submitted % 500 == 0:
                    current_free = free_bytes(RAW_FILINGS_DIR)
                    if current_free < min_free_bytes:
                        print(
                            f"[download] stopping due to low disk space. "
                            f"current_free={human_bytes(current_free)} < reserve={human_bytes(min_free_bytes)}",
                            flush=True,
                        )
                        break

                while len(pending_futures) >= max_pending_futures:
                    done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
                    for fut in done:
                        pending_futures.pop(fut, None)
                        result = fut.result()
                        handle_result(result)

                fut = executor.submit(download_one, row, max_retries)
                pending_futures[fut] = row
                submitted += 1

            while pending_futures:
                done, _ = wait(pending_futures.keys(), return_when=FIRST_COMPLETED)
                for fut in done:
                    pending_futures.pop(fut, None)
                    result = fut.result()
                    handle_result(result)

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


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter giant SEC filings manifest by company CIK union + selected forms, then download with resume support."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_filter = sub.add_parser("filter-manifest", help="Stream-filter the giant manifest down to unioned-company core forms.")
    p_filter.add_argument("--input", type=str, default=str(DEFAULT_INPUT_MANIFEST))
    p_filter.add_argument("--output", type=str, default=str(DEFAULT_FILTERED_MANIFEST))
    p_filter.add_argument("--cik-map", type=str, default=str(DEFAULT_CIK_MAP))
    p_filter.add_argument("--issuer-tickers", type=str, default=str(DEFAULT_ISSUER_TICKERS))
    p_filter.add_argument("--start-year", type=int, default=2000)
    p_filter.add_argument("--end-year", type=int, default=2024)
    p_filter.add_argument("--overwrite", action="store_true")

    p_download = sub.add_parser("download", help="Download filtered company-scope core filings in parallel.")
    p_download.add_argument("--manifest", type=str, default=str(DEFAULT_FILTERED_MANIFEST))
    p_download.add_argument("--workers", type=int, default=8)
    p_download.add_argument("--max-pending-futures", type=int, default=128)
    p_download.add_argument("--min-free-gb", type=float, default=12.0)
    p_download.add_argument("--max-retries", type=int, default=2)
    p_download.add_argument("--force", action="store_true")

    p_all = sub.add_parser("all", help="Filter by CIK union and forms, then download.")
    p_all.add_argument("--input", type=str, default=str(DEFAULT_INPUT_MANIFEST))
    p_all.add_argument("--output", type=str, default=str(DEFAULT_FILTERED_MANIFEST))
    p_all.add_argument("--cik-map", type=str, default=str(DEFAULT_CIK_MAP))
    p_all.add_argument("--issuer-tickers", type=str, default=str(DEFAULT_ISSUER_TICKERS))
    p_all.add_argument("--start-year", type=int, default=2000)
    p_all.add_argument("--end-year", type=int, default=2024)
    p_all.add_argument("--overwrite", action="store_true")
    p_all.add_argument("--workers", type=int, default=8)
    p_all.add_argument("--max-pending-futures", type=int, default=128)
    p_all.add_argument("--min-free-gb", type=float, default=12.0)
    p_all.add_argument("--max-retries", type=int, default=8)
    p_all.add_argument("--force", action="store_true")

    return parser.parse_args()


def main() -> None:
    require_user_agent()
    args = parse_args()

    RAW_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if args.command == "filter-manifest":
        cik_union = load_cik_union([Path(args.cik_map), Path(args.issuer_tickers)])
        print(f"[cik-union] final unique CIKs={len(cik_union):,}", flush=True)

        filter_manifest_by_cik_and_form(
            input_manifest=Path(args.input),
            output_manifest=Path(args.output),
            cik_union=cik_union,
            start_year=args.start_year,
            end_year=args.end_year,
            overwrite=args.overwrite,
        )

    elif args.command == "download":
        download_manifest(
            manifest_path=Path(args.manifest),
            workers=args.workers,
            max_pending_futures=args.max_pending_futures,
            min_free_gb=args.min_free_gb,
            max_retries=args.max_retries,
            force=args.force,
        )

    elif args.command == "all":
        cik_union = load_cik_union([Path(args.cik_map), Path(args.issuer_tickers)])
        print(f"[cik-union] final unique CIKs={len(cik_union):,}", flush=True)

        out_manifest = Path(args.output)
        filter_manifest_by_cik_and_form(
            input_manifest=Path(args.input),
            output_manifest=out_manifest,
            cik_union=cik_union,
            start_year=args.start_year,
            end_year=args.end_year,
            overwrite=args.overwrite,
        )

        download_manifest(
            manifest_path=out_manifest,
            workers=args.workers,
            max_pending_futures=args.max_pending_futures,
            min_free_gb=args.min_free_gb,
            max_retries=args.max_retries,
            force=args.force,
        )

    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

# Filter only:
# python data/sec_filings_core_pipeline.py filter-manifest --input data/sec_edgar/processed/manifests/filings_manifest_2000_2024.csv  --output data/sec_edgar/processed/manifests/filings_manifest_core_companyscope_2000_2024.csv  --cik-map data/sec_edgar/processed/cleaned/cik_ticker_map_cleaned.csv --issuer-tickers data/sec_edgar/processed/cleaned/issuer_master_onlyTickers.csv --start-year 2000 --end-year 2024 --overwrite
#Download only:
# python data/sec_filings_core_pipeline.py download --manifest data/sec_edgar/processed/manifests/filings_manifest_core_companyscope_2000_2024.csv --workers 8   --max-pending-futures 256   --min-free-gb 12
#Or both:
# python data/sec_filings_core_pipeline.py all --input data/sec_edgar/processed/manifests/filings_manifest_2000_2024.csv  --output data/sec_edgar/processed/manifests/filings_manifest_core_companyscope_2000_2024.csv --cik-map data/sec_edgar/processed/cleaned/cik_ticker_map_cleaned.csv --issuer-tickers data/sec_edgar/processed/cleaned/issuer_master_onlyTickers.csv --start-year 2000 --end-year 2024 --overwrite --workers 8 --max-pending-futures 256 --min-free-gb 12
