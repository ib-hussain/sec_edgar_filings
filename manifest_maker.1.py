#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import date
import os
import re
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Iterator

import dotenv
import requests

dotenv.load_dotenv()


# PATHS / CONFIG
RAW_DIR = Path(os.getenv("FILING_DIR", "raw"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
INDEX_DIR = RAW_DIR / "indexes"
MANIFEST_DIR = Path(os.getenv("MANIFEST_DIR", "manifests"))

SEC_USER_AGENT = os.getenv(
    "SEC_USER_AGENT",
    "IbrahimHussain ibrahimbeaconarion@gmail.com",
).strip()
REQUEST_INTERVAL_SECONDS = float(os.getenv("SEC_REQUEST_INTERVAL_SECONDS", "0.15"))
TIMEOUT = (20, 180)

FORMS_EXACT = {
    "8-K",
    "10-K",
    "10-Q",
    "DEF 14A",
}

HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
}

MASTER_IDX_HEADER_MARKER = "-----"


def require_user_agent() -> None:
    if not SEC_USER_AGENT:
        raise SystemExit(
            "SEC_USER_AGENT is not set.\n"
            "Example:\n"
            'export SEC_USER_AGENT="Your Name your.email@example.com"'
        )


def ensure_dirs() -> None:
    for path in (RAW_DIR, LOG_DIR, INDEX_DIR, MANIFEST_DIR):
        path.mkdir(parents=True, exist_ok=True)


def log(message: str) -> None:
    print(f"LOGS:{message}", flush=True)


class SECClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request_time
        if elapsed < REQUEST_INTERVAL_SECONDS:
            time.sleep(REQUEST_INTERVAL_SECONDS - elapsed)

    def get(self, url: str, stream: bool = False) -> requests.Response:
        last_error: Exception | None = None

        for attempt in range(1, 6):
            try:
                self._throttle()
                response = self.session.get(
                    url,
                    headers=HEADERS,
                    timeout=TIMEOUT,
                    stream=stream,
                )
                self.last_request_time = time.monotonic()

                if response.status_code == 200:
                    return response

                if response.status_code in {403, 429, 500, 502, 503, 504}:
                    last_error = RuntimeError(f"HTTP {response.status_code} for {url}")
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait_seconds = float(retry_after) if retry_after else min(60, 2**attempt)
                    except ValueError:
                        wait_seconds = min(60, 2**attempt)
                    response.close()
                    log(
                        f"[retry] {url} -> {response.status_code}, "
                        f"sleeping {wait_seconds:g}s"
                    )
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                wait_seconds = min(60, 2**attempt)
                log(f"[retry-exc] {url} -> {exc}, sleeping {wait_seconds}s")
                time.sleep(wait_seconds)

        raise RuntimeError(f"Failed to fetch after retries: {url}\nLast error: {last_error}")

    def download_to_file(self, url: str, dest: Path, overwrite: bool = False) -> Path:
        if dest.exists() and not overwrite:
            if dest.is_file() and dest.stat().st_size > 0:
                return dest
            if dest.is_dir():
                raise IsADirectoryError(f"Expected file but found directory: {dest}")

        dest.parent.mkdir(parents=True, exist_ok=True)
        temporary = dest.with_suffix(dest.suffix + ".part-downloading")

        response = self.get(url, stream=True)
        try:
            with temporary.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            response.close()

        if temporary.stat().st_size == 0:
            temporary.unlink(missing_ok=True)
            raise IOError(f"Downloaded empty file from {url}")

        temporary.replace(dest)
        return dest


def padded_cik(cik: str | int) -> str:
    return str(cik).strip().zfill(10)


def sanitize_filename(value: str, max_len: int = 200) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return value[:max_len].strip("_") or "unknown"


def filing_form_wanted(form_type: str) -> bool:
    return form_type.strip().upper() in FORMS_EXACT


def quarter_iter(start_year: int, end_year: int) -> Iterator[tuple[int, int]]:
    today = date.today()
    current_year = today.year
    current_quarter = ((today.month - 1) // 3) + 1

    if start_year > end_year:
        raise ValueError("start_year must be <= end_year")
    if end_year > current_year:
        raise ValueError(
            f"end_year={end_year} is in the future; current year is {current_year}"
        )

    for year in range(start_year, end_year + 1):
        last_quarter = current_quarter if year == current_year else 4
        for quarter in range(1, last_quarter + 1):
            yield year, quarter


def master_idx_url(year: int, quarter: int) -> str:
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"


def master_idx_local_path(year: int, quarter: int) -> Path:
    return INDEX_DIR / str(year) / f"QTR{quarter}" / "master.idx"


def parse_master_idx(path: Path) -> Iterator[dict[str, str]]:
    lines = path.read_text(encoding="latin-1", errors="ignore").splitlines()
    start = None

    for index, line in enumerate(lines):
        if line.startswith(MASTER_IDX_HEADER_MARKER):
            start = index + 1
            break

    if start is None:
        raise ValueError(f"Could not find data header in {path}")

    for line in lines[start:]:
        if not line.strip():
            continue

        parts = line.split("|")
        if len(parts) != 5:
            continue

        cik, company_name, form_type, date_filed, filename = [part.strip() for part in parts]
        yield {
            "cik": cik,
            "company_name": company_name,
            "form_type": form_type,
            "date_filed": date_filed,
            "filename": filename,
            "filing_url": f"https://www.sec.gov/Archives/{filename}",
        }


def accession_from_filename(filename: str) -> str:
    return Path(filename).name.removesuffix(".txt")


def filing_output_path(row: dict[str, str]) -> str:
    """Return a portable logical path; downloader maps it to FILING_DIR."""
    year = row["date_filed"][:4]
    form_type = sanitize_filename(row["form_type"])
    cik = padded_cik(row["cik"])
    accession = accession_from_filename(row["filename"])
    company = sanitize_filename(row["company_name"])
    filename = f"{cik}__{row['date_filed']}__{accession}__{company}.txt"
    return PurePosixPath("raw", year, form_type, filename).as_posix()


def build_manifest(
    client: SECClient,
    start_year: int,
    end_year: int,
    overwrite: bool = False,
    refresh_indexes: bool = False,
) -> Path:
    manifest_path = MANIFEST_DIR / f"filings_manifest_{start_year}_{end_year}.csv"

    if manifest_path.exists() and not overwrite:
        log(f"[manifest] already exists: {manifest_path}")
        log("[manifest] use --overwrite to rebuild it safely")
        return manifest_path

    fieldnames = [
        "cik",
        "company_name",
        "form_type",
        "date_filed",
        "filename",
        "filing_url",
        "output_path",
    ]

    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    rows_written = 0

    log(f"[manifest] building manifest for {start_year}-{end_year} ...")
    log(f"[manifest] exact forms only: {', '.join(sorted(FORMS_EXACT))}")

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            dir=MANIFEST_DIR,
            prefix=f"{manifest_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            writer = csv.DictWriter(temporary, fieldnames=fieldnames)
            writer.writeheader()

            for year, quarter in quarter_iter(start_year, end_year):
                index_url = master_idx_url(year, quarter)
                index_path = master_idx_local_path(year, quarter)

                log(f"[manifest] index {year} QTR{quarter}")
                # Current-year quarterly indexes can have been cached while a quarter
                # was still in progress. Refresh the whole current year automatically
                # so a rebuild cannot silently retain an incomplete index.
                refresh_this_index = refresh_indexes or year == date.today().year
                client.download_to_file(
                    index_url,
                    index_path,
                    overwrite=refresh_this_index,
                )

                for row in parse_master_idx(index_path):
                    if not filing_form_wanted(row["form_type"]):
                        continue

                    writer.writerow({
                        **row,
                        "form_type": row["form_type"].strip().upper(),
                        "output_path": filing_output_path(row),
                    })
                    rows_written += 1

        if manifest_path.exists() and not overwrite:
            raise FileExistsError(
                f"Manifest appeared while rebuilding: {manifest_path}. "
                "Use --overwrite to replace it."
            )

        temporary_path.replace(manifest_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()

    log(f"[manifest] rows written: {rows_written:,}")
    log(f"[manifest] written to: {manifest_path}")
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an SEC EDGAR manifest containing only exact 8-K, 10-K, 10-Q, "
            "and DEF 14A filings."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser(
        "manifest",
        help="Build a filing manifest from quarterly EDGAR master indexes",
    )
    manifest.add_argument("--start-year", type=int, default=2000)
    manifest.add_argument("--end-year", type=int, default=date.today().year)
    manifest.add_argument(
        "--overwrite",
        action="store_true",
        help="Atomically replace an existing manifest after a successful rebuild.",
    )
    manifest.add_argument(
        "--refresh-indexes",
        action="store_true",
        help="Re-download cached master.idx files before building the manifest.",
    )

    return parser.parse_args()


def main() -> None:
    require_user_agent()
    ensure_dirs()
    args = parse_args()
    client = SECClient()

    if args.command == "manifest":
        build_manifest(
            client,
            args.start_year,
            args.end_year,
            overwrite=args.overwrite,
            refresh_indexes=args.refresh_indexes,
        )
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

# Example usage:
# python manifest_maker.1.py manifest --start-year 2000 --end-year 2026
# python manifest_maker.1.py manifest --start-year 2000 --end-year 2026 --overwrite --refresh-indexes
