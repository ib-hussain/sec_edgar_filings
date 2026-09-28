#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import dotenv
dotenv.load_dotenv()  # Load .env if present
import re
import time
from pathlib import Path
from typing import Iterable, Iterator

import requests

# DATA_ROOT = Path(os.getenv("secDataPathGlobal", ""data/sec_edgar")) 
DATA_ROOT = Path(".") 
RAW_DIR = DATA_ROOT / "raw"
LOG_DIR = DATA_ROOT / "logs"
INDEX_DIR = RAW_DIR / "indexes"
FILING_DIR = RAW_DIR 
MANIFEST_DIR = DATA_ROOT / "manifests"

SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "IbrahimHussain ibrahimbeaconarion@gmail.com").strip()
REQUEST_INTERVAL_SECONDS = 0.15  # < 10 req/s, conservative
TIMEOUT = (20, 180)

FORMS_EXACT = {
    "8-K", 
    "10-K",
    "10-Q", 
    "DEF 14A",
}
FORM_PREFIXES = ("424B",)

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
            'export SEC_USER_AGENT="IbrahimHussain ibrahimbeaconarion@gmail.com"'
        )

def ensure_dirs() -> None:
    for p in [RAW_DIR, LOG_DIR, INDEX_DIR, FILING_DIR, MANIFEST_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def log(msg: str) -> None:  
    msg = "LOGS:" +msg
    print(msg, flush=True)

class SECClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.last_request_time = 0.0
    def _throttle(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_INTERVAL_SECONDS:
            time.sleep(REQUEST_INTERVAL_SECONDS - elapsed)
    def get(self, url: str, stream: bool = False) -> requests.Response:
        last_err = None
        for attempt in range(1, 6):
            try:
                self._throttle()
                resp = self.session.get(url, headers=HEADERS, timeout=TIMEOUT, stream=stream)
                self.last_request_time = time.time()
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (403, 429, 500, 502, 503, 504):
                    wait = min(60, 2 ** attempt)
                    log(f"[retry] {url} -> {resp.status_code}, sleeping {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                wait = min(60, 2 ** attempt)
                log(f"[retry-exc] {url} -> {exc}, sleeping {wait}s")
                time.sleep(wait)
        raise RuntimeError(f"Failed to fetch after retries: {url}\nLast error: {last_err}")
    def download_to_file(self, url: str, dest: Path, overwrite: bool = False) -> Path:
        if dest.exists() and not overwrite:
            return dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part-downloading")
        resp = self.get(url, stream=True)
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)
        return dest


def padded_cik(cik: str | int) -> str:
    return str(cik).strip().zfill(10)
def sanitize_filename(value: str, max_len: int = 200) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return value[:max_len].strip("_") or "unknown"

def filing_form_wanted(form_type: str) -> bool:
    form_type = form_type.strip().upper()
    if form_type in FORMS_EXACT:
        return True
    return any(form_type.startswith(prefix) for prefix in FORM_PREFIXES)


def quarter_iter(start_year: int, end_year: int) -> Iterator[tuple[int, int]]:
    for year in range(start_year, end_year + 1):
        for qtr in range(1, 5):
            yield year, qtr


def master_idx_url(year: int, qtr: int) -> str:    
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"

def master_idx_local_path(year: int, qtr: int) -> Path:    
    return INDEX_DIR / str(year) / f"QTR{qtr}" / "master.idx"

def parse_master_idx(path: Path) -> Iterator[dict[str, str]]:
    lines = path.read_text(encoding="latin-1", errors="ignore").splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(MASTER_IDX_HEADER_MARKER):
            start = i + 1
            break
    if start is None:
        raise ValueError(f"Could not find data header in {path}")
    for line in lines[start:]:
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company_name, form_type, date_filed, filename = [p.strip() for p in parts]
        yield {
            "cik": cik,
            "company_name": company_name,
            "form_type": form_type,
            "date_filed": date_filed,
            "filename": filename,
            "filing_url": f"https://www.sec.gov/Archives/{filename}",
        }

def accession_from_filename(filename: str) -> str:
    stem = Path(filename).name
    return stem.replace(".txt", "")

def filing_output_path(row: dict[str, str]) -> Path:
    year = row["date_filed"][:4]
    form_type = sanitize_filename(row["form_type"])
    cik = padded_cik(row["cik"])
    accession = accession_from_filename(row["filename"])
    company = sanitize_filename(row["company_name"])
    return FILING_DIR / year / form_type / f"{cik}__{row['date_filed']}__{accession}__{company}.txt"

def save_csv_rows(path: Path, rows: Iterable[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def append_csv_row(path: Path, row: dict[str, str], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def build_manifest(client: SECClient, start_year: int, end_year: int) -> Path:
    manifest_path = MANIFEST_DIR / f"filings_manifest_{start_year}_{end_year}.csv"
    fieldnames = [
        "cik",
        "company_name",
        "form_type",
        "date_filed",
        "filename",
        "filing_url",
        "output_path",
    ]

    if manifest_path.exists():
        log(f"[manifest] already exists: {manifest_path}")
        return manifest_path

    log(f"[manifest] building manifest for {start_year}-{end_year} ...")

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for year, qtr in quarter_iter(start_year, end_year):
            idx_url = master_idx_url(year, qtr)
            idx_path = master_idx_local_path(year, qtr)

            log(f"[manifest] index {year} QTR{qtr}")
            client.download_to_file(idx_url, idx_path)

            for row in parse_master_idx(idx_path):
                if not filing_form_wanted(row["form_type"]):
                    continue

                out_path = filing_output_path(row)
                writer.writerow({
                    **row,
                    "output_path": str(out_path),
                })

    log(f"[manifest] written to: {manifest_path}")
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SEC EDGAR raw downloader: bulk metadata, manifest building, and raw filing text download."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    manifest = sub.add_parser("manifest", help="Build raw filing manifest from quarterly EDGAR master indexes")
    manifest.add_argument("--start-year", type=int, default=2000)
    manifest.add_argument("--end-year", type=int, default=2026)



    return parser.parse_args()


def main() -> None:
    require_user_agent()
    ensure_dirs()
    args = parse_args()
    client = SECClient()

    if args.command == "manifest":    
        build_manifest(client, args.start_year, args.end_year)
    else:       
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

# Example Usage:
# python manifest_maker.1.py manifest --start-year 2000 --end-year 2026
