#!/usr/bin/env python3
"""
sec_filings_text_pipeline_v2.py

Linux-targeted end-to-end text engineering pipeline for a downloaded SEC filings corpus.
Designed for storage-constrained, partially downloaded corpora and HDD-backed storage.

MODULES
A. inventory   -> Scan downloaded filings on disk and build raw inventory
B. clean       -> Parse metadata + extract/clean primary text + build full filtered corpus
C. sections    -> Extract section-level text
D. quality     -> Coverage + quality reports
E. datasets    -> Build full/balanced modeling manifests
F. finbert     -> Build FinBERT-ready chunk datasets

Run modules selectively:
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py inventory
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py clean --resume
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py sections --resume
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py quality
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py datasets
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py finbert
    nice -n -20 python -u data/sec_filings_text_pipeline_v2.py all --resume

Key design decisions:
- Minimal repeated I/O on HDD: parse each raw filing once in module B.
- Large text outputs are partitioned and gzip-compressed JSONL.
- Resume-friendly: completed parts are marked and never reprocessed.
- Company-universe filtering uses union of cleaned SEC universe CSVs.
- Balanced dataset is manifest-based (no text duplication).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import heapq
import html
import io
import json
import math
import os
import random
import re
import shutil
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

try:
    import orjson  # type: ignore
    HAS_ORJSON = True
except Exception:
    orjson = None
    HAS_ORJSON = False


# ============================================================
# GLOBAL PATHS / CONFIG
# ============================================================

dataPath = Path(os.getenv("dataPathGlobal", "data"))

RAW_FILINGS_DIR = dataPath / "sec_edgar" / "raw" / "filings_txt"
CLEANED_DIR = dataPath / "sec_edgar" / "processed" / "cleaned"

DEFAULT_CIK_MAP = CLEANED_DIR / "cik_ticker_map_cleaned.csv"
DEFAULT_ISSUER_TICKERS = CLEANED_DIR / "issuer_master_onlyTickers.csv"

OUT_ROOT = dataPath / "sec_edgar" / "processed" / "filings_text"
PARTS_DIR = OUT_ROOT / "parts"
TMP_DIR = OUT_ROOT / "_tmp"

INVENTORY_CSV = OUT_ROOT / "filings_downloaded_inventory.csv"
FULL_DOCS_MANIFEST = OUT_ROOT / "filings_filtered_full.csv"
FULL_SECTIONS_MANIFEST = OUT_ROOT / "filings_sections_full.csv"

YEAR_COVERAGE_CSV = OUT_ROOT / "filings_year_coverage.csv"
YEAR_FORM_COVERAGE_CSV = OUT_ROOT / "filings_year_form_coverage.csv"
YEAR_COMPANY_COVERAGE_CSV = OUT_ROOT / "filings_year_company_coverage.csv"
SECTION_COVERAGE_CSV = OUT_ROOT / "filings_section_coverage.csv"
QUALITY_SUMMARY_JSON = OUT_ROOT / "filings_quality_summary.json"

BALANCED_DOCS_MANIFEST = OUT_ROOT / "filings_filtered_balanced.csv"
BALANCED_SECTIONS_MANIFEST = OUT_ROOT / "filings_sections_balanced.csv"
BALANCE_SUMMARY_JSON = OUT_ROOT / "filings_balance_summary.json"

FINBERT_CHUNKS_ALL_CSV = OUT_ROOT / "filings_finbert_chunks_all.csv"
FINBERT_CHUNKS_BALANCED_CSV = OUT_ROOT / "filings_finbert_chunks_balanced.csv"
FINBERT_SUMMARY_JSON = OUT_ROOT / "filings_finbert_summary.json"

LOG_DIR = dataPath / "sec_edgar" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
ERRORS_CSV = OUT_ROOT / "filings_text_errors.csv"

ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}
ALLOWED_FORM_FOLDERS = {"10-K", "10-Q", "8-K", "DEF_14A"}

COMPRESSLEVEL = 1

INVENTORY_HEADER = [
    "rel_path",
    "abs_path",
    "year",
    "form_folder",
    "file_size_bytes",
    "file_name",
]

DOC_META_HEADER = [
    "doc_id",
    "rel_path",
    "part_id",
    "year",
    "form_type",
    "folder_form",
    "cik",
    "entity_name",
    "filing_date",
    "period_of_report",
    "acceptance_datetime",
    "accession",
    "primary_doc_type",
    "primary_doc_filename",
    "is_company_in_universe",
    "clean_text_chars",
    "clean_text_words",
    "section_hint_count",
    "status",
    "error_message",
]

SECTION_META_HEADER = [
    "section_id",
    "doc_id",
    "part_id",
    "year",
    "form_type",
    "cik",
    "filing_date",
    "accession",
    "section_name",
    "section_rank",
    "source_mode",
    "text_chars",
    "text_words",
]

ERRORS_HEADER = [
    "stage",
    "rel_path",
    "doc_id",
    "error_type",
    "error_message",
]

FORM_ORDER = ["10-K", "10-Q", "8-K", "DEF 14A"]

DEFAULT_FORM_WEIGHTS = {
    "10-K": 0.30,
    "10-Q": 0.30,
    "8-K": 0.30,
    "DEF 14A": 0.10,
}


# ============================================================
# HELPERS
# ============================================================

def json_dumps(obj: Any) -> bytes:
    if HAS_ORJSON:
        return orjson.dumps(obj)
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def human_bytes(n: int | float) -> str:
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} B"


def fmt_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.2f}h"


def now_ts() -> float:
    return time.time()


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def choose_worker_count(requested: int) -> int:
    if requested > 0:
        return requested
    cpu = os.cpu_count() or 4
    return max(1, min(8, cpu // 2 if cpu >= 4 else 1))


def normalize_cik(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return ""
    s = s.lstrip("0")
    return s if s else "0"


def detect_cik_column(fieldnames: list[str]) -> str:
    lowered = {x.lower(): x for x in fieldnames}
    for name in ("cik", "padded_cik", "sec_cik", "issuer_cik"):
        if name in lowered:
            return lowered[name]
    raise RuntimeError(f"Could not find CIK column in {fieldnames}")


def load_cik_union(csv_paths: list[Path]) -> set[str]:
    cik_set: set[str] = set()
    for path in csv_paths:
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            cik_col = detect_cik_column(fieldnames)
            n = 0
            for row in reader:
                cik = normalize_cik(row.get(cik_col, ""))
                if cik:
                    cik_set.add(cik)
                    n += 1
            print(f"[cik-union] loaded {n:,} rows from {path} | cumulative unique CIKs={len(cik_set):,}", flush=True)
    return cik_set


def ensure_dirs(overwrite: bool) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    if overwrite:
        # only remove stage outputs, not raw data
        targets = [
            INVENTORY_CSV, FULL_DOCS_MANIFEST, FULL_SECTIONS_MANIFEST,
            YEAR_COVERAGE_CSV, YEAR_FORM_COVERAGE_CSV, YEAR_COMPANY_COVERAGE_CSV,
            SECTION_COVERAGE_CSV, QUALITY_SUMMARY_JSON,
            BALANCED_DOCS_MANIFEST, BALANCED_SECTIONS_MANIFEST, BALANCE_SUMMARY_JSON,
            FINBERT_CHUNKS_ALL_CSV, FINBERT_CHUNKS_BALANCED_CSV, FINBERT_SUMMARY_JSON,
            ERRORS_CSV,
        ]
        for t in targets:
            if t.exists():
                t.unlink()
        if PARTS_DIR.exists():
            for p in PARTS_DIR.glob("*"):
                if p.is_file():
                    p.unlink()


def write_csv(path: Path, header: list[str], rows: Iterable[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)
    tmp.replace(path)


def append_csv_row(path: Path, header: list[str], row: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        w.writerow(row)


def greedy_partition(files: list[Path], shard_count: int) -> list[list[Path]]:
    shard_count = max(1, min(shard_count, len(files)))
    shards = [[] for _ in range(shard_count)]
    heap = [(0, i) for i in range(shard_count)]
    heapq.heapify(heap)
    sized = []
    for p in files:
        try:
            sz = p.stat().st_size
        except Exception:
            sz = 0
        sized.append((sz, p))
    sized.sort(key=lambda x: x[0], reverse=True)
    for sz, p in sized:
        total, idx = heapq.heappop(heap)
        shards[idx].append(p)
        heapq.heappush(heap, (total + sz, idx))
    return [s for s in shards if s]


def load_processed_relpaths(pattern: str, rel_field: str) -> set[str]:
    done: set[str] = set()
    for p in PARTS_DIR.glob(pattern):
        try:
            with p.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if rel_field not in (reader.fieldnames or []):
                    continue
                for row in reader:
                    relp = (row.get(rel_field) or "").replace("\\", "/")
                    if relp:
                        done.add(relp)
        except Exception:
            continue
    return done


def done_marker(base: Path) -> Path:
    return base.with_suffix(base.suffix + ".done")


def normalize_space(text: str) -> str:
    text = text.replace("\x00", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_tags(text: str) -> str:
    # remove scripts/styles first
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?is)<!--.*?-->", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return text


def decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def hash_id(*parts: str, n: int = 16) -> str:
    h = hashlib.blake2b(digest_size=16)
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
        h.update(b"\x1f")
    return h.hexdigest()[:n]


SEC_HEADER_PATTERNS = {
    "cik": [
        r"CENTRAL INDEX KEY:\s*([0-9]+)",
        r"CIK:\s*([0-9]+)",
    ],
    "entity_name": [
        r"COMPANY CONFORMED NAME:\s*(.+)",
        r"COMPANY:\s*(.+)",
        r"CONFORMED NAME:\s*(.+)",
    ],
    "form_type": [
        r"CONFORMED SUBMISSION TYPE:\s*([^\n<]+)",
        r"FORM TYPE:\s*([^\n<]+)",
    ],
    "filing_date": [
        r"FILED AS OF DATE:\s*([0-9]{8})",
        r"FILING DATE:\s*([0-9]{8})",
    ],
    "period_of_report": [
        r"CONFORMED PERIOD OF REPORT:\s*([0-9]{8})",
        r"PERIOD OF REPORT:\s*([0-9]{8})",
    ],
    "acceptance_datetime": [
        r"ACCEPTANCE-DATETIME:\s*([0-9]{14})",
    ],
    "accession": [
        r"ACCESSION NUMBER:\s*([0-9\-]+)",
    ],
}


def sec_header_search(text: str, key: str) -> str:
    for pat in SEC_HEADER_PATTERNS[key]:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def yyyymmdd_to_iso(s: str) -> str:
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def folder_form_to_canonical(folder_name: str) -> str:
    return folder_name.replace("_", " ")


def derive_year_form(rel_path: str) -> tuple[str, str]:
    parts = rel_path.replace("\\", "/").split("/")
    # expected: YEAR/FORM/file.txt OR maybe root/YEAR/FORM/file.txt already stripped in rel path
    if len(parts) >= 3:
        year = parts[0]
        folder_form = parts[1]
        return year, folder_form
    return "", ""


def find_document_blocks(text: str) -> list[dict[str, str]]:
    """
    Parse SEC SGML-style <DOCUMENT> blocks if present.
    """
    blocks: list[dict[str, str]] = []
    pattern = re.compile(r"(?is)<DOCUMENT>(.*?)</DOCUMENT>")
    for block in pattern.findall(text):
        doc_type = ""
        filename = ""
        description = ""
        m = re.search(r"(?im)^\s*<TYPE>\s*([^\n\r]+)", block)
        if m:
            doc_type = m.group(1).strip()
        m = re.search(r"(?im)^\s*<FILENAME>\s*([^\n\r]+)", block)
        if m:
            filename = m.group(1).strip()
        m = re.search(r"(?im)^\s*<DESCRIPTION>\s*([^\n\r]+)", block)
        if m:
            description = m.group(1).strip()
        # Body starts after SGML headers; if not clear, keep full block
        body = re.sub(r"(?im)^\s*<(TYPE|SEQUENCE|FILENAME|DESCRIPTION|TEXT)>\s*[^\n\r]*", "", block)
        blocks.append({
            "type": doc_type,
            "filename": filename,
            "description": description,
            "body": body,
        })
    return blocks


def choose_primary_document(full_text: str, declared_form: str) -> tuple[str, str, str]:
    """
    Returns: body, doc_type, doc_filename
    """
    blocks = find_document_blocks(full_text)
    if not blocks:
        return full_text, "", ""

    declared = declared_form.upper().strip()
    normalized_candidates = {
        declared,
        declared.replace("/A", ""),
        declared.replace(" ", ""),
        declared.replace(" ", "_"),
    }

    # best: exact or near-exact type match to declared form
    for b in blocks:
        t = b["type"].upper().strip()
        if t in normalized_candidates or t.replace(" ", "") in normalized_candidates:
            return b["body"], b["type"], b["filename"]

    # next: choose longest non-exhibit-like document
    good = []
    for b in blocks:
        t = b["type"].upper().strip()
        if t.startswith("EX-"):
            continue
        if t in {"GRAPHIC", "XML", "ZIP", "PDF"}:
            continue
        good.append(b)

    if good:
        best = max(good, key=lambda x: len(x["body"]))
        return best["body"], best["type"], best["filename"]

    best = max(blocks, key=lambda x: len(x["body"]))
    return best["body"], best["type"], best["filename"]


def clean_primary_text(body: str) -> str:
    text = html.unescape(body)
    text = strip_tags(text)
    # normalize SGML remnants
    text = re.sub(r"(?im)^\s*<[^>\n]+>\s*$", " ", text)
    text = re.sub(r"[\u00a0\u200b]+", " ", text)
    text = normalize_space(text)
    return text


def count_words(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"\b\w+\b", text))


def parse_raw_filing(raw: bytes, rel_path: str, universe_ciks: set[str]) -> tuple[dict[str, Any], str]:
    full_text = decode_bytes(raw)
    header_window = full_text[:250_000]

    year, folder_form = derive_year_form(rel_path)

    cik = normalize_cik(sec_header_search(header_window, "cik"))
    entity_name = sec_header_search(header_window, "entity_name")
    form_type = sec_header_search(header_window, "form_type") or folder_form_to_canonical(folder_form)
    filing_date = yyyymmdd_to_iso(sec_header_search(header_window, "filing_date"))
    period_of_report = yyyymmdd_to_iso(sec_header_search(header_window, "period_of_report"))
    acceptance_datetime = sec_header_search(header_window, "acceptance_datetime")
    accession = sec_header_search(header_window, "accession")

    primary_body, primary_doc_type, primary_doc_filename = choose_primary_document(full_text, form_type)
    clean_text = clean_primary_text(primary_body)

    doc_id = hash_id(rel_path, accession or "", cik or "", form_type or "")
    section_hint_count = len(re.findall(r"(?im)\bitem\s+[0-9]{1,2}[a-z]?(?:\.[0-9]{2})?\b", clean_text))

    meta = {
        "doc_id": doc_id,
        "rel_path": rel_path,
        "year": year,
        "form_type": form_type,
        "folder_form": folder_form,
        "cik": cik,
        "entity_name": entity_name,
        "filing_date": filing_date,
        "period_of_report": period_of_report,
        "acceptance_datetime": acceptance_datetime,
        "accession": accession,
        "primary_doc_type": primary_doc_type,
        "primary_doc_filename": primary_doc_filename,
        "is_company_in_universe": 1 if cik and cik in universe_ciks else 0,
        "clean_text_chars": len(clean_text),
        "clean_text_words": count_words(clean_text),
        "section_hint_count": section_hint_count,
        "status": "ok",
        "error_message": "",
    }
    return meta, clean_text


def extract_sections_for_doc(doc_meta: dict[str, Any], clean_text: str) -> list[dict[str, Any]]:
    form = (doc_meta.get("form_type") or "").upper().strip()
    text = clean_text

    sections: list[dict[str, Any]] = []

    def slice_items(item_patterns: list[tuple[str, list[str]]], text_src: str) -> None:
        nonlocal sections
        markers = []
        for idx, (name, patterns) in enumerate(item_patterns):
            for pat in patterns:
                for m in re.finditer(pat, text_src, flags=re.IGNORECASE | re.MULTILINE):
                    markers.append((m.start(), name))
                    break
        markers = sorted(set(markers), key=lambda x: x[0])
        for i, (start, name) in enumerate(markers):
            end = markers[i + 1][0] if i + 1 < len(markers) else len(text_src)
            chunk = text_src[start:end].strip()
            if len(chunk) >= 300:
                sections.append({
                    "section_name": name,
                    "section_rank": i + 1,
                    "source_mode": "regex_item",
                    "text": chunk,
                })

    if form == "10-K":
        patterns = [
            ("business", [r"(?im)^\s*item\s+1[\.\-:\s]+business\b"]),
            ("risk_factors", [r"(?im)^\s*item\s+1A[\.\-:\s]+risk\s+factors\b"]),
            ("mda", [r"(?im)^\s*item\s+7[\.\-:\s]+management'?s?\s+discussion", r"(?im)^\s*item\s+7[\.\-:\s]+md&a"]),
        ]
        slice_items(patterns, text)

    elif form == "10-Q":
        patterns = [
            ("financial_statements", [r"(?im)^\s*item\s+1[\.\-:\s]+financial\s+statements\b"]),
            ("mda", [r"(?im)^\s*item\s+2[\.\-:\s]+management'?s?\s+discussion", r"(?im)^\s*item\s+2[\.\-:\s]+md&a"]),
            ("risk_factors", [r"(?im)^\s*item\s+1A[\.\-:\s]+risk\s+factors\b"]),
        ]
        slice_items(patterns, text)

    elif form == "8-K":
        # extract item-based segments
        markers = []
        for m in re.finditer(r"(?im)^\s*item\s+([0-9]{1,2}\.[0-9]{2})\b", text):
            item_no = m.group(1)
            markers.append((m.start(), f"item_{item_no.replace('.', '_')}"))
        markers = sorted(markers, key=lambda x: x[0])
        for i, (start, name) in enumerate(markers):
            end = markers[i + 1][0] if i + 1 < len(markers) else len(text)
            chunk = text[start:end].strip()
            if len(chunk) >= 200:
                sections.append({
                    "section_name": name,
                    "section_rank": i + 1,
                    "source_mode": "regex_item",
                    "text": chunk,
                })

    elif form == "DEF 14A":
        heading_patterns = [
            ("executive_compensation", [r"(?im)^\s*executive\s+compensation\b", r"(?im)^\s*compensation\s+discussion"]),
            ("corporate_governance", [r"(?im)^\s*corporate\s+governance\b", r"(?im)^\s*board\s+of\s+directors\b"]),
            ("security_ownership", [r"(?im)^\s*security\s+ownership\b", r"(?im)^\s*beneficial\s+ownership\b"]),
        ]
        slice_items(heading_patterns, text)

    # fallback if no sections found: split by paragraphs and keep the first large chunk
    if not sections:
        paras = [p.strip() for p in re.split(r"\n{2,}", text) if len(p.strip()) >= 250]
        if paras:
            joined = "\n\n".join(paras[:20])
            sections.append({
                "section_name": "full_document_fallback",
                "section_rank": 1,
                "source_mode": "fallback",
                "text": joined,
            })

    return sections


def merge_csv_parts(glob_pattern: str, out_path: Path, header: list[str]) -> int:
    parts = sorted(PARTS_DIR.glob(glob_pattern))
    if not parts:
        return 0
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    count = 0
    with tmp.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(header)
        for i, p in enumerate(parts):
            with p.open("r", newline="", encoding="utf-8") as in_f:
                reader = csv.reader(in_f)
                # skip header
                try:
                    next(reader)
                except StopIteration:
                    continue
                for row in reader:
                    writer.writerow(row)
                    count += 1
    tmp.replace(out_path)
    return count


def build_inventory_rows(input_root: Path) -> list[list[Any]]:
    rows = []
    for p in sorted(input_root.rglob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".txt", ".html", ".htm"}:
            continue
        rel = str(p.relative_to(input_root)).replace("\\", "/")
        year, form_folder = derive_year_form(rel)
        if form_folder not in ALLOWED_FORM_FOLDERS:
            continue
        rows.append([
            rel,
            str(p),
            year,
            form_folder,
            p.stat().st_size,
            p.name,
        ])
    return rows


# ============================================================
# MODULE A — INVENTORY
# ============================================================

def run_inventory(args: argparse.Namespace) -> None:
    start = now_ts()
    rows = build_inventory_rows(Path(args.input_dir))
    write_csv(INVENTORY_CSV, INVENTORY_HEADER, rows)
    elapsed = now_ts() - start
    print(f"[inventory] rows={len(rows):,} | output={INVENTORY_CSV} | elapsed={fmt_elapsed(elapsed)}", flush=True)


# ============================================================
# MODULE B — CLEAN / PARSE / FILTER / FULL CORPUS
# ============================================================

def process_clean_shard(
    shard_id: int,
    files: list[str],
    input_root_str: str,
    universe_ciks: list[str],
    min_free_bytes: int,
) -> dict[str, Any]:
    input_root = Path(input_root_str)
    universe = set(universe_ciks)

    meta_tmp = PARTS_DIR / f"docs_meta_part_{shard_id:05d}.csv.part"
    text_tmp = PARTS_DIR / f"docs_text_part_{shard_id:05d}.jsonl.gz.part"
    err_tmp = PARTS_DIR / f"docs_errors_part_{shard_id:05d}.csv.part"

    meta_final = PARTS_DIR / f"docs_meta_part_{shard_id:05d}.csv"
    text_final = PARTS_DIR / f"docs_text_part_{shard_id:05d}.jsonl.gz"
    err_final = PARTS_DIR / f"docs_errors_part_{shard_id:05d}.csv"

    # skip if already done
    if meta_final.exists() and text_final.exists() and done_marker(meta_final).exists():
        return {
            "shard_id": shard_id,
            "input_files": 0,
            "ok_files": 0,
            "failed_files": 0,
            "text_records": 0,
            "reused": True,
        }

    ok_files = 0
    failed_files = 0
    text_records = 0

    with meta_tmp.open("w", newline="", encoding="utf-8") as meta_f, \
         gzip.open(text_tmp, "wb", compresslevel=COMPRESSLEVEL) as text_f, \
         err_tmp.open("w", newline="", encoding="utf-8") as err_f:

        meta_w = csv.writer(meta_f)
        err_w = csv.writer(err_f)
        meta_w.writerow(DOC_META_HEADER)
        err_w.writerow(ERRORS_HEADER)

        for idx, file_str in enumerate(files, start=1):
            if idx % 64 == 0:
                if free_bytes(PARTS_DIR) < min_free_bytes:
                    raise RuntimeError("LowDiskSpaceError: free disk dropped below reserve during clean stage")

            path = Path(file_str)
            rel = str(path.relative_to(input_root)).replace("\\", "/")

            try:
                raw = path.read_bytes()
                meta, clean_text = parse_raw_filing(raw, rel, universe)

                # Only keep allowed form canonical names and target universe
                canonical_form = (meta["form_type"] or "").upper().replace("_", " ")
                if canonical_form not in ALLOWED_FORMS:
                    # If SEC header says something odd but folder is target, fall back to folder form.
                    folder_form = folder_form_to_canonical(meta["folder_form"]).upper()
                    if folder_form in ALLOWED_FORMS:
                        meta["form_type"] = folder_form
                    else:
                        meta["status"] = "skipped_non_target_form"

                meta["part_id"] = f"{shard_id:05d}"

                meta_w.writerow([
                    meta["doc_id"], meta["rel_path"], meta["part_id"], meta["year"],
                    meta["form_type"], meta["folder_form"], meta["cik"], meta["entity_name"],
                    meta["filing_date"], meta["period_of_report"], meta["acceptance_datetime"],
                    meta["accession"], meta["primary_doc_type"], meta["primary_doc_filename"],
                    meta["is_company_in_universe"], meta["clean_text_chars"], meta["clean_text_words"],
                    meta["section_hint_count"], meta["status"], meta["error_message"],
                ])

                if meta["is_company_in_universe"] == 1 and meta["status"] == "ok" and clean_text:
                    rec = {
                        "doc_id": meta["doc_id"],
                        "rel_path": meta["rel_path"],
                        "part_id": meta["part_id"],
                        "year": meta["year"],
                        "form_type": meta["form_type"],
                        "cik": meta["cik"],
                        "entity_name": meta["entity_name"],
                        "filing_date": meta["filing_date"],
                        "period_of_report": meta["period_of_report"],
                        "acceptance_datetime": meta["acceptance_datetime"],
                        "accession": meta["accession"],
                        "primary_doc_type": meta["primary_doc_type"],
                        "primary_doc_filename": meta["primary_doc_filename"],
                        "clean_text": clean_text,
                    }
                    text_f.write(json_dumps(rec) + b"\n")
                    text_records += 1
                    ok_files += 1
                else:
                    ok_files += 1

            except Exception as exc:
                failed_files += 1
                doc_id = hash_id(rel)
                err_w.writerow([
                    "clean",
                    rel,
                    doc_id,
                    type(exc).__name__,
                    str(exc),
                ])
                meta_w.writerow([
                    doc_id, rel, f"{shard_id:05d}", "", "", "", "", "", "", "", "", "", "", "",
                    0, 0, 0, 0, "error", str(exc),
                ])

    meta_tmp.replace(meta_final)
    text_tmp.replace(text_final)
    err_tmp.replace(err_final)
    done_marker(meta_final).write_text("done", encoding="utf-8")

    return {
        "shard_id": shard_id,
        "input_files": len(files),
        "ok_files": ok_files,
        "failed_files": failed_files,
        "text_records": text_records,
        "reused": False,
    }


def run_clean(args: argparse.Namespace) -> None:
    start = now_ts()
    input_root = Path(args.input_dir)
    workers = choose_worker_count(args.workers)
    min_free_bytes = int(args.min_free_gb * (1024 ** 3))

    universe = load_cik_union([Path(args.cik_map), Path(args.issuer_tickers)])
    inv_rows = []
    if INVENTORY_CSV.exists():
        with INVENTORY_CSV.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            inv_rows = [row for row in reader]
    else:
        rows = build_inventory_rows(input_root)
        write_csv(INVENTORY_CSV, INVENTORY_HEADER, rows)
        inv_rows = [
            dict(zip(INVENTORY_HEADER, map(str, row)))
            for row in rows
        ]

    all_files = [Path(r["abs_path"]) for r in inv_rows]

    if args.resume:
        processed = load_processed_relpaths("docs_meta_part_*.csv", "rel_path")
        files = [p for p in all_files if str(p.relative_to(input_root)).replace("\\", "/") not in processed]
    else:
        files = all_files

    if not files:
        print("[clean] nothing to do", flush=True)
        return

    shard_count = max(1, min(len(files), workers * max(1, args.shard_multiplier)))
    shards = greedy_partition(files, shard_count)

    print(f"[clean] files={len(files):,} workers={workers} shards={len(shards)} free_disk={human_bytes(free_bytes(OUT_ROOT))}", flush=True)

    total_done = 0
    total_ok = 0
    total_fail = 0
    total_records = 0

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = []
        for i, shard_files in enumerate(shards, start=1):
            futures.append(ex.submit(
                process_clean_shard,
                i,
                [str(p) for p in shard_files],
                str(input_root),
                list(universe),
                min_free_bytes,
            ))

        completed = 0
        for fut in as_completed(futures):
            res = fut.result()
            completed += 1
            total_done += res["input_files"]
            total_ok += res["ok_files"]
            total_fail += res["failed_files"]
            total_records += res["text_records"]
            elapsed = now_ts() - start
            rate = total_done / max(elapsed, 1e-9)
            print(
                f"[clean] shards {completed}/{len(shards)} | files {total_done:,}/{len(files):,} | "
                f"ok {total_ok:,} | fail {total_fail:,} | docs {total_records:,} | rate {rate:,.1f} files/s",
                flush=True
            )

    # Merge metadata and errors; text stays partitioned
    merged_meta = merge_csv_parts("docs_meta_part_*.csv", FULL_DOCS_MANIFEST, DOC_META_HEADER)
    merged_err = merge_csv_parts("docs_errors_part_*.csv", ERRORS_CSV, ERRORS_HEADER)

    elapsed = now_ts() - start
    print(
        f"[clean] done | manifest_rows={merged_meta:,} | errors={merged_err:,} | "
        f"text_parts={len(list(PARTS_DIR.glob('docs_text_part_*.jsonl.gz'))):,} | elapsed={fmt_elapsed(elapsed)}",
        flush=True
    )


# ============================================================
# MODULE C — SECTIONS
# ============================================================


def iter_doc_text_records() -> Iterable[dict[str, Any]]:
    for p in sorted(PARTS_DIR.glob("docs_text_part_*.jsonl.gz")):
        with gzip.open(p, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                yield json.loads(line)


def iter_doc_text_part_paths() -> list[Path]:
    return sorted(PARTS_DIR.glob("docs_text_part_*.jsonl.gz"))




def process_section_part(
    shard_id: int,
    text_part_path_str: str,
    min_free_bytes: int,
) -> dict[str, Any]:
    text_part_path = Path(text_part_path_str)

    meta_tmp = PARTS_DIR / f"sections_meta_part_{shard_id:05d}.csv.part"
    text_tmp = PARTS_DIR / f"sections_text_part_{shard_id:05d}.jsonl.gz.part"
    err_tmp = PARTS_DIR / f"sections_errors_part_{shard_id:05d}.csv.part"

    meta_final = PARTS_DIR / f"sections_meta_part_{shard_id:05d}.csv"
    text_final = PARTS_DIR / f"sections_text_part_{shard_id:05d}.jsonl.gz"
    err_final = PARTS_DIR / f"sections_errors_part_{shard_id:05d}.csv"

    if meta_final.exists() and text_final.exists() and done_marker(meta_final).exists():
        return {
            "shard_id": shard_id,
            "input_docs": 0,
            "section_rows": 0,
            "failed_docs": 0,
            "reused": True,
        }

    input_docs = 0
    section_rows = 0
    failed_docs = 0

    with meta_tmp.open("w", newline="", encoding="utf-8") as meta_f,          gzip.open(text_tmp, "wb", compresslevel=COMPRESSLEVEL) as text_f,          err_tmp.open("w", newline="", encoding="utf-8") as err_f,          gzip.open(text_part_path, "rb") as in_f:

        meta_w = csv.writer(meta_f)
        err_w = csv.writer(err_f)
        meta_w.writerow(SECTION_META_HEADER)
        err_w.writerow(ERRORS_HEADER)

        for idx, line in enumerate(in_f, start=1):
            if not line.strip():
                continue
            if idx % 64 == 0 and free_bytes(PARTS_DIR) < min_free_bytes:
                raise RuntimeError("LowDiskSpaceError: free disk dropped below reserve during sections stage")

            rec = json.loads(line)
            input_docs += 1
            try:
                doc_meta = {
                    "doc_id": rec["doc_id"],
                    "part_id": rec["part_id"],
                    "year": rec["year"],
                    "form_type": rec["form_type"],
                    "cik": rec["cik"],
                    "filing_date": rec["filing_date"],
                    "accession": rec["accession"],
                }
                sections = extract_sections_for_doc(doc_meta, rec["clean_text"])

                for s in sections:
                    sid = hash_id(rec["doc_id"], s["section_name"], str(s["section_rank"]))
                    text_chars = len(s["text"])
                    text_words = count_words(s["text"])
                    meta_w.writerow([
                        sid,
                        rec["doc_id"],
                        f"{shard_id:05d}",
                        rec["year"],
                        rec["form_type"],
                        rec["cik"],
                        rec["filing_date"],
                        rec["accession"],
                        s["section_name"],
                        s["section_rank"],
                        s["source_mode"],
                        text_chars,
                        text_words,
                    ])
                    text_f.write(json_dumps({
                        "section_id": sid,
                        "doc_id": rec["doc_id"],
                        "part_id": f"{shard_id:05d}",
                        "year": rec["year"],
                        "form_type": rec["form_type"],
                        "cik": rec["cik"],
                        "filing_date": rec["filing_date"],
                        "accession": rec["accession"],
                        "section_name": s["section_name"],
                        "section_rank": s["section_rank"],
                        "source_mode": s["source_mode"],
                        "text": s["text"],
                    }) + b"\n")
                    section_rows += 1

            except Exception as exc:
                failed_docs += 1
                err_w.writerow([
                    "sections",
                    rec.get("rel_path", ""),
                    rec.get("doc_id", ""),
                    type(exc).__name__,
                    str(exc),
                ])

    meta_tmp.replace(meta_final)
    text_tmp.replace(text_final)
    err_tmp.replace(err_final)
    done_marker(meta_final).write_text("done", encoding="utf-8")

    return {
        "shard_id": shard_id,
        "input_docs": input_docs,
        "section_rows": section_rows,
        "failed_docs": failed_docs,
        "reused": False,
    }




def run_sections(args: argparse.Namespace) -> None:
    start = now_ts()
    workers = choose_worker_count(args.workers)
    min_free_bytes = int(args.min_free_gb * (1024 ** 3))

    part_paths = iter_doc_text_part_paths()
    if args.resume:
        pending = []
        for i, p in enumerate(part_paths, start=1):
            meta_final = PARTS_DIR / f"sections_meta_part_{i:05d}.csv"
            text_final = PARTS_DIR / f"sections_text_part_{i:05d}.jsonl.gz"
            if meta_final.exists() and text_final.exists() and done_marker(meta_final).exists():
                continue
            pending.append((i, p))
    else:
        pending = [(i, p) for i, p in enumerate(part_paths, start=1)]

    if not pending:
        print("[sections] nothing to do", flush=True)
        return

    total_docs = 0
    total_sections = 0
    total_fail = 0

    print(f"[sections] parts={len(pending):,} workers={workers}", flush=True)

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(process_section_part, shard_id, str(p), min_free_bytes) for shard_id, p in pending]

        completed = 0
        for fut in as_completed(futures):
            res = fut.result()
            completed += 1
            total_docs += res["input_docs"]
            total_sections += res["section_rows"]
            total_fail += res["failed_docs"]
            elapsed = now_ts() - start
            rate = total_docs / max(elapsed, 1e-9)
            print(
                f"[sections] parts {completed}/{len(pending)} | docs {total_docs:,} | "
                f"sections {total_sections:,} | fail_docs {total_fail:,} | rate {rate:,.1f} docs/s",
                flush=True
            )

    merged_meta = merge_csv_parts("sections_meta_part_*.csv", FULL_SECTIONS_MANIFEST, SECTION_META_HEADER)
    merge_csv_parts("sections_errors_part_*.csv", ERRORS_CSV, ERRORS_HEADER)

    elapsed = now_ts() - start
    print(
        f"[sections] done | section_rows={merged_meta:,} | text_parts={len(list(PARTS_DIR.glob('sections_text_part_*.jsonl.gz'))):,} "
        f"| elapsed={fmt_elapsed(elapsed)}",
        flush=True
    )


# ============================================================
# MODULE D — QUALITY / COVERAGE REPORTS
# ============================================================

def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))



def run_quality(args: argparse.Namespace) -> None:
    start = now_ts()

    by_year = Counter()
    by_year_form = Counter()
    by_year_company = Counter()
    by_section = Counter()
    by_doc_section_presence = Counter()
    duplicate_accessions = Counter()
    length_stats = defaultdict(list)
    kept_doc_ids = set()
    docs_total = 0
    docs_kept_count = 0

    if not FULL_DOCS_MANIFEST.exists():
        raise RuntimeError("No full docs manifest rows found. Run clean first.")

    with FULL_DOCS_MANIFEST.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for d in reader:
            docs_total += 1
            if d.get("status") != "ok" or d.get("is_company_in_universe") != "1":
                continue
            docs_kept_count += 1
            kept_doc_ids.add(d["doc_id"])
            year = d["year"]
            form = d["form_type"]
            cik = d["cik"]
            by_year[year] += 1
            by_year_form[(year, form)] += 1
            by_year_company[(year, cik)] += 1
            if d.get("accession"):
                duplicate_accessions[(d["cik"], d["accession"])] += 1
            try:
                length_stats[form].append(int(d["clean_text_words"] or 0))
            except Exception:
                pass

    if not kept_doc_ids:
        raise RuntimeError("No full docs manifest rows found. Run clean first.")

    sections_total = 0
    sections_by_doc = defaultdict(set)
    if FULL_SECTIONS_MANIFEST.exists():
        with FULL_SECTIONS_MANIFEST.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for s in reader:
                if s.get("doc_id") not in kept_doc_ids:
                    continue
                sections_total += 1
                did = s["doc_id"]
                sec_name = s["section_name"]
                sections_by_doc[did].add(sec_name)
                by_section[(s["year"], s["form_type"], sec_name)] += 1

    for did, secs in sections_by_doc.items():
        by_doc_section_presence[len(secs)] += 1

    write_csv(
        YEAR_COVERAGE_CSV,
        ["year", "doc_count"],
        [[y, c] for y, c in sorted(by_year.items())]
    )
    write_csv(
        YEAR_FORM_COVERAGE_CSV,
        ["year", "form_type", "doc_count"],
        [[y, f, c] for (y, f), c in sorted(by_year_form.items())]
    )
    write_csv(
        YEAR_COMPANY_COVERAGE_CSV,
        ["year", "cik", "doc_count"],
        [[y, cik, c] for (y, cik), c in sorted(by_year_company.items())]
    )
    write_csv(
        SECTION_COVERAGE_CSV,
        ["year", "form_type", "section_name", "section_count"],
        [[y, f, s, c] for (y, f, s), c in sorted(by_section.items())]
    )

    summary = {
        "docs_total": docs_total,
        "docs_kept_full_filtered": docs_kept_count,
        "sections_total": sections_total,
        "years_covered": sorted(by_year.keys()),
        "duplicate_accession_pairs_gt1": sum(1 for _, v in duplicate_accessions.items() if v > 1),
        "forms": {
            form: {
                "docs": sum(1 for _ in length_stats[form]),
                "avg_words": (sum(length_stats[form]) / len(length_stats[form])) if length_stats[form] else 0.0,
                "median_words_approx": sorted(length_stats[form])[len(length_stats[form]) // 2] if length_stats[form] else 0,
            }
            for form in FORM_ORDER
        },
        "year_counts": dict(sorted(by_year.items())),
        "section_presence_by_num_sections_per_doc": dict(sorted(by_doc_section_presence.items())),
    }

    with QUALITY_SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    elapsed = now_ts() - start
    print(f"[quality] done | docs={docs_kept_count:,} | sections={sections_total:,} | elapsed={fmt_elapsed(elapsed)}", flush=True)


# ============================================================
# MODULE E — FULL + BALANCED DATASETS
# ============================================================

def weighted_form_allocation(target_n: int, available_by_form: dict[str, int], weights: dict[str, float]) -> dict[str, int]:
    quotas = {f: 0 for f in FORM_ORDER}
    remaining = target_n

    # initial weighted allocation
    for f in FORM_ORDER:
        want = int(round(target_n * weights.get(f, 0.0)))
        take = min(want, available_by_form.get(f, 0))
        quotas[f] = take
        remaining -= take

    if remaining <= 0:
        return quotas

    # fill remainder from forms with spare capacity, prioritized by weight then availability
    forms = sorted(FORM_ORDER, key=lambda x: (weights.get(x, 0.0), available_by_form.get(x, 0)), reverse=True)
    while remaining > 0:
        progressed = False
        for f in forms:
            if quotas[f] < available_by_form.get(f, 0):
                quotas[f] += 1
                remaining -= 1
                progressed = True
                if remaining <= 0:
                    break
        if not progressed:
            break
    return quotas


def run_datasets(args: argparse.Namespace) -> None:
    start = now_ts()
    rng = random.Random(args.seed)

    docs = read_csv_dicts(FULL_DOCS_MANIFEST)
    sections = read_csv_dicts(FULL_SECTIONS_MANIFEST)

    docs_kept = [d for d in docs if d.get("status") == "ok" and d.get("is_company_in_universe") == "1"]
    if not docs_kept:
        raise RuntimeError("No full docs manifest rows found. Run clean first.")

    # full dataset manifests (already filtered by universe)
    write_csv(FULL_DOCS_MANIFEST, DOC_META_HEADER, [
        [d.get(col, "") for col in DOC_META_HEADER] for d in docs_kept
    ])

    sections_kept = [s for s in sections if s.get("doc_id") in {d["doc_id"] for d in docs_kept}]
    write_csv(FULL_SECTIONS_MANIFEST, SECTION_META_HEADER, [
        [s.get(col, "") for col in SECTION_META_HEADER] for s in sections_kept
    ])

    by_year = Counter(d["year"] for d in docs_kept if d.get("year"))
    usable_years = [y for y, c in sorted(by_year.items()) if c >= args.min_docs_per_year]
    if not usable_years:
        raise RuntimeError("No usable years satisfy min_docs_per_year; lower the threshold or inspect coverage.")

    weakest_usable_year = min(usable_years, key=lambda y: by_year[y])
    target_per_year = by_year[weakest_usable_year]

    # optional override
    if args.target_per_year > 0:
        target_per_year = min(target_per_year, args.target_per_year)

    weights = DEFAULT_FORM_WEIGHTS.copy()
    if args.form_weights:
        for kv in args.form_weights.split(","):
            if "=" not in kv:
                continue
            k, v = kv.split("=", 1)
            k = k.strip().upper().replace("_", " ")
            if k in weights:
                weights[k] = float(v)

    # normalize weights
    total_w = sum(weights.values()) or 1.0
    weights = {k: v / total_w for k, v in weights.items()}

    docs_by_year_form: dict[str, dict[str, list[dict[str, str]]]] = defaultdict(lambda: defaultdict(list))
    for d in docs_kept:
        if d["year"] in usable_years:
            docs_by_year_form[d["year"]][d["form_type"]].append(d)

    sampled_doc_ids: set[str] = set()
    balanced_rows: list[dict[str, str]] = []
    sampling_audit: list[dict[str, Any]] = []

    for year in sorted(usable_years):
        available_by_form = {f: len(docs_by_year_form[year].get(f, [])) for f in FORM_ORDER}
        quotas = weighted_form_allocation(target_per_year, available_by_form, weights)

        for form in FORM_ORDER:
            candidates = docs_by_year_form[year].get(form, [])
            # optional company cap
            if args.max_docs_per_cik_per_year_form > 0:
                by_cik = defaultdict(list)
                for d in candidates:
                    by_cik[d["cik"]].append(d)
                capped = []
                for cik, rows in by_cik.items():
                    rows = rows[:]
                    rng.shuffle(rows)
                    capped.extend(rows[:args.max_docs_per_cik_per_year_form])
                candidates = capped

            rng.shuffle(candidates)
            chosen = candidates[:quotas.get(form, 0)]
            for d in chosen:
                sampled_doc_ids.add(d["doc_id"])
                balanced_rows.append(d)

        sampling_audit.append({
            "year": year,
            "available_total": by_year[year],
            "target_total": target_per_year,
            "available_by_form": available_by_form,
            "quotas": quotas,
        })

    balanced_sections = [s for s in sections_kept if s["doc_id"] in sampled_doc_ids]

    write_csv(BALANCED_DOCS_MANIFEST, DOC_META_HEADER, [
        [d.get(col, "") for col in DOC_META_HEADER] for d in balanced_rows
    ])
    write_csv(BALANCED_SECTIONS_MANIFEST, SECTION_META_HEADER, [
        [s.get(col, "") for col in SECTION_META_HEADER] for s in balanced_sections
    ])

    with BALANCE_SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump({
            "seed": args.seed,
            "min_docs_per_year": args.min_docs_per_year,
            "usable_years": usable_years,
            "weakest_usable_year": weakest_usable_year,
            "target_per_year": target_per_year,
            "form_weights": weights,
            "balanced_doc_count": len(balanced_rows),
            "balanced_section_count": len(balanced_sections),
            "sampling_audit": sampling_audit,
        }, f, indent=2)

    elapsed = now_ts() - start
    print(
        f"[datasets] done | usable_years={len(usable_years):,} | target_per_year={target_per_year:,} | "
        f"balanced_docs={len(balanced_rows):,} | balanced_sections={len(balanced_sections):,} | elapsed={fmt_elapsed(elapsed)}",
        flush=True
    )


# ============================================================
# MODULE F — FINBERT-READY CHUNKS
# ============================================================

SECTION_PRIORITY = {
    "risk_factors": 1,
    "mda": 2,
    "business": 3,
    "financial_statements": 4,
    "executive_compensation": 5,
    "corporate_governance": 6,
    "security_ownership": 7,
    "full_document_fallback": 99,
}



def iter_section_text_records() -> Iterable[dict[str, Any]]:
    for p in sorted(PARTS_DIR.glob("sections_text_part_*.jsonl.gz")):
        with gzip.open(p, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                yield json.loads(line)


def chunk_words(text: str, chunk_size: int, overlap: int) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks = []
    i = 0
    step = max(1, chunk_size - overlap)
    while i < len(words):
        chunk = words[i:i + chunk_size]
        if len(chunk) >= max(50, chunk_size // 3):
            chunks.append(" ".join(chunk))
        i += step
    return chunks



def load_doc_text_map() -> dict[str, dict[str, Any]]:
    m = {}
    for p in sorted(PARTS_DIR.glob("docs_text_part_*.jsonl.gz")):
        with gzip.open(p, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                m[rec["doc_id"]] = rec
    return m


def chunk_words(text: str, chunk_size: int, overlap: int) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks = []
    i = 0
    step = max(1, chunk_size - overlap)
    while i < len(words):
        chunk = words[i:i + chunk_size]
        if len(chunk) >= max(50, chunk_size // 3):
            chunks.append(" ".join(chunk))
        i += step
    return chunks



def run_finbert(args: argparse.Namespace) -> None:
    start = now_ts()
    full_docs = read_csv_dicts(FULL_DOCS_MANIFEST)
    balanced_docs = read_csv_dicts(BALANCED_DOCS_MANIFEST)

    full_doc_ids = {d["doc_id"] for d in full_docs}
    balanced_doc_ids = {d["doc_id"] for d in balanced_docs}

    if not full_doc_ids:
        raise RuntimeError("No full docs manifest found for finbert stage.")
    if not FULL_SECTIONS_MANIFEST.exists():
        raise RuntimeError("No sections manifest found for finbert stage. Run sections first.")

    header = [
        "chunk_id",
        "doc_id",
        "year",
        "form_type",
        "cik",
        "filing_date",
        "accession",
        "source_name",
        "chunk_index",
        "word_count",
        "text",
    ]

    all_tmp = FINBERT_CHUNKS_ALL_CSV.with_suffix(FINBERT_CHUNKS_ALL_CSV.suffix + ".part")
    bal_tmp = FINBERT_CHUNKS_BALANCED_CSV.with_suffix(FINBERT_CHUNKS_BALANCED_CSV.suffix + ".part")

    all_count = 0
    bal_count = 0

    with all_tmp.open("w", newline="", encoding="utf-8") as all_f,          bal_tmp.open("w", newline="", encoding="utf-8") as bal_f:
        all_w = csv.writer(all_f)
        bal_w = csv.writer(bal_f)
        all_w.writerow(header)
        bal_w.writerow(header)

        current_doc_id = None
        current_sections = []

        def flush_doc_sections(doc_id: str, doc_sections: list[dict[str, Any]]) -> tuple[int, int]:
            if not doc_id or not doc_sections:
                return 0, 0

            doc_sections_sorted = sorted(
                doc_sections,
                key=lambda x: (SECTION_PRIORITY.get(x["section_name"], 50), int(x.get("section_rank", 999)))
            )
            selected = []
            for s in doc_sections_sorted[:args.max_sections_per_doc]:
                if len(s["text"]) >= args.min_section_chars:
                    selected.append((s["section_name"], s["text"], s))
            if not selected:
                s = doc_sections_sorted[0]
                selected = [(s["section_name"], s["text"], s)]

            local_all = 0
            local_bal = 0
            chunk_idx = 0
            for source_name, source_text, meta in selected:
                for ch in chunk_words(source_text, args.chunk_words, args.overlap_words):
                    chunk_idx += 1
                    row = [
                        hash_id(doc_id, source_name, str(chunk_idx)),
                        doc_id,
                        meta["year"],
                        meta["form_type"],
                        meta["cik"],
                        meta["filing_date"],
                        meta["accession"],
                        source_name,
                        chunk_idx,
                        count_words(ch),
                        ch,
                    ]
                    all_w.writerow(row)
                    local_all += 1
                    if doc_id in balanced_doc_ids:
                        bal_w.writerow(row)
                        local_bal += 1
            return local_all, local_bal

        for sec in iter_section_text_records():
            did = sec["doc_id"]
            if did not in full_doc_ids:
                continue
            if current_doc_id is None:
                current_doc_id = did
            if did != current_doc_id:
                a, b = flush_doc_sections(current_doc_id, current_sections)
                all_count += a
                bal_count += b
                current_doc_id = did
                current_sections = [sec]
            else:
                current_sections.append(sec)

        if current_sections:
            a, b = flush_doc_sections(current_doc_id, current_sections)
            all_count += a
            bal_count += b

    all_tmp.replace(FINBERT_CHUNKS_ALL_CSV)
    bal_tmp.replace(FINBERT_CHUNKS_BALANCED_CSV)

    with FINBERT_SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump({
            "chunk_words": args.chunk_words,
            "overlap_words": args.overlap_words,
            "max_sections_per_doc": args.max_sections_per_doc,
            "min_section_chars": args.min_section_chars,
            "all_chunk_count": all_count,
            "balanced_chunk_count": bal_count,
        }, f, indent=2)

    elapsed = now_ts() - start
    print(
        f"[finbert] done | all_chunks={all_count:,} | balanced_chunks={bal_count:,} | elapsed={fmt_elapsed(elapsed)}",
        flush=True
    )


# ============================================================
# ALL
# ============================================================

def run_all(args: argparse.Namespace) -> None:
    if not INVENTORY_CSV.exists() or args.force_inventory:
        run_inventory(args)
    run_clean(args)
    run_sections(args)
    run_quality(args)
    run_datasets(args)
    run_finbert(args)


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Comprehensive HDD-aware SEC filings text cleaning + engineering pipeline."
    )
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("--input-dir", type=str, default=str(RAW_FILINGS_DIR))
        sp.add_argument("--cik-map", type=str, default=str(DEFAULT_CIK_MAP))
        sp.add_argument("--issuer-tickers", type=str, default=str(DEFAULT_ISSUER_TICKERS))
        sp.add_argument("--workers", type=int, default=0)
        sp.add_argument("--shard-multiplier", type=int, default=2)
        sp.add_argument("--resume", action="store_true")
        sp.add_argument("--overwrite", action="store_true")
        sp.add_argument("--min-free-gb", type=float, default=8.0)

    s = sub.add_parser("inventory")
    common(s)

    s = sub.add_parser("clean")
    common(s)

    s = sub.add_parser("sections")
    common(s)

    s = sub.add_parser("quality")
    common(s)

    s = sub.add_parser("datasets")
    common(s)
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--min-docs-per-year", type=int, default=1000)
    s.add_argument("--target-per-year", type=int, default=0)
    s.add_argument("--max-docs-per-cik-per-year-form", type=int, default=5)
    s.add_argument("--form-weights", type=str, default="")

    s = sub.add_parser("finbert")
    common(s)
    s.add_argument("--chunk-words", type=int, default=350)
    s.add_argument("--overlap-words", type=int, default=50)
    s.add_argument("--max-sections-per-doc", type=int, default=3)
    s.add_argument("--min-section-chars", type=int, default=500)

    s = sub.add_parser("all")
    common(s)
    s.add_argument("--force-inventory", action="store_true")
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--min-docs-per-year", type=int, default=1000)
    s.add_argument("--target-per-year", type=int, default=0)
    s.add_argument("--max-docs-per-cik-per-year-form", type=int, default=5)
    s.add_argument("--form-weights", type=str, default="")
    s.add_argument("--chunk-words", type=int, default=350)
    s.add_argument("--overlap-words", type=int, default=50)
    s.add_argument("--max-sections-per-doc", type=int, default=3)
    s.add_argument("--min-section-chars", type=int, default=500)

    return p.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs(args.overwrite)

    print(f"Input directory : {args.input_dir}", flush=True)
    print(f"Output root     : {OUT_ROOT}", flush=True)
    print(f"Workers         : {choose_worker_count(args.workers)}", flush=True)
    print(f"Resume mode     : {args.resume}", flush=True)
    print(f"Min free space  : {args.min_free_gb:.2f} GB", flush=True)
    print(f"JSON parser     : {'orjson' if HAS_ORJSON else 'stdlib json'}", flush=True)

    cmd = args.command
    if cmd == "inventory":
        run_inventory(args)
    elif cmd == "clean":
        run_clean(args)
    elif cmd == "sections":
        run_sections(args)
    elif cmd == "quality":
        run_quality(args)
    elif cmd == "datasets":
        run_datasets(args)
    elif cmd == "finbert":
        run_finbert(args)
    elif cmd == "all":
        run_all(args)
    else:
        raise SystemExit(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()

