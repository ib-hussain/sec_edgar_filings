#!/usr/bin/env python3
"""
sec_filings_topup_manifest.py

Detect weak years in the already-processed filings text corpus and generate a small
targeted SEC download manifest that tops those years up to the current balanced target.

This script does NOT hallucinate missing data. It only creates a targeted download list.
That is the only honest way to "bring weak years up to the same level" without corrupting
the text corpus with synthetic oversampling.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from collections import defaultdict, Counter, deque
from pathlib import Path
from typing import Any

dataPath = Path(os.getenv("dataPathGlobal", "data"))

CLEANED_DIR = dataPath / "sec_edgar" / "processed" / "cleaned"
FILINGS_TEXT_DIR = dataPath / "sec_edgar" / "processed" / "filings_text"
MANIFEST_DIR = dataPath / "sec_edgar" / "processed" / "manifests"

DEFAULT_CIK_MAP = CLEANED_DIR / "cik_ticker_map_cleaned.csv"
DEFAULT_ISSUER_TICKERS = CLEANED_DIR / "issuer_master_onlyTickers.csv"

DEFAULT_YEAR_COVERAGE = FILINGS_TEXT_DIR / "filings_year_coverage.csv"
DEFAULT_BALANCE_SUMMARY = FILINGS_TEXT_DIR / "filings_balance_summary.json"
DEFAULT_FULL_DOCS = FILINGS_TEXT_DIR / "filings_filtered_full.csv"

DEFAULT_SOURCE_MANIFEST_COMPANYSCOPE = MANIFEST_DIR / "filings_manifest_core_companyscope_2000_2024.csv"
DEFAULT_SOURCE_MANIFEST_MERGED = MANIFEST_DIR / "filings_manifest_2000_2024_merged.csv"

DEFAULT_OUTPUT_MANIFEST = MANIFEST_DIR / "filings_manifest_weak_years_topup.csv"
DEFAULT_OUTPUT_SUMMARY = FILINGS_TEXT_DIR / "filings_topup_summary.json"

ALLOWED_FORMS = {"10-K", "10-Q", "8-K", "DEF 14A"}
FORM_ORDER = ["10-K", "10-Q", "8-K", "DEF 14A"]

DEFAULT_FORM_WEIGHTS = {
    "10-K": 0.30,
    "10-Q": 0.30,
    "8-K": 0.30,
    "DEF 14A": 0.10,
}


def normalize_cik(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return ""
    s = s.lstrip("0")
    return s if s else "0"


def detect_cik_column(fieldnames: list[str]) -> str:
    lowered = {c.lower(): c for c in fieldnames}
    for cand in ("cik", "padded_cik", "sec_cik", "issuer_cik"):
        if cand in lowered:
            return lowered[cand]
    raise RuntimeError(f"Could not find a CIK column in {fieldnames}")


def load_cik_union(csv_paths: list[Path]) -> set[str]:
    cik_set: set[str] = set()
    for path in csv_paths:
        if not path.exists():
            raise FileNotFoundError(f"Required CIK source file not found: {path}")
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            cik_col = detect_cik_column(fieldnames)
            loaded = 0
            for row in reader:
                cik = normalize_cik(row.get(cik_col, ""))
                if cik:
                    cik_set.add(cik)
                    loaded += 1
            print(f"[cik-union] loaded {loaded:,} rows from {path} | cumulative unique CIKs={len(cik_set):,}", flush=True)
    return cik_set


def parse_form_weights(text: str) -> dict[str, float]:
    weights = DEFAULT_FORM_WEIGHTS.copy()
    if text:
        for kv in text.split(","):
            if "=" not in kv:
                continue
            k, v = kv.split("=", 1)
            k = k.strip().upper().replace("_", " ")
            if k in weights:
                weights[k] = float(v)
    total = sum(weights.values()) or 1.0
    return {k: v / total for k, v in weights.items()}


def load_target_per_year(balance_summary_path: Path, override_target: int) -> int:
    if override_target > 0:
        return override_target
    if not balance_summary_path.exists():
        raise RuntimeError(
            f"Could not find balance summary at {balance_summary_path} and no --target-per-year was given."
        )
    with balance_summary_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    target = int(data.get("target_per_year", 0) or 0)
    if target <= 0:
        raise RuntimeError("target_per_year missing/invalid in balance summary.")
    return target


def load_year_coverage(path: Path) -> dict[str, int]:
    if not path.exists():
        raise FileNotFoundError(path)
    out: dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = (row.get("year") or "").strip()
            count = int(row.get("doc_count") or 0)
            if year:
                out[year] = count
    return out


def load_existing_doc_keys(full_docs_path: Path) -> tuple[set[str], set[str]]:
    relpaths: set[str] = set()
    cik_acc: set[str] = set()
    if not full_docs_path.exists():
        return relpaths, cik_acc

    with full_docs_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            relp = (row.get("rel_path") or "").replace("\\", "/").strip()
            if relp:
                relpaths.add(relp)

            cik = normalize_cik(row.get("cik", ""))
            accession = (row.get("accession") or "").strip()
            if cik and accession:
                cik_acc.add(f"{cik}|{accession}")
    return relpaths, cik_acc


def year_from_date(date_str: str) -> str:
    if not date_str:
        return ""
    return date_str[:4]


def weighted_form_quotas(deficit: int, available_by_form: dict[str, int], weights: dict[str, float]) -> dict[str, int]:
    quotas = {f: 0 for f in FORM_ORDER}
    remaining = deficit

    for f in FORM_ORDER:
        want = int(round(deficit * weights.get(f, 0.0)))
        take = min(want, available_by_form.get(f, 0))
        quotas[f] = take
        remaining -= take

    if remaining <= 0:
        return quotas

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


def choose_default_manifest() -> Path:
    if DEFAULT_SOURCE_MANIFEST_COMPANYSCOPE.exists():
        return DEFAULT_SOURCE_MANIFEST_COMPANYSCOPE
    return DEFAULT_SOURCE_MANIFEST_MERGED


def manifest_row_allowed(row: dict[str, str], cik_union: set[str], weak_years: set[str], allowed_forms: set[str]) -> bool:
    form = (row.get("form_type") or "").strip().upper()
    cik = normalize_cik(row.get("cik", ""))
    date_filed = (row.get("date_filed") or "").strip()
    year = year_from_date(date_filed)

    if form not in allowed_forms:
        return False
    if year not in weak_years:
        return False
    if cik not in cik_union:
        return False
    return True


def rr_select_diverse(rows: list[dict[str, str]], quota: int, seed: int) -> list[dict[str, str]]:
    if quota <= 0 or not rows:
        return []

    rng = random.Random(seed)
    by_cik: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        by_cik[normalize_cik(r.get("cik", ""))].append(r)

    cik_order = list(by_cik.keys())
    rng.shuffle(cik_order)
    for cik in cik_order:
        rng.shuffle(by_cik[cik])

    queues = deque(cik_order)
    selected: list[dict[str, str]] = []

    while queues and len(selected) < quota:
        cik = queues.popleft()
        bucket = by_cik[cik]
        if bucket:
            selected.append(bucket.pop())
        if bucket:
            queues.append(cik)

    return selected


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate a targeted top-up SEC filings manifest for weak years only."
    )
    p.add_argument("--cik-map", type=str, default=str(DEFAULT_CIK_MAP))
    p.add_argument("--issuer-tickers", type=str, default=str(DEFAULT_ISSUER_TICKERS))
    p.add_argument("--year-coverage", type=str, default=str(DEFAULT_YEAR_COVERAGE))
    p.add_argument("--balance-summary", type=str, default=str(DEFAULT_BALANCE_SUMMARY))
    p.add_argument("--full-docs", type=str, default=str(DEFAULT_FULL_DOCS))
    p.add_argument("--source-manifest", type=str, default=str(choose_default_manifest()))
    p.add_argument("--output-manifest", type=str, default=str(DEFAULT_OUTPUT_MANIFEST))
    p.add_argument("--output-summary", type=str, default=str(DEFAULT_OUTPUT_SUMMARY))
    p.add_argument("--target-per-year", type=int, default=0)
    p.add_argument("--max-topup-per-year", type=int, default=0)
    p.add_argument("--form-weights", type=str, default="")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    cik_union = load_cik_union([Path(args.cik_map), Path(args.issuer_tickers)])
    target_per_year = load_target_per_year(Path(args.balance_summary), args.target_per_year)
    year_cov = load_year_coverage(Path(args.year_coverage))
    processed_relpaths, processed_cik_acc = load_existing_doc_keys(Path(args.full_docs))
    weights = parse_form_weights(args.form_weights)

    weak_years = {}
    for year, count in sorted(year_cov.items()):
        if count < target_per_year:
            deficit = target_per_year - count
            if args.max_topup_per_year > 0:
                deficit = min(deficit, args.max_topup_per_year)
            weak_years[year] = deficit

    if not weak_years:
        print("[topup] No weak years detected. Nothing to do.", flush=True)
        return

    print(f"[topup] target_per_year={target_per_year:,}", flush=True)
    print(f"[topup] weak years detected: {weak_years}", flush=True)

    source_manifest = Path(args.source_manifest)
    if not source_manifest.exists():
        raise FileNotFoundError(f"Source manifest not found: {source_manifest}")

    candidates_by_year_form: dict[str, dict[str, list[dict[str, str]]]] = defaultdict(lambda: defaultdict(list))
    scanned = 0
    kept_candidates = 0
    skipped_existing = 0
    skipped_processed_accession = 0

    with source_manifest.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise RuntimeError(f"No header in source manifest: {source_manifest}")

        required = {"cik", "form_type", "date_filed", "output_path", "filing_url"}
        missing = required - set(fieldnames)
        if missing:
            raise RuntimeError(f"Source manifest missing required columns: {sorted(missing)}")

        for row in reader:
            scanned += 1

            if not manifest_row_allowed(row, cik_union, set(weak_years.keys()), ALLOWED_FORMS):
                continue

            output_path = (row.get("output_path") or "").replace("\\", "/").strip()
            if output_path in processed_relpaths:
                skipped_existing += 1
                continue

            cik = normalize_cik(row.get("cik", ""))
            accession = (row.get("accession") or "").strip()
            if cik and accession and f"{cik}|{accession}" in processed_cik_acc:
                skipped_processed_accession += 1
                continue

            year = year_from_date(row.get("date_filed", ""))
            form = (row.get("form_type") or "").strip().upper()

            candidates_by_year_form[year][form].append(row)
            kept_candidates += 1

            if scanned % 500_000 == 0:
                print(
                    f"[topup] scanned={scanned:,} candidate_rows={kept_candidates:,} "
                    f"skip_existing={skipped_existing:,} skip_processed_accession={skipped_processed_accession:,}",
                    flush=True,
                )

    final_rows: list[dict[str, str]] = []
    planning = {}

    for year in sorted(weak_years.keys()):
        deficit = weak_years[year]
        available_by_form = {f: len(candidates_by_year_form[year].get(f, [])) for f in FORM_ORDER}
        quotas = weighted_form_quotas(deficit, available_by_form, weights)

        selected_for_year: list[dict[str, str]] = []
        for i, form in enumerate(FORM_ORDER, start=1):
            candidates = candidates_by_year_form[year].get(form, [])
            selected = rr_select_diverse(candidates, quotas.get(form, 0), args.seed + i + int(year))
            selected_for_year.extend(selected)

        still_need = deficit - len(selected_for_year)
        if still_need > 0:
            leftovers = []
            selected_keys = {
                (
                    normalize_cik(r.get("cik", "")),
                    (r.get("accession") or "").strip(),
                    (r.get("filing_url") or "").strip(),
                )
                for r in selected_for_year
            }
            for form in FORM_ORDER:
                for r in candidates_by_year_form[year].get(form, []):
                    key = (
                        normalize_cik(r.get("cik", "")),
                        (r.get("accession") or "").strip(),
                        (r.get("filing_url") or "").strip(),
                    )
                    if key not in selected_keys:
                        leftovers.append(r)
            extra = rr_select_diverse(leftovers, still_need, args.seed + 999 + int(year))
            selected_for_year.extend(extra)

        final_rows.extend(selected_for_year)

        planning[year] = {
            "current_count": year_cov.get(year, 0),
            "target_per_year": target_per_year,
            "deficit_requested": deficit,
            "available_by_form": available_by_form,
            "quotas": quotas,
            "selected_count": len(selected_for_year),
            "selected_by_form": dict(Counter((r.get("form_type") or "").strip().upper() for r in selected_for_year)),
        }

    output_manifest = Path(args.output_manifest)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)

    with source_manifest.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

    unique_rows = []
    seen_urls = set()
    for row in final_rows:
        url = (row.get("filing_url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        unique_rows.append(row)

    tmp_manifest = output_manifest.with_suffix(output_manifest.suffix + ".part")
    with tmp_manifest.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in unique_rows:
            writer.writerow(row)
    tmp_manifest.replace(output_manifest)

    summary = {
        "target_per_year": target_per_year,
        "weak_years": weak_years,
        "weights": weights,
        "source_manifest": str(source_manifest),
        "output_manifest": str(output_manifest),
        "scanned_rows": scanned,
        "candidate_rows_kept": kept_candidates,
        "skipped_existing_relpath": skipped_existing,
        "skipped_existing_cik_accession": skipped_processed_accession,
        "final_topup_rows": len(unique_rows),
        "planning": planning,
    }

    output_summary = Path(args.output_summary)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    with output_summary.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"[topup] wrote targeted manifest: {output_manifest}", flush=True)
    print(f"[topup] rows={len(unique_rows):,}", flush=True)
    print(f"[topup] summary={output_summary}", flush=True)


if __name__ == "__main__":
    main()

#  run:
# nice -n -20 python -u data/sec_filings_topup_manifest.py
# Rerun the text pipeline incrementally:
# python data/sec_filings_text_pipeline_v2.py inventory
# python data/sec_filings_text_pipeline_v2.py clean --resume
# python data/sec_filings_text_pipeline_v2.py sections --resume
# python data/sec_filings_text_pipeline_v2.py quality
# python data/sec_filings_text_pipeline_v2.py datasets
# python data/sec_filings_text_pipeline_v2.py finbert
