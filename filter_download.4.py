#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any

RAW_DIR = Path(os.getenv("FILING_DIR", "raw"))
CIK_LIST = Path(os.getenv("MAIN_TICKERS_FILE", "main_tickers.csv"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
PURGE_LOG = LOG_DIR / "purge_filings.jsonl"
FILENAME_CIK_PATTERN = re.compile(r"^(\d{1,10})__")


def normalize_cik(value: str | None) -> str:
	cik = (value or "").strip()
	if not cik.isdigit():
		return ""
	return cik.lstrip("0") or "0"


def load_allowed_ciks(path: Path) -> set[str]:
	if not path.is_file():
		raise FileNotFoundError(f"CIK allowlist not found: {path}")

	with path.open("r", newline="", encoding="utf-8-sig") as source:
		reader = csv.DictReader(source)
		if not reader.fieldnames or "cik" not in reader.fieldnames:
			raise ValueError(f"Missing 'cik' column in CIK allowlist: {path}")

		allowed_ciks = {
			normalize_cik(row.get("cik"))
			for row in reader
		}
		allowed_ciks.discard("")

	if not allowed_ciks:
		raise ValueError(f"No valid CIKs found in allowlist: {path}")
	return allowed_ciks


def empty_year_stats() -> dict[str, int]:
	return {
		"filing_files_scanned": 0,
		"allowed_cik_files": 0,
		"unclassified_files_left_untouched": 0,
		"undesired_files": 0,
		"undesired_bytes": 0,
		"deleted_files": 0,
		"deleted_bytes": 0,
		"delete_failures": 0,
	}


def find_undesired_files(
	raw_dir: Path,
	allowed_ciks: set[str],
) -> tuple[dict[str, dict[str, int]], list[dict[str, Any]]]:
	if not raw_dir.is_dir():
		raise FileNotFoundError(f"Raw filings directory not found: {raw_dir}")

	yearly_stats: dict[str, dict[str, int]] = {}
	candidates: list[dict[str, Any]] = []

	for year_dir in sorted(raw_dir.iterdir()):
		if not year_dir.is_dir() or not re.fullmatch(r"\d{4}", year_dir.name):
			continue

		stats = yearly_stats.setdefault(year_dir.name, empty_year_stats())
		for form_dir in sorted(year_dir.iterdir()):
			if not form_dir.is_dir():
				continue

			for path in sorted(form_dir.iterdir()):
				if path.is_symlink() or not path.is_file() or path.suffix.lower() != ".txt":
					continue

				stats["filing_files_scanned"] += 1
				match = FILENAME_CIK_PATTERN.match(path.name)
				if not match:
					stats["unclassified_files_left_untouched"] += 1
					continue

				cik = normalize_cik(match.group(1))
				if cik in allowed_ciks:
					stats["allowed_cik_files"] += 1
					continue

				size_bytes = path.stat().st_size
				stats["undesired_files"] += 1
				stats["undesired_bytes"] += size_bytes
				candidates.append({
					"year": year_dir.name,
					"cik": cik,
					"file_name": path.name,
					"path": str(path.resolve()),
					"size_bytes": size_bytes,
				})

	return yearly_stats, candidates


def append_jsonl(log_file: Any, record: dict[str, Any]) -> None:
	log_file.write(json.dumps(record, ensure_ascii=True) + "\n")
	log_file.flush()


def delete_candidates(
	candidates: list[dict[str, Any]],
	yearly_stats: dict[str, dict[str, int]],
	log_path: Path,
) -> None:
	log_path.parent.mkdir(parents=True, exist_ok=True)
	timestamp = datetime.now(timezone.utc).isoformat()

	with log_path.open("a", encoding="utf-8") as log_file:
		for candidate in candidates:
			path = Path(candidate["path"])
			try:
				current_size = path.stat().st_size
			except FileNotFoundError:
				yearly_stats[candidate["year"]]["delete_failures"] += 1
				append_jsonl(log_file, {
					"timestamp_utc": timestamp,
					"status": "missing_before_delete",
					**candidate,
				})
				continue

			record = {**candidate, "size_bytes": current_size}
			append_jsonl(log_file, {
				"timestamp_utc": timestamp,
				"status": "delete_started",
				**record,
			})
			try:
				path.unlink()
			except OSError as exc:
				yearly_stats[candidate["year"]]["delete_failures"] += 1
				append_jsonl(log_file, {
					"timestamp_utc": timestamp,
					"status": "delete_failed",
					"error": str(exc),
					**record,
				})
				continue

			yearly_stats[candidate["year"]]["deleted_files"] += 1
			yearly_stats[candidate["year"]]["deleted_bytes"] += current_size
			append_jsonl(log_file, {
				"timestamp_utc": timestamp,
				"status": "deleted",
				**record,
			})


def summarize(yearly_stats: dict[str, dict[str, int]]) -> dict[str, int]:
	totals = empty_year_stats()
	for stats in yearly_stats.values():
		for key, value in stats.items():
			totals[key] += value
	return totals


def write_report(
	yearly_stats: dict[str, dict[str, int]],
	totals: dict[str, int],
	raw_dir: Path,
	cik_list: Path,
	logs_dir: Path,
	delete_mode: bool,
) -> Path:
	logs_dir.mkdir(parents=True, exist_ok=True)
	timestamp = datetime.now(timezone.utc)
	report_path = logs_dir / f"purge_report_{timestamp:%Y-%m-%d}.json"
	report = {
		"timestamp_utc": timestamp.isoformat(),
		"mode": "delete" if delete_mode else "dry_run",
		"raw_directory": str(raw_dir.resolve()),
		"cik_allowlist": str(cik_list.resolve()),
		"allowed_cik_count": len(load_allowed_ciks(cik_list)),
		"deletion_log": (
			str((logs_dir / PURGE_LOG.name).resolve()) if delete_mode else None
		),
		"totals": totals,
		"by_year": yearly_stats,
	}
	with report_path.open("w", encoding="utf-8") as output:
		json.dump(report, output, indent=2)
		output.write("\n")
	return report_path


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Report or remove downloaded SEC filing text files whose filename CIK "
			"is not present in main_tickers.csv. Default mode only reports."
		)
	)
	parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
	parser.add_argument("--cik-list", type=Path, default=CIK_LIST)
	parser.add_argument("--logs-dir", type=Path, default=LOG_DIR)
	parser.add_argument(
		"--delete",
		action="store_true",
		help="Delete all identified undesired filing files; default is report-only.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	allowed_ciks = load_allowed_ciks(args.cik_list)
	yearly_stats, candidates = find_undesired_files(args.raw_dir, allowed_ciks)

	if args.delete:
		delete_candidates(candidates, yearly_stats, args.logs_dir / PURGE_LOG.name)

	totals = summarize(yearly_stats)
	report_path = write_report(
		yearly_stats=yearly_stats,
		totals=totals,
		raw_dir=args.raw_dir,
		cik_list=args.cik_list,
		logs_dir=args.logs_dir,
		delete_mode=args.delete,
	)

	mode = "deleted" if args.delete else "would delete"
	print(f"Mode: {'DELETE' if args.delete else 'DRY RUN'}")
	print("Year  Scanned  Allowed  Unclassified  Undesired  Undesired MB  Deleted  Failures")
	for year, stats in sorted(yearly_stats.items()):
		undesired_mb = stats["undesired_bytes"] / (1024 ** 2)
		print(
			f"{year}  {stats['filing_files_scanned']:>7,}  "
			f"{stats['allowed_cik_files']:>7,}  "
			f"{stats['unclassified_files_left_untouched']:>12,}  "
			f"{stats['undesired_files']:>9,}  {undesired_mb:>12,.2f}  "
			f"{stats['deleted_files']:>7,}  {stats['delete_failures']:>8,}"
		)

	print(
		f"Total: {totals['undesired_files']:,} files {mode}; "
		f"{totals['undesired_bytes'] / (1024 ** 3):,.2f} GiB identified"
	)
	if args.delete:
		print(f"Per-file removal log: {args.logs_dir / PURGE_LOG.name}")
	print(f"JSON report: {report_path}")


if __name__ == "__main__":
	main()


# Usage:
#   python filter_download.4.py                  # report only (safe default)
#   python filter_download.4.py --delete          # delete undesired CIK files
#   python filter_download.4.py --cik-list main_tickers.csv --raw-dir raw
