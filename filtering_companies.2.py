#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import tempfile
import dotenv
from pathlib import Path

dotenv.load_dotenv()


# PATHS / CONFIG
MANIFEST_DIR = Path(os.getenv("MANIFEST_DIR", "manifests"))

DEFAULT_INPUT_MANIFEST = MANIFEST_DIR / "filings_manifest_2000_2026.csv"
DEFAULT_FILTERED_MANIFEST = MANIFEST_DIR / "filtered_manifest_2000_2026.csv"
DEFAULT_TICKERS_FILE = Path(os.getenv("MAIN_TICKERS_FILE", "main_tickers.csv"))

FORMS_EXACT = {"8-K", "10-K", "10-Q", "DEF 14A"}


def normalize_cik(value: str | None) -> str:
	cik = (value or "").strip()
	if cik.isdigit():
		return cik.lstrip("0") or "0"
	return cik


def load_ticker_ciks(path: Path) -> set[str]:
	ciks: set[str] = set()
	with path.open("r", newline="", encoding="utf-8-sig") as source:
		reader = csv.DictReader(source)
		if not reader.fieldnames or "cik" not in reader.fieldnames:
			raise ValueError(f"Missing 'cik' column in ticker file: {path}")

		for row in reader:
			cik = normalize_cik(row.get("cik"))
			if cik:
				ciks.add(cik)

	if not ciks:
		raise ValueError(f"No CIK values found in ticker file: {path}")
	return ciks


def filter_manifest(
	input_path: Path,
	output_path: Path,
	ticker_path: Path,
	overwrite: bool = False,
) -> None:
	if input_path.resolve() == output_path.resolve():
		raise ValueError("Input manifest and output manifest must be different files")
	if output_path.exists() and not overwrite:
		raise FileExistsError(
			f"Output already exists: {output_path}. Use --overwrite to replace it."
		)

	ticker_ciks = load_ticker_ciks(ticker_path)
	output_path.parent.mkdir(parents=True, exist_ok=True)
	temporary_path: Path | None = None
	scanned = kept = dropped_form = dropped_cik = 0

	try:
		with input_path.open("r", newline="", encoding="utf-8-sig") as source:
			reader = csv.DictReader(source)
			fieldnames = reader.fieldnames
			if not fieldnames:
				raise ValueError(f"Manifest has no header: {input_path}")

			required = {"cik", "form_type"}
			missing = required - set(fieldnames)
			if missing:
				raise ValueError(
					f"Manifest missing required columns: {sorted(missing)}"
				)

			with tempfile.NamedTemporaryFile(
				mode="w",
				newline="",
				encoding="utf-8",
				dir=output_path.parent,
				prefix=f"{output_path.name}.",
				suffix=".tmp",
				delete=False,
			) as temporary:
				temporary_path = Path(temporary.name)
				writer = csv.DictWriter(temporary, fieldnames=fieldnames)
				writer.writeheader()

				for row in reader:
					scanned += 1
					form_type = (row.get("form_type") or "").strip().upper()
					if form_type not in FORMS_EXACT:
						dropped_form += 1
						continue

					if normalize_cik(row.get("cik")) not in ticker_ciks:
						dropped_cik += 1
						continue

					row["form_type"] = form_type
					writer.writerow(row)
					kept += 1

		if output_path.exists() and not overwrite:
			raise FileExistsError(
				f"Output appeared while filtering: {output_path}. "
				"Use --overwrite to replace it."
			)
		temporary_path.replace(output_path)
	finally:
		if temporary_path is not None and temporary_path.exists():
			temporary_path.unlink()

	print(f"Input rows scanned: {scanned:,}")
	print(f"Rows retained: {kept:,}")
	print(f"Rows dropped for form type: {dropped_form:,}")
	print(f"Rows dropped for CIK: {dropped_cik:,}")
	print(f"Accepted form types: {', '.join(sorted(FORMS_EXACT))}")
	print(f"Filtered manifest: {output_path}")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Filter an SEC filing manifest to the four core forms and CIKs "
			"listed in main_tickers.csv."
		)
	)
	parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_MANIFEST)
	parser.add_argument("--output", type=Path, default=DEFAULT_FILTERED_MANIFEST)
	parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS_FILE)
	parser.add_argument(
		"--overwrite",
		action="store_true",
		help="Replace the output manifest if it already exists.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	filter_manifest(
		input_path=args.input,
		output_path=args.output,
		ticker_path=args.tickers,
		overwrite=args.overwrite,
	)


if __name__ == "__main__":
	main()

# Usage:
# python filtering_companies.2.py --input data/sec_edgar/manifests/filings_manifest_2000_2026.csv  --output  data/sec_edgar/manifests/filtered_manifest_2000_2026.csv --tickers data/main_tickers.csv
