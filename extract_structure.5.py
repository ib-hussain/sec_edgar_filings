#!/usr/bin/env python3
"""Run phase one: SEC submission decomposition and normalized SQLite capture."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.processing.orchestrator import FORM_FOLDERS, auto_workers, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decompose SEC raw submission files into per-filing folders and form-specific SQLite databases."
    )
    parser.add_argument("--raw-dir", type=Path, default=Path(os.getenv("FILING_DIR", REPO_ROOT / "raw")))
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "processed")
    parser.add_argument("--workers", type=int, default=0,
                        help=f"Worker threads; default 0 uses all {auto_workers()} available CPU threads.")
    parser.add_argument("--forms", nargs="+", choices=FORM_FOLDERS, default=list(FORM_FOLDERS))
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--max-zip-bytes", type=int, default=4 * 1024**3,
                        help="Maximum expanded bytes per ZIP attachment (default 4 GiB).")
    parser.add_argument("--max-zip-members", type=int, default=100_000)
    parser.add_argument("--verify-hashes", action="store_true",
                        help="Rehash source files before treating existing outputs as current (extra disk I/O).")
    parser.add_argument("--min-free-gb", type=float, default=10.0,
                        help="Stop writing below this free-space reserve; use 0 to disable the guard.")
    parser.add_argument("--delete-raw-after-success", action="store_true",
                        help="Delete each raw .txt only after its extracted folder and SQLite transaction succeed.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.workers < 0:
        raise SystemExit("--workers must be zero (auto) or a positive integer")
    if args.progress_every < 1:
        raise SystemExit("--progress-every must be at least 1")
    if args.max_zip_bytes < 1 or args.max_zip_members < 1:
        raise SystemExit("ZIP limits must be positive")
    run_pipeline(
        raw_root=args.raw_dir,
        output_root=args.output_dir,
        forms=tuple(args.forms),
        workers=args.workers or auto_workers(),
        delete_raw_after_success=args.delete_raw_after_success,
        max_zip_bytes=args.max_zip_bytes,
        max_zip_members=args.max_zip_members,
        progress_every=args.progress_every,
        verify_hashes=args.verify_hashes,
        min_free_gb=args.min_free_gb,
    )


if __name__ == "__main__":
    main()
