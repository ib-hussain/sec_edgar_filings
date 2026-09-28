#!/usr/bin/env python3
from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
args = [str(ROOT / "extract_structure.5.py"), "--forms", "8-K", *sys.argv[1:]]
sys.argv = args
runpy.run_path(str(ROOT / "extract_structure.5.py"), run_name="__main__")
