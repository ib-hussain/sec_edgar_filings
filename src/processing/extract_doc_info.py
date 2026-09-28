"""Helpers for document-level SGML fields."""

from __future__ import annotations

import re

DOC_FIELD_RE = re.compile(r"^\s*<(TYPE|SEQUENCE|FILENAME|DESCRIPTION)>\s*(.*?)\s*$", re.I)


def parse_document_field(line: str) -> tuple[str, str] | None:
    match = DOC_FIELD_RE.match(line.rstrip("\r\n"))
    return (match.group(1).upper(), match.group(2).strip()) if match else None
