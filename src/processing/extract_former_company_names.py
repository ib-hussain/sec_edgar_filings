"""Extract former-company name history from the SEC header."""

from __future__ import annotations

from typing import Any


def extract_former_names(sections: list[dict[str, Any]]) -> list[dict[str, str | None]]:
    return [
        {
            "former_name": section.get("FORMER CONFORMED NAME"),
            "date_of_name_change": section.get("DATE OF NAME CHANGE"),
        }
        for section in sections
        if section.get("FORMER CONFORMED NAME") or section.get("DATE OF NAME CHANGE")
    ]
