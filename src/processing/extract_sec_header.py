"""Parse the plain-text SEC-HEADER without assuming fixed indentation."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .extract_former_company_names import extract_former_names

FIELD_RE = re.compile(r"^\s*([A-Z][A-Z0-9 _./()-]*?)\s*:\s*(.*?)\s*$", re.I)
TOP_LEVEL = {"FILER", "COMPANY DATA", "FILING VALUES", "BUSINESS ADDRESS", "MAIL ADDRESS", "FORMER COMPANY"}


def parse_sec_header(lines: list[str]) -> dict[str, Any]:
    """Return both lossless key/value occurrences and commonly queried fields."""
    contexts: dict[str, dict[str, str]] = {
        "company_data": {}, "filing_values": {}, "business_address": {}, "mail_address": {}
    }
    former_sections: list[dict[str, str]] = []
    all_fields: list[dict[str, Any]] = []
    items: list[str] = []
    current = "filing"
    current_former: dict[str, str] | None = None
    scalar: dict[str, str] = {}
    occurrence: defaultdict[tuple[str, str], int] = defaultdict(int)

    for raw in lines:
        line = raw.rstrip("\r\n")
        stripped = line.strip()
        if not stripped:
            continue
        acceptance = re.match(r"<ACCEPTANCE-DATETIME>\s*(.*?)\s*$", stripped, re.I)
        if acceptance:
            scalar["ACCEPTANCE-DATETIME"] = acceptance.group(1).strip()
            all_fields.append({"section": "filing", "field_name": "ACCEPTANCE-DATETIME",
                               "field_value": acceptance.group(1).strip(), "occurrence": 0})
            continue
        if stripped.startswith("<"):
            continue
        upper = stripped.upper().rstrip(":")
        if upper in TOP_LEVEL:
            if upper == "COMPANY DATA": current = "company_data"
            elif upper == "FILING VALUES": current = "filing_values"
            elif upper == "BUSINESS ADDRESS": current = "business_address"
            elif upper == "MAIL ADDRESS": current = "mail_address"
            elif upper == "FORMER COMPANY":
                current = "former_company"
                current_former = {}
                former_sections.append(current_former)
            elif upper == "FILER": current = "filer"
            continue
        match = FIELD_RE.match(line)
        if not match:
            continue
        key, value = match.group(1).strip().upper(), match.group(2).strip()
        value = value or ""
        if key == "ITEM INFORMATION":
            items.append(value)
        else:
            context = current
            all_fields.append({"section": context, "field_name": key, "field_value": value,
                               "occurrence": occurrence[(context, key)]})
            occurrence[(context, key)] += 1
            if current in contexts:
                contexts[current][key] = value
            elif current == "former_company" and current_former is not None:
                current_former[key] = value
            elif current in {"filing", "filer"}:
                scalar[key] = value

    company = contexts["company_data"]
    filing_values = contexts["filing_values"]
    filing = {
        "accession_number": scalar.get("ACCESSION NUMBER"),
        "form_type": scalar.get("CONFORMED SUBMISSION TYPE") or filing_values.get("FORM TYPE"),
        "public_document_count": scalar.get("PUBLIC DOCUMENT COUNT"),
        "period_of_report": scalar.get("CONFORMED PERIOD OF REPORT"),
        "filed_as_of_date": scalar.get("FILED AS OF DATE"),
        "acceptance_datetime": scalar.get("ACCEPTANCE-DATETIME"),
        "date_as_of_change": scalar.get("DATE AS OF CHANGE"),
        "effectiveness_date": scalar.get("EFFECTIVENESS DATE"),
        "company_name": company.get("COMPANY CONFORMED NAME"),
        "cik": company.get("CENTRAL INDEX KEY"),
        "sic": company.get("STANDARD INDUSTRIAL CLASSIFICATION"),
        "irs_number": company.get("IRS NUMBER"),
        "state_of_incorporation": company.get("STATE OF INCORPORATION"),
        "fiscal_year_end": company.get("FISCAL YEAR END"),
        "form_type_from_filing_values": filing_values.get("FORM TYPE"),
        "sec_act": filing_values.get("SEC ACT"),
        "sec_file_number": filing_values.get("SEC FILE NUMBER"),
        "film_number": filing_values.get("FILM NUMBER"),
    }
    return {
        "filing": filing,
        "company_data": company,
        "filing_values": filing_values,
        "addresses": {
            "business": contexts["business_address"],
            "mailing": contexts["mail_address"],
        },
        "former_names": extract_former_names(former_sections),
        "items": items,
        "header_fields": all_fields,
        "former_company_sections": former_sections,
    }
