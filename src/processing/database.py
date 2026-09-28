"""SQLite schema management and idempotent per-filing writes."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .extract_address import normalize_address_fields


def connect_database(path: Path, schema_path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(schema_path.read_text(encoding="utf-8"))
    return connection


def make_filing_id(filing: dict[str, Any]) -> str:
    material = "|".join((filing.get("source_relpath") or "", filing.get("accession_number") or "", filing.get("cik") or ""))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def record_exists(connection: sqlite3.Connection, source_relpath: str) -> dict[str, Any] | None:
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT filing_id, source_size_bytes, source_mtime_ns, source_sha256, filing_folder, processing_status "
        "FROM filings WHERE source_relpath=?", (source_relpath,)
    ).fetchone()
    return dict(row) if row else None


def write_filing(connection: sqlite3.Connection, payload: dict[str, Any]) -> str:
    """Commit one filing and all its normalized header/document rows atomically."""
    filing = payload["filing"]
    filing_id = make_filing_id(filing)
    warnings = payload.get("warnings", [])
    status = "complete_with_warnings" if warnings or any(d.get("zip_error") for d in payload.get("documents", [])) else "complete"
    now = datetime.now(timezone.utc).isoformat()
    company = payload.get("company_data", {})
    with connection:
        connection.execute(
            """INSERT INTO filings (
                filing_id, accession_number, cik, company_name, form_type, filed_as_of_date,
                period_of_report, acceptance_datetime, public_document_count, sec_act,
                sec_file_number, film_number, sic, irs_number, state_of_incorporation,
                fiscal_year_end, source_filename, source_relpath, source_size_bytes,
                source_mtime_ns, source_sha256, filing_folder, sec_header_raw,
                processed_at_utc, processing_status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_relpath) DO UPDATE SET
                accession_number=excluded.accession_number, cik=excluded.cik,
                company_name=excluded.company_name, form_type=excluded.form_type,
                filed_as_of_date=excluded.filed_as_of_date, period_of_report=excluded.period_of_report,
                acceptance_datetime=excluded.acceptance_datetime,
                public_document_count=excluded.public_document_count, sec_act=excluded.sec_act,
                sec_file_number=excluded.sec_file_number, film_number=excluded.film_number,
                sic=excluded.sic, irs_number=excluded.irs_number,
                state_of_incorporation=excluded.state_of_incorporation,
                fiscal_year_end=excluded.fiscal_year_end, source_filename=excluded.source_filename,
                source_size_bytes=excluded.source_size_bytes, source_mtime_ns=excluded.source_mtime_ns,
                source_sha256=excluded.source_sha256, filing_folder=excluded.filing_folder,
                sec_header_raw=excluded.sec_header_raw, processed_at_utc=excluded.processed_at_utc,
                processing_status=excluded.processing_status""",
            (filing_id, filing.get("accession_number"), filing.get("cik"), filing.get("company_name"),
             filing.get("form_type"), filing.get("filed_as_of_date"), filing.get("period_of_report"),
             filing.get("acceptance_datetime"), filing.get("public_document_count"), filing.get("sec_act"),
             filing.get("sec_file_number"), filing.get("film_number"), filing.get("sic"),
             filing.get("irs_number"), filing.get("state_of_incorporation"), filing.get("fiscal_year_end"),
             filing.get("source_filename"), filing.get("source_relpath"), filing.get("source_size_bytes"),
             filing.get("source_mtime_ns"), filing.get("source_sha256"), filing.get("filing_folder"),
             filing.get("sec_header_raw", ""), now, status),
        )
        # Resolve the durable filing id after an UPSERT in case this row already existed.
        persisted_id = connection.execute(
            "SELECT filing_id FROM filings WHERE source_relpath=?", (filing["source_relpath"],)
        ).fetchone()[0]
        for table in ("header_fields", "company_data", "addresses", "former_names", "filing_items", "documents", "processing_warnings"):
            connection.execute(f"DELETE FROM {table} WHERE filing_id=?", (persisted_id,))
        connection.execute(
            "INSERT INTO company_data VALUES (?,?,?,?,?,?,?)",
            (persisted_id, company.get("COMPANY CONFORMED NAME"), filing.get("cik"),
             company.get("STANDARD INDUSTRIAL CLASSIFICATION"), company.get("IRS NUMBER"),
             company.get("STATE OF INCORPORATION"), company.get("FISCAL YEAR END")),
        )
        for kind, fields in payload.get("addresses", {}).items():
            address = normalize_address_fields(fields)
            connection.execute(
                """INSERT INTO addresses(filing_id,address_kind,street1,street2,city,state,postal_code,phone)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (persisted_id, kind, address["street1"], address["street2"], address["city"],
                 address["state"], address["zip"], address["phone"]),
            )
        connection.executemany(
            "INSERT INTO former_names(filing_id,ordinal,former_name,date_of_name_change) VALUES(?,?,?,?)",
            [(persisted_id, i, row.get("former_name"), row.get("date_of_name_change"))
             for i, row in enumerate(payload.get("former_names", []))],
        )
        connection.executemany(
            "INSERT INTO filing_items(filing_id,ordinal,item_information) VALUES(?,?,?)",
            [(persisted_id, i, item) for i, item in enumerate(payload.get("items", []))],
        )
        connection.executemany(
            "INSERT INTO header_fields(filing_id,section,field_name,field_value,occurrence) VALUES(?,?,?,?,?)",
            [(persisted_id, x["section"], x["field_name"], x["field_value"], x["occurrence"])
             for x in payload.get("header_fields", [])],
        )
        for document in payload.get("documents", []):
            cursor = connection.execute(
                """INSERT INTO documents(filing_id,sequence,document_type,filename,description,relative_path,
                   size_bytes,sha256,encoding_status,zip_extracted_files,zip_extracted_bytes,zip_error)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (persisted_id, document.get("sequence"), document.get("document_type"), document.get("filename"),
                 document.get("description"), document.get("relative_path"), int(document.get("size_bytes", 0)),
                 document.get("sha256"), document.get("encoding_status", "unknown"),
                 int(document.get("zip_extracted_files", 0)), int(document.get("zip_extracted_bytes", 0)),
                 document.get("zip_error")),
            )
            document_id = cursor.lastrowid
            connection.executemany(
                "INSERT INTO archive_members(document_id,member_name,extracted_path,size_bytes,sha256) VALUES(?,?,?,?,?)",
                [(document_id, member["member_name"], member["extracted_path"], int(member["size_bytes"]), member["sha256"])
                 for member in document.get("zip_members", [])],
            )
        connection.executemany(
            "INSERT INTO processing_warnings(filing_id,warning) VALUES(?,?)",
            [(persisted_id, warning) for warning in warnings],
        )
    return persisted_id
