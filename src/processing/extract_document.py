"""Streaming decomposition of an SEC SGML submission into document files."""

from __future__ import annotations

import binascii
import hashlib
import json
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import Any

from .extract_doc_info import parse_document_field
from .extract_sec_header import parse_sec_header
from .making_file import ensure_free_space, extract_zip_safely, safe_filename, sha256_file

FORM_CANONICAL = {"DEF_14A": "DEF 14A", "DEF 14A": "DEF 14A", "8-K": "8-K", "10-K": "10-K", "10-Q": "10-Q"}
UU_BEGIN = re.compile(rb"^begin [0-7]{3} .+$")


def canonical_form(folder_name: str) -> str:
    try:
        return FORM_CANONICAL[folder_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported form directory: {folder_name}") from exc


def _decode_uu_if_present(path: Path) -> str:
    """Decode SEC uuencoded attachment bodies in place; retain undecodable bytes."""
    with path.open("rb") as source:
        first = source.readline().rstrip(b"\r\n")
    if not UU_BEGIN.match(first):
        return "plain"
    decoded = path.with_name(path.name + ".decoded")
    try:
        with path.open("rb") as source, decoded.open("wb") as target:
            source.readline()  # begin <mode> <name>
            for line in source:
                line = line.rstrip(b"\r\n")
                if line in {b"end", b"`"}:
                    break
                if not line:
                    continue
                target.write(binascii.a2b_uu(line))
        os.replace(decoded, path)
        return "uuencoded"
    except (binascii.Error, ValueError, OSError):
        decoded.unlink(missing_ok=True)
        return "uuencoded_decode_failed_kept_original"


def _normalise_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if re.fullmatch(r"\d{8}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def process_source_file(source: Path, raw_root: Path, form_folder: str, stage_root: Path,
                        max_zip_bytes: int, max_zip_members: int,
                        min_free_bytes: int = 0) -> dict[str, Any]:
    """Stream one raw submission to a private stage directory and return a DB record."""
    source = source.resolve()
    try:
        rel_path = source.relative_to(raw_root.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError(f"Source is outside raw root: {source}") from exc
    form_type_expected = canonical_form(form_folder)
    source_stat = source.stat()
    ensure_free_space(stage_root, min_free_bytes)
    source_digest = hashlib.sha256()
    folder_name = safe_filename(source.stem, "filing")
    stage = stage_root / f"{folder_name}.stage-{os.getpid()}-{__import__('threading').get_ident()}"
    if stage.exists():
        shutil.rmtree(stage)
    docs_dir = stage / "documents"
    extracted_dir = stage / "extracted"
    docs_dir.mkdir(parents=True)
    header_lines: list[str] = []
    document_records: list[dict[str, Any]] = []
    warnings: list[str] = []
    in_header = False
    header_done = False
    sec_document_closed = False
    in_document = False
    in_text = False
    doc_info: dict[str, str] = {}
    doc_order = 0
    output_handle = None
    output_path: Path | None = None
    output_tmp: Path | None = None
    output_rel: str | None = None
    doc_size = 0
    bytes_since_space_check = 0

    def start_document() -> None:
        nonlocal in_document, in_text, doc_info, doc_order
        in_document, in_text = True, False
        doc_info = {}
        doc_order += 1

    def start_text() -> None:
        nonlocal output_handle, output_path, output_tmp, output_rel, doc_size, bytes_since_space_check
        # Filesystem names use observed order, so duplicate SEC sequence values
        # in malformed submissions cannot overwrite an earlier document.
        safe_seq = f"{doc_order:04d}"
        filename = safe_filename(doc_info.get("FILENAME") or f"document_{doc_order:04d}.txt")
        output_rel = (Path("documents") / f"{safe_seq}_{filename}").as_posix()
        output_path = stage / output_rel
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_tmp = output_path.with_name(output_path.name + ".part")
        output_handle = output_tmp.open("wb")
        doc_size = 0
        bytes_since_space_check = 0

    def finish_document() -> None:
        nonlocal output_handle, in_text, in_document, output_path, output_tmp, output_rel, doc_size
        encoding_status = "no_text_block"
        size_bytes = 0
        digest = None
        extracted_count = 0
        extracted_bytes = 0
        extracted_members: list[dict[str, Any]] = []
        zip_error = None
        if output_handle is not None:
            output_handle.flush()
            os.fsync(output_handle.fileno())
            output_handle.close()
            output_handle = None
            assert output_tmp is not None and output_path is not None and output_rel is not None
            os.replace(output_tmp, output_path)
            encoding_status = _decode_uu_if_present(output_path)
            size_bytes = output_path.stat().st_size
            digest = sha256_file(output_path)
            try:
                is_zip = zipfile.is_zipfile(output_path) or output_path.suffix.lower() == ".zip"
            except OSError:
                is_zip = False
            if is_zip:
                archive_label = Path(doc_info.get("FILENAME") or output_path.name).stem
                extract_path = extracted_dir / f"{doc_order:04d}_{safe_filename(archive_label, 'archive')}"
                try:
                    extracted_count, extracted_bytes, extracted_members = extract_zip_safely(
                        output_path, extract_path, max_zip_bytes, max_zip_members, min_free_bytes
                    )
                    for member in extracted_members:
                        member["extracted_path"] = (extract_path.relative_to(stage) / str(member["relative_path"])).as_posix()
                    (stage / f"_archive_manifest_{doc_order:04d}.json").write_text(
                        json.dumps(extracted_members, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                    )
                except (OSError, zipfile.BadZipFile, ValueError) as exc:
                    zip_error = str(exc)
                    shutil.rmtree(extract_path, ignore_errors=True)
                    warnings.append(f"zip extraction failed for {doc_info.get('FILENAME')}: {exc}")
            document_records.append({
                "sequence": doc_info.get("SEQUENCE") or str(doc_order),
                "document_type": doc_info.get("TYPE"),
                "filename": doc_info.get("FILENAME"),
                "description": doc_info.get("DESCRIPTION"),
                "relative_path": output_rel,
                "size_bytes": size_bytes,
                "sha256": digest,
                "encoding_status": encoding_status,
                "zip_extracted_files": extracted_count,
                "zip_extracted_bytes": extracted_bytes,
                "zip_error": zip_error,
                "zip_members": extracted_members,
            })
        else:
            document_records.append({
                "sequence": doc_info.get("SEQUENCE") or str(doc_order),
                "document_type": doc_info.get("TYPE"), "filename": doc_info.get("FILENAME"),
                "description": doc_info.get("DESCRIPTION"), "relative_path": None,
                "size_bytes": 0, "sha256": None, "encoding_status": "no_text_block",
                "zip_extracted_files": 0, "zip_extracted_bytes": 0, "zip_error": None,
                "zip_members": [],
            })
        in_text = False
        in_document = False
        output_path = output_tmp = None
        output_rel = None

    try:
        with source.open("rb") as handle:
            for raw_line in handle:
                source_digest.update(raw_line)
                line = raw_line.decode("utf-8", errors="replace")
                stripped_upper = line.strip().upper()
                if stripped_upper == "</SEC-DOCUMENT>":
                    sec_document_closed = True
                if not header_done:
                    if stripped_upper.startswith("<SEC-HEADER>"):
                        in_header = True
                        header_lines.append(line)
                        continue
                    if in_header:
                        if stripped_upper == "</SEC-HEADER>":
                            header_lines.append(line)
                            in_header, header_done = False, True
                        else:
                            header_lines.append(line)
                        continue
                    if stripped_upper == "<DOCUMENT>":
                        header_done = True
                if in_document:
                    if in_text:
                        if stripped_upper == "</TEXT>":
                            if output_handle is not None:
                                output_handle.flush()
                            in_text = False
                        else:
                            if output_handle is not None:
                                # Filing payload bytes are preserved exactly; only
                                # header parsing uses replacement decoding.
                                output_handle.write(raw_line)
                                doc_size += len(raw_line)
                                bytes_since_space_check += len(raw_line)
                                if bytes_since_space_check >= 64 * 1024 * 1024:
                                    ensure_free_space(stage_root, min_free_bytes)
                                    bytes_since_space_check = 0
                        if stripped_upper == "</DOCUMENT>":
                            finish_document()
                        continue
                    if stripped_upper == "<TEXT>":
                        in_text = True
                        start_text()
                        continue
                    if stripped_upper == "</DOCUMENT>":
                        finish_document()
                        continue
                    parsed_field = parse_document_field(line)
                    if parsed_field:
                        key, value = parsed_field
                        doc_info[key] = value
                    continue
                if stripped_upper == "<DOCUMENT>":
                    start_document()
                    continue
        if in_document:
            warnings.append("EOF reached before </DOCUMENT>")
            finish_document()
        if output_handle is not None:
            output_handle.close()
            output_handle = None
        if not header_lines:
            warnings.append("SEC-HEADER block not found")
        if not sec_document_closed:
            warnings.append("SEC-DOCUMENT closing tag not found")
        if not document_records:
            raise ValueError("No <DOCUMENT> blocks found; source was not published")
        if not any(doc.get("relative_path") for doc in document_records):
            raise ValueError("No document <TEXT> bodies found; source was not published")
        for doc in document_records:
            if not doc.get("relative_path"):
                warnings.append(f"document sequence {doc.get('sequence')} has no <TEXT> body")
        parsed_header = parse_sec_header(header_lines)
        meta = parsed_header["filing"]
        actual_form = meta.get("form_type") or form_type_expected
        if actual_form.strip().upper() != form_type_expected.upper():
            raise ValueError(f"Form mismatch: folder={form_type_expected!r}, header={actual_form!r}")
        meta["form_type"] = form_type_expected
        meta["cik"] = (meta.get("cik") or "").strip().lstrip("0") or None
        meta["filed_as_of_date"] = _normalise_date(meta.get("filed_as_of_date"))
        meta["period_of_report"] = _normalise_date(meta.get("period_of_report"))
        meta["source_filename"] = source.name
        meta["source_relpath"] = rel_path
        meta["source_size_bytes"] = source_stat.st_size
        meta["source_mtime_ns"] = source_stat.st_mtime_ns
        meta["source_sha256"] = source_digest.hexdigest()
        meta["filing_folder"] = folder_name
        meta["sec_header_raw"] = "".join(header_lines)
        payload = {
            "filing": meta,
            "company_data": parsed_header["company_data"],
            "filing_values": parsed_header["filing_values"],
            "addresses": parsed_header["addresses"],
            "former_names": parsed_header["former_names"],
            "items": parsed_header["items"],
            "header_fields": parsed_header["header_fields"],
            "documents": document_records,
            "warnings": warnings,
            "delete_safe": not warnings and not any(d.get("zip_error") for d in document_records),
        }
        (stage / "_filing_record.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return {"stage_dir": str(stage), "payload": payload, "source": str(source), "status": "staged"}
    except BaseException:
        if output_handle is not None:
            output_handle.close()
        shutil.rmtree(stage, ignore_errors=True)
        raise


def load_filing_record(folder: Path) -> dict[str, Any]:
    return json.loads((folder / "_filing_record.json").read_text(encoding="utf-8"))
