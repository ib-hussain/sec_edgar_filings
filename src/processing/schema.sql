PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS filings (
    filing_id TEXT PRIMARY KEY,
    accession_number TEXT,
    cik TEXT,
    company_name TEXT,
    form_type TEXT NOT NULL,
    filed_as_of_date TEXT,
    period_of_report TEXT,
    acceptance_datetime TEXT,
    public_document_count TEXT,
    sec_act TEXT,
    sec_file_number TEXT,
    film_number TEXT,
    sic TEXT,
    irs_number TEXT,
    state_of_incorporation TEXT,
    fiscal_year_end TEXT,
    source_filename TEXT NOT NULL,
    source_relpath TEXT NOT NULL UNIQUE,
    source_size_bytes INTEGER NOT NULL,
    source_mtime_ns INTEGER NOT NULL,
    source_sha256 TEXT NOT NULL,
    filing_folder TEXT NOT NULL,
    sec_header_raw TEXT NOT NULL,
    processed_at_utc TEXT NOT NULL,
    processing_status TEXT NOT NULL CHECK (processing_status IN ('complete', 'complete_with_warnings'))
);
CREATE INDEX IF NOT EXISTS idx_filings_cik_date ON filings(cik, filed_as_of_date);
CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings(accession_number);
CREATE INDEX IF NOT EXISTS idx_filings_form_date ON filings(form_type, filed_as_of_date);

CREATE TABLE IF NOT EXISTS header_fields (
    header_field_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    section TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT NOT NULL,
    occurrence INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_header_fields_lookup ON header_fields(filing_id, section, field_name);

CREATE TABLE IF NOT EXISTS company_data (
    filing_id TEXT PRIMARY KEY REFERENCES filings(filing_id) ON DELETE CASCADE,
    conformed_name TEXT,
    cik TEXT,
    sic TEXT,
    irs_number TEXT,
    state_of_incorporation TEXT,
    fiscal_year_end TEXT
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    address_kind TEXT NOT NULL,
    street1 TEXT,
    street2 TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    phone TEXT,
    UNIQUE(filing_id, address_kind)
);

CREATE TABLE IF NOT EXISTS former_names (
    former_name_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    former_name TEXT,
    date_of_name_change TEXT,
    UNIQUE(filing_id, ordinal)
);

CREATE TABLE IF NOT EXISTS filing_items (
    item_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    item_information TEXT NOT NULL,
    UNIQUE(filing_id, ordinal)
);

CREATE TABLE IF NOT EXISTS documents (
    document_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    sequence TEXT,
    document_type TEXT,
    filename TEXT,
    description TEXT,
    relative_path TEXT,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT,
    encoding_status TEXT NOT NULL,
    zip_extracted_files INTEGER NOT NULL DEFAULT 0,
    zip_extracted_bytes INTEGER NOT NULL DEFAULT 0,
    zip_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_documents_filing_sequence ON documents(filing_id, sequence);

CREATE TABLE IF NOT EXISTS archive_members (
    archive_member_id INTEGER PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
    member_name TEXT NOT NULL,
    extracted_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    UNIQUE(document_id, member_name)
);

CREATE TABLE IF NOT EXISTS processing_warnings (
    warning_id INTEGER PRIMARY KEY,
    filing_id TEXT NOT NULL REFERENCES filings(filing_id) ON DELETE CASCADE,
    warning TEXT NOT NULL
);
