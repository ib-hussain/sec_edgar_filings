# SEC filing extraction — phase one

This bundle implements the first structural stage for the four exact filing forms: `8-K`, `10-K`, `10-Q`, and `DEF 14A` (stored as `DEF_14A` on disk). It reads raw SEC submission `.txt` files from `raw/<year>/<form>/`, writes one directory per filing under `processed/<form>/`, and creates one normalized SQLite database per form.

## Run

From the repository root after copying these files into place:

```bash
python extract_structure.5.py --workers 0
```

`--workers 0` uses all CPU threads reported by Python. To test one form or a smaller worker count:

```bash
python extract_structure.5.py --forms 8-K 10-K --workers 4
```

Raw filings are retained by default. To delete a raw `.txt` only after its filing folder has been published and the corresponding database transaction commits:

```bash
python extract_structure.5.py --workers 0 --delete-raw-after-success
```

The scripts never delete files from `processed/`. ZIP attachments are retained as document files and extracted under the filing's `extracted/` directory. ZIP member path traversal and symlinks are rejected. Default ZIP extraction limits are 4 GiB expanded bytes per archive and 100,000 members; both are configurable.

## Output shape

```text
processed/
├── 10-K/
│   ├── 10-K_forms.db
│   └── <source-filing-stem>/
│       ├── _filing_record.json
│       ├── documents/<sequence>_<source-filename>
│       └── extracted/<sequence>_<archive-name>/...
├── 10-Q/10-Q_forms.db
├── 8-K/8-K_forms.db
├── DEF_14A/DEF_14A_forms.db
└── logs/phase1_{events,errors}.jsonl
```

SQLite stores filing identity and source provenance, the original SEC header, repeated header fields and 8-K item information, filer data, business/mailing addresses, former names, document metadata, attachment hashes, ZIP extraction results, archive-member paths/hashes, and warnings. Each archive gets a sibling `_archive_manifest_<sequence>.json` in the filing folder. `processed/schema.sql` documents the relational schema. `_filing_record.json` is a recovery marker used if the process stops after publishing a filing folder but before committing its SQLite transaction.

## Resume and safety

- Completed source files are skipped using their saved path, size, and modification time and by checking that the filing folder exists.
- `--verify-hashes` rechecks SHA-256 before skipping existing files. Hash verification is always done before an existing raw file can be deleted.
- Partial staging directories are isolated below `processed/<form>/.staging/`; raw source files are never edited.
- A 10 GiB free-space reserve is checked before work and while writing large document/ZIP payloads; configure it with `--min-free-gb` (set to `0` to disable).
- A rerun recovers a completed folder/database write interrupted between filesystem publication and SQLite commit.
- Changed source content is never allowed to overwrite a different existing filing folder. The raw file is retained and the conflict is logged.
- Incomplete submissions, filings with no document text, and ZIP extraction failures are marked with warnings; even with the deletion flag, those raw submissions are retained.
- Processing errors are logged per filing; successful filings remain committed and a rerun continues with failed items.
- SQLite uses foreign keys, WAL, full synchronous commits, and a single writer per form database.

## Scope

This is only source decomposition and first-level metadata capture. It deliberately does not clean or summarize filing text, select a primary narrative document, extract sections, balance dates/forms, or prepare model inputs. Those choices belong to later engineering stages and should use the traceable filing/document IDs created here.
