manifest_maker.1.py
python manifest_maker.1.py manifest --start-year 2000 --end-year 2026

filtering_companies.2.py
python filtering_companies.2.py --input data/sec_edgar/manifests/filings_manifest_2000_2026.csv  --output  data/sec_edgar/manifests/filtered_manifest_2000_2026.csv --tickers data/main_tickers.csv

downloader.3.py
python downloader.3.py --workers 8 --max-pending-futures 256 --min-free-gb 12 --max-retries 8 --force

filter_download.4.py
python filter_download.4.py

DEFAULT_FORM_WEIGHTS = {
    "8-K": 0.50,
    "10-Q": 0.27,
    "10-K": 0.09,
    "DEF 14A": 0.14,
}
