manifest_maker.py
python manifest_maker.py manifest --start-year 2000 --end-year 2026

filtering_companies.py
python filtering_companies.py --input data/sec_edgar/manifests/filings_manifest_2000_2026.csv  --output  data/sec_edgar/manifests/filtered_manifest_2000_2026.csv --tickers data/main_tickers.csv

downloader.py
python downloader.py --workers 8 --max-pending-futures 256 --min-free-gb 12 --max-retries 8 --force

filter_download.py