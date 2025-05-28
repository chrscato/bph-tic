# TIC MRF Scraper

A Python-based tool for scraping and processing Machine Readable Files (MRFs) from healthcare payer websites.

## Features

- Discovers and lists blob URLs from payer websites
- Streams and parses MRF data using efficient JSON parsing
- Applies CPT code whitelisting and schema normalization
- Writes data to Parquet format with S3 upload support
- Includes retry logic and structured logging
- Docker support for reproducible environments

## Installation

```bash
# Using Poetry
poetry install

# Or using pip
pip install -r requirements.txt
```

## Configuration

Edit `config.yaml` to set:
- Endpoint URLs
- AWS/S3 credentials
- CPT code whitelist
- Logging levels

## Usage

```bash
# Run directly
python -m tic_mrf_scraper

# Or using Docker
docker build -t tic-mrf-scraper .
docker run tic-mrf-scraper
```

See `examples/simple_run.sh` for a complete example.

## Development

```bash
# Run tests
pytest

# Lint code
flake8 src tests
```

## License

MIT 