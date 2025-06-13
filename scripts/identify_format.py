#!/usr/bin/env python3
"""Identify the format of an index or MRF data file."""

import argparse
import json
import gzip
from io import BytesIO
import logging

import requests

from tic_mrf_scraper.utils import (
    detect_compression,
    identify_index,
    identify_in_network,
)


def fetch_json(url: str) -> dict:
    """Fetch and return JSON content from a URL handling gzip."""
    resp = requests.get(url)
    resp.raise_for_status()
    content = resp.content
    if url.lower().endswith(".gz"):
        with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
            return json.load(gz)
    return json.loads(content.decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Identify index or MRF format")
    parser.add_argument("url", help="Index or data file URL")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("identify_format")

    url = args.url
    report = {"url": url}

    # Detect compression based on the URL
    report["compression"] = detect_compression(url)

    # Analyze as if it were an index
    index_info = identify_index(url)
    report["index"] = index_info

    # If index analysis produced sample URLs, inspect one
    sample_urls = index_info.get("sample_urls") if isinstance(index_info, dict) else None
    if sample_urls:
        sample_url = sample_urls[0]
        logger.info("Inspecting sample MRF: %s", sample_url)
        try:
            data = fetch_json(sample_url)
            report["in_network"] = identify_in_network(data)
        except Exception as exc:
            logger.warning("Failed to analyze sample MRF: %s", exc)
            report["in_network"] = {"error": str(exc)}

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
