#!/usr/bin/env python3
"""Inspect the structure of a JSON file from a Transparency in Coverage endpoint."""

import argparse
import json
import logging
from typing import Any, Dict

import requests

from tic_mrf_scraper.utils import identify_index, identify_in_network


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_json(url: str) -> Dict[str, Any]:
    """Fetch a JSON URL and return the parsed object."""
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect TiC JSON structure")
    parser.add_argument("--url", required=True, help="URL of the JSON file")
    args = parser.parse_args()

    url = args.url
    logger.info("fetching", extra={"url": url})

    data = fetch_json(url)

    # Determine file type using helper utilities
    index_info = identify_index(url)
    in_net_info = identify_in_network(data)
    logger.info("index_info", extra=index_info)
    logger.info("in_network_info", extra=in_net_info)

    if in_net_info.get("in_network_count", 0) > 0 and "in_network" in data:
        sample = data.get("in_network", [])[:2]
        logger.info("sample_in_network_count", extra={"count": len(sample)})
        for i, item in enumerate(sample, start=1):
            logger.info(
                "sample_item",
                extra={
                    "index": i,
                    "billing_code": item.get("billing_code"),
                    "keys": list(item.keys()),
                },
            )
    else:
        keys = list(data.keys()) if isinstance(data, dict) else []
        logger.info("file_keys", extra={"keys": keys})
        if isinstance(data, dict) and "reporting_structure" in data:
            rs = data["reporting_structure"]
            logger.info(
                "reporting_structure",
                extra={"entries": len(rs)},
            )

    logger.info("done")


if __name__ == "__main__":
    main()
