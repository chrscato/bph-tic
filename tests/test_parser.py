"""Tests for the MRF parser module."""

import json
from io import BytesIO
from unittest.mock import patch, MagicMock

import pytest
import requests

from tic_mrf_scraper.stream.parser import parse_mrf

@pytest.fixture
def mock_response():
    """Create a mock response with sample MRF data."""
    data = [
        {
            "cpt_code": "99201",
            "provider_npi": "1234567890",
            "negotiated_rate": 100.00
        },
        {
            "cpt_code": "99202",
            "provider_npi": "0987654321",
            "negotiated_rate": 150.00
        }
    ]
    
    # Create a gzipped response
    import gzip
    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
        f.write(json.dumps(data).encode())
    buffer.seek(0)
    
    response = MagicMock(spec=requests.Response)
    response.raw = buffer
    response.raise_for_status = MagicMock()
    return response

def test_parse_mrf(mock_response):
    """Test parsing MRF data from a URL."""
    with patch('tic_mrf_scraper.stream.parser.fetch_url', return_value=mock_response):
        records = list(parse_mrf("http://example.com/mrf.json.gz"))
        
        assert len(records) == 2
        assert records[0]["cpt_code"] == "99201"
        assert records[1]["cpt_code"] == "99202"
        assert records[0]["negotiated_rate"] == 100.00
        assert records[1]["negotiated_rate"] == 150.00

def test_parse_mrf_error():
    """Test error handling when parsing fails."""
    with patch('tic_mrf_scraper.stream.parser.fetch_url', side_effect=requests.RequestException):
        with pytest.raises(requests.RequestException):
            list(parse_mrf("http://example.com/mrf.json.gz")) 