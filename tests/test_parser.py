import gzip, json
from io import BytesIO
import ijson
from tic_mrf_scraper.stream.parser import stream_parse, stream_parse_enhanced
from unittest.mock import patch, MagicMock

def make_gzipped(items):
    bio = BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="w") as gf:
        gf.write(json.dumps(items).encode())
    bio.seek(0)
    return bio

@patch("requests.get")
def test_stream_parse(mock_get):
    """Test legacy parser with simple structure."""
    items = [{"billing_code": "99213"}, {"billing_code": "00000"}]
    mock_resp = MagicMock()
    mock_resp.raw = make_gzipped(items)
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    out = list(stream_parse("http://x"))
    assert out == items

@patch("requests.get")
def test_parse_nested_tic_structure(mock_get):
    """Test parsing realistic TiC MRF nested structure."""
    nested_data = {
        "reporting_entity_name": "Test Payer",
        "in_network": [
            {
                "billing_code": "99213",
                "billing_code_type": "CPT",
                "description": "Office visit",
                "negotiated_rates": [
                    {
                        "provider_groups": [
                            {
                                "providers": [
                                    {"npi": "1234567890", "provider_group_name": "Test Clinic"}
                                ]
                            }
                        ],
                        "negotiated_prices": [
                            {
                                "negotiated_rate": 150.00,
                                "service_code": ["11", "22"],
                                "billing_class": "professional",
                                "negotiated_type": "fee schedule"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    mock_resp = MagicMock()
    mock_resp.raw = make_gzipped(nested_data)
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp
    
    # Test with enhanced parser
    records = list(stream_parse_enhanced("mock_url", "TEST_PAYER"))
    
    # Verify we got the expected number of records
    assert len(records) == 1
    
    # Verify the record structure
    record = records[0]
    assert record["billing_code"] == "99213"
    assert record["billing_code_type"] == "CPT"
    assert record["description"] == "Office visit"
    assert record["negotiated_rate"] == 150.00
    assert record["service_codes"] == ["11", "22"]
    assert record["billing_class"] == "professional"
    assert record["negotiated_type"] == "fee schedule"
    assert record["provider_npi"] == "1234567890"
    assert record["provider_name"] == "Test Clinic"

@patch("requests.get")
def test_parse_multiple_providers(mock_get):
    """Test parsing MRF with multiple providers per rate."""
    nested_data = {
        "reporting_entity_name": "Test Payer",
        "in_network": [
            {
                "billing_code": "99214",
                "billing_code_type": "CPT",
                "description": "Complex visit",
                "negotiated_rates": [
                    {
                        "provider_groups": [
                            {
                                "providers": [
                                    {"npi": "1111111111", "provider_group_name": "Clinic A"},
                                    {"npi": "2222222222", "provider_group_name": "Clinic B"}
                                ]
                            }
                        ],
                        "negotiated_prices": [
                            {
                                "negotiated_rate": 200.00,
                                "service_code": ["33"],
                                "billing_class": "professional",
                                "negotiated_type": "fee schedule"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    mock_resp = MagicMock()
    mock_resp.raw = make_gzipped(nested_data)
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp
    
    # Test with enhanced parser
    records = list(stream_parse_enhanced("mock_url", "TEST_PAYER"))
    
    # Should get one record per provider
    assert len(records) == 2
    
    # Verify first provider record
    record1 = records[0]
    assert record1["billing_code"] == "99214"
    assert record1["provider_npi"] == "1111111111"
    assert record1["provider_name"] == "Clinic A"
    assert record1["negotiated_rate"] == 200.00
    
    # Verify second provider record
    record2 = records[1]
    assert record2["billing_code"] == "99214"
    assert record2["provider_npi"] == "2222222222"
    assert record2["provider_name"] == "Clinic B"
    assert record2["negotiated_rate"] == 200.00

@patch("requests.get")
def test_parse_with_provider_references(mock_get):
    """Test parsing MRF with provider references."""
    # Mock provider reference data
    provider_ref_data = {
        "provider_references": {
            "1234567890": {
                "npi": "1234567890",
                "name": "Test Clinic",
                "tin": "123456789"
            }
        }
    }
    
    # Mock MRF data
    mrf_data = {
        "reporting_entity_name": "Test Payer",
        "in_network": [
            {
                "billing_code": "99213",
                "billing_code_type": "CPT",
                "description": "Office visit",
                "negotiated_rates": [
                    {
                        "provider_groups": [
                            {
                                "providers": [
                                    {"npi": "1234567890"}
                                ]
                            }
                        ],
                        "negotiated_prices": [
                            {
                                "negotiated_rate": 150.00,
                                "service_code": ["11"],
                                "billing_class": "professional",
                                "negotiated_type": "fee schedule"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Mock responses
    mock_resp = MagicMock()
    mock_resp.raw = make_gzipped(mrf_data)
    mock_resp.raise_for_status.return_value = None
    
    mock_provider_resp = MagicMock()
    mock_provider_resp.raw = make_gzipped(provider_ref_data)
    mock_provider_resp.raise_for_status.return_value = None
    
    # Set up mock to return different responses for different URLs
    def mock_get_side_effect(url, *args, **kwargs):
        if "provider_reference" in url:
            return mock_provider_resp
        return mock_resp
    
    mock_get.side_effect = mock_get_side_effect
    
    # Test with enhanced parser
    records = list(stream_parse_enhanced("mock_url", "TEST_PAYER", "mock_provider_ref_url"))
    
    # Verify record structure
    assert len(records) == 1
    record = records[0]
    assert record["billing_code"] == "99213"
    assert record["provider_npi"] == "1234567890"
    assert record["provider_name"] == "Test Clinic"
    assert record["provider_tin"] == "123456789"
    assert record["negotiated_rate"] == 150.00
