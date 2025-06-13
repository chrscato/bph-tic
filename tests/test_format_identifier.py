from tic_mrf_scraper.utils.format_identifier import (
    detect_compression,
    identify_in_network,
)

def test_detect_compression():
    assert detect_compression('http://example.com/file.json') == 'json'
    assert detect_compression('http://example.com/file.json.gz') == 'json.gz'

def test_identify_in_network_structure():
    sample = {
        'in_network': [
            {
                'billing_code': '12345',
                'negotiated_rates': []
            }
        ]
    }
    report = identify_in_network(sample)
    assert report['is_valid_tic_mrf'] is True
    assert report['structure_type'] == 'in_network_rates'
    assert report['in_network_count'] == 1

