from unittest.mock import MagicMock, patch
from tic_mrf_scraper.payers import PayerHandler, register_handler, get_handler
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
import gzip, json
from io import BytesIO


def make_gzipped(data):
    bio = BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="w") as gz:
        gz.write(json.dumps(data).encode())
    bio.seek(0)
    return bio.getvalue()


def test_handler_registry_lookup():
    handler = get_handler("centene")
    assert isinstance(handler, PayerHandler)


def test_parse_in_network_hook():
    class Dummy(PayerHandler):
        def parse_in_network(self, record):
            record["extra"] = True
            return [record]

    dummy = Dummy()

    mrf = {
        "in_network": [
            {
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_groups": [{"providers": [{"npi": "1"}]}],
                        "negotiated_prices": [{"negotiated_rate": 10, "service_code": ["11"]}]
                    }
                ]
            }
        ]
    }

    with patch("tic_mrf_scraper.stream.parser.fetch_url", return_value=make_gzipped(mrf)):
        records = list(stream_parse_enhanced("mock", "TEST", handler=dummy))

    assert records[0]["extra"] is True
