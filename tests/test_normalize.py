from tic_mrf_scraper.transform.normalize import normalize_record

def test_normalize_record_filters_and_maps():
    raw = {
        "billing_code": "99213",
        "plan_id": "planA",
        "negotiated_rates": [{"negotiated_price": 123.45}],
        "service_area": {"state": "VA"},
        "negotiation_arrangement": "2025-01-01",
    }
    rec = normalize_record(raw, {"99213"}, payer="PAYER1")
    assert rec["service_code"] == "99213"
    assert rec["negotiated_rate"] == 123.45
    assert rec["payer"] == "PAYER1"

    rec2 = normalize_record(raw, {"00000"}, payer="PAYER1")
    assert rec2 is None
