from tic_mrf_scraper.transform.normalize import normalize_record, normalize_tic_record

def test_normalize_record_filters_and_maps():
    """Test legacy normalization with simple structure."""
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

def test_normalize_tic_record_full_structure():
    """Test enhanced normalization with full TiC structure."""
    raw = {
        "billing_code": "99213",
        "billing_code_type": "CPT",
        "description": "Office visit",
        "negotiated_rate": 150.00,
        "service_codes": ["11", "22"],
        "billing_class": "professional",
        "negotiated_type": "fee schedule",
        "expiration_date": "2025-12-31",
        "provider_npi": "1234567890",
        "provider_name": "Test Clinic",
        "provider_tin": "123456789"
    }
    
    rec = normalize_tic_record(raw, {"99213"}, payer="TEST_PAYER")
    
    # Verify all fields are correctly mapped
    assert rec["service_code"] == "99213"
    assert rec["billing_code_type"] == "CPT"
    assert rec["description"] == "Office visit"
    assert rec["negotiated_rate"] == 150.00
    assert rec["service_codes"] == ["11", "22"]
    assert rec["billing_class"] == "professional"
    assert rec["negotiated_type"] == "fee schedule"
    assert rec["expiration_date"] == "2025-12-31"
    assert rec["provider_npi"] == "1234567890"
    assert rec["provider_name"] == "Test Clinic"
    assert rec["provider_tin"] == "123456789"
    assert rec["payer"] == "TEST_PAYER"

def test_normalize_tic_record_missing_fields():
    """Test enhanced normalization with missing optional fields."""
    raw = {
        "billing_code": "99213",
        "negotiated_rate": 150.00
    }
    
    rec = normalize_tic_record(raw, {"99213"}, payer="TEST_PAYER")
    
    # Verify required fields
    assert rec["service_code"] == "99213"
    assert rec["negotiated_rate"] == 150.00
    assert rec["payer"] == "TEST_PAYER"
    
    # Verify optional fields have default values
    assert rec["billing_code_type"] == ""
    assert rec["description"] == ""
    assert rec["service_codes"] == []
    assert rec["billing_class"] == ""
    assert rec["negotiated_type"] == ""
    assert rec["expiration_date"] == ""
    assert rec["provider_npi"] is None
    assert rec["provider_name"] is None
    assert rec["provider_tin"] is None

def test_normalize_tic_record_invalid():
    """Test enhanced normalization with invalid records."""
    # Missing required fields
    raw1 = {
        "billing_code": "99213"
        # Missing negotiated_rate
    }
    assert normalize_tic_record(raw1, {"99213"}, payer="TEST_PAYER") is None
    
    # Invalid billing code
    raw2 = {
        "billing_code": "00000",
        "negotiated_rate": 150.00
    }
    assert normalize_tic_record(raw2, {"99213"}, payer="TEST_PAYER") is None
    
    # Invalid negotiated rate
    raw3 = {
        "billing_code": "99213",
        "negotiated_rate": None
    }
    assert normalize_tic_record(raw3, {"99213"}, payer="TEST_PAYER") is None
