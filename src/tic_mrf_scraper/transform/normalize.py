"""Module for normalizing MRF records."""

from typing import Dict, Any, Optional, Set

def normalize_record(record: Dict[str, Any], cpt_whitelist: Set[str], payer: str) -> Optional[Dict[str, Any]]:
    """Normalize a single MRF record.
    
    Args:
        record: Raw MRF record
        cpt_whitelist: Set of allowed CPT codes
        payer: Payer name
        
    Returns:
        Normalized record or None if invalid
    """
    # Extract billing code (try multiple possible field names)
    billing_code = record.get("billing_code") or record.get("cpt_code")
    if not billing_code or billing_code not in cpt_whitelist:
        return None
        
    # Extract negotiated rate (handle nested structure)
    rate = None
    if "negotiated_rates" in record and record["negotiated_rates"]:
        rate = record["negotiated_rates"][0].get("negotiated_price")
    elif "negotiated_rate" in record:
        rate = record["negotiated_rate"]
        
    if rate is None:
        return None
        
    return {
        "service_code": billing_code,  # Match test expectations
        "negotiated_rate": float(rate),
        "payer": payer
    }
