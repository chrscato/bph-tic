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
    # Extract CPT code
    cpt = record.get("billing_code")
    if not cpt or cpt not in cpt_whitelist:
        return None
        
    # Extract provider NPI
    npi = record.get("npi")
    if not npi:
        return None
        
    # Extract negotiated rate
    rate = record.get("negotiated_rate")
    if not rate:
        return None
        
    return {
        "cpt_code": cpt,
        "provider_npi": npi,
        "negotiated_rate": float(rate),
        "payer": payer
    }
