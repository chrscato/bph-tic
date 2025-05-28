#!/usr/bin/env python3

import tempfile
import json
import os
from pathlib import Path

# Test data
test_records = [
    {
        "billing_code": "99213",
        "negotiated_rates": [{"negotiated_price": 123.45}],
        "plan_id": "test_plan"
    },
    {
        "billing_code": "99201", 
        "negotiated_rates": [{"negotiated_price": 100.00}],
        "plan_id": "test_plan"
    }
]

def test_pipeline():
    from tic_mrf_scraper.transform.normalize import normalize_record
    from tic_mrf_scraper.write.parquet_writer import ParquetWriter
    
    # Test normalization
    cpt_whitelist = {"99213", "99201"}
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "test_output.parquet")
        writer = ParquetWriter(output_path, batch_size=2)
        
        for record in test_records:
            normalized = normalize_record(record, cpt_whitelist, "TEST_PAYER")
            if normalized:
                writer.write(normalized)
                print(f"Processed: {normalized}")
        
        writer.close()
        
        # Verify file was created
        if os.path.exists(output_path):
            print(f"✅ Success! Output written to {output_path}")
            
            # Read back and verify
            import pyarrow.parquet as pq
            table = pq.read_table(output_path)
            print(f"Records in output: {table.num_rows}")
            print(f"Columns: {table.column_names}")
        else:
            print("❌ No output file created")

if __name__ == "__main__":
    test_pipeline() 