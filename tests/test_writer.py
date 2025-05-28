"""Tests for the Parquet writer module."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from tic_mrf_scraper.write.parquet_writer import ParquetWriter

@pytest.fixture
def output_dir(tmp_path):
    """Create a temporary output directory."""
    return tmp_path / "output"

@pytest.fixture
def writer(output_dir):
    """Create a ParquetWriter instance."""
    return ParquetWriter(output_dir, batch_size=2)

def test_write_single_record(writer, output_dir):
    """Test writing a single record."""
    record = {
        "cpt_code": "99201",
        "provider_npi": "1234567890",
        "negotiated_rate": 100.00
    }
    
    writer.write(record)
    writer.close()
    
    # Check that no files were written (batch size not reached)
    assert len(list(output_dir.glob("*.parquet"))) == 0

def test_write_batch(writer, output_dir):
    """Test writing a full batch."""
    records = [
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
    
    for record in records:
        writer.write(record)
    
    # Check that a file was written
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    
    # Read and verify the data
    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 2
    assert table.column("cpt_code").to_pylist() == ["99201", "99202"]
    assert table.column("provider_npi").to_pylist() == ["1234567890", "0987654321"]
    assert table.column("negotiated_rate").to_pylist() == [100.00, 150.00]

def test_write_multiple_batches(writer, output_dir):
    """Test writing multiple batches."""
    records = [
        {
            "cpt_code": "99201",
            "provider_npi": "1234567890",
            "negotiated_rate": 100.00
        },
        {
            "cpt_code": "99202",
            "provider_npi": "0987654321",
            "negotiated_rate": 150.00
        },
        {
            "cpt_code": "99203",
            "provider_npi": "1122334455",
            "negotiated_rate": 200.00
        }
    ]
    
    for record in records:
        writer.write(record)
    writer.close()
    
    # Check that two files were written
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) == 2
    
    # Read and verify all data
    all_data = []
    for file in parquet_files:
        table = pq.read_table(file)
        all_data.extend(table.to_pylist())
    
    assert len(all_data) == 3
    assert all_data[0]["cpt_code"] == "99201"
    assert all_data[1]["cpt_code"] == "99202"
    assert all_data[2]["cpt_code"] == "99203"

def test_write_error(writer, output_dir):
    """Test error handling during write."""
    with patch('pyarrow.Table.from_pylist', side_effect=Exception("Test error")):
        with pytest.raises(Exception):
            writer.write({"cpt_code": "99201"})
            writer.close() 