import os, shutil
import pyarrow.parquet as pq
from tic_mrf_scraper.write.parquet_writer import ParquetWriter

def test_parquet_writer(tmp_path):
    local = str(tmp_path / "test.parquet")
    writer = ParquetWriter(local_path=local, batch_size=2)
    writer.write({"a": 1})
    writer.write({"a": 2})
    writer.write({"a": 3})
    writer.close()

    # file should exist and contain 3 rows
    tbl = pq.read_table(local)
    assert tbl.num_rows == 3
