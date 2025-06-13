import pytest
from unittest.mock import MagicMock, patch

from production_etl_pipeline import ETLConfig, ProductionETLPipeline


def make_config(**overrides):
    base = dict(
        payer_endpoints={"payer": "http://example.com/index.json"},
        cpt_whitelist=["99213"],
        batch_size=10,
        parallel_workers=1,
        max_files_per_payer=None,
        max_records_per_file=None,
        local_output_dir="/tmp",
        s3_bucket=None,
    )
    base.update(overrides)
    return ETLConfig(**base)


def dummy_files(n):
    return [
        {"url": f"http://example.com/file{i}.json", "type": "in_network_rates", "plan_name": f"plan{i}"}
        for i in range(n)
    ]


def dummy_records(n):
    for _ in range(n):
        yield {"billing_code": "99213"}


def test_process_payer_respects_file_limit():
    config = make_config(max_files_per_payer=2)
    pipeline = ProductionETLPipeline(config)

    handler = MagicMock()
    handler.list_mrf_files.return_value = dummy_files(5)

    with patch("production_etl_pipeline.get_handler", return_value=handler):
        with patch.object(pipeline, "create_payer_record", return_value="p1"):
            with patch.object(
                pipeline,
                "process_mrf_file_enhanced",
                return_value={"records_extracted": 0, "records_validated": 0},
            ) as proc:
                stats = pipeline.process_payer("payer", "http://example.com/index.json")

    assert proc.call_count == 2
    assert stats["files_processed"] == 2


def test_process_mrf_respects_record_limit():
    config = make_config(max_records_per_file=5)
    pipeline = ProductionETLPipeline(config)

    with patch("production_etl_pipeline.stream_parse_enhanced", return_value=dummy_records(10)):
        with patch(
            "production_etl_pipeline.normalize_tic_record",
            return_value={
                "service_code": "99213",
                "negotiated_rate": 1,
                "provider_npi": [],
                "provider_tin": "",
                "provider_name": "",
            },
        ):
            with patch.object(pipeline, "create_rate_record", return_value={"organization_uuid": "o", "provider_network": {"npi_list": []}}):
                with patch.object(pipeline.validator, "validate_rate_record", return_value={"is_validated": True}):
                    with patch.object(pipeline, "create_organization_record", return_value={}):
                        with patch.object(pipeline, "create_provider_records", return_value=[]):
                            with patch.object(pipeline, "write_batches_to_s3", return_value={"files_uploaded": 0}):
                                stats = pipeline.process_mrf_file_enhanced(
                                    "p",
                                    "payer",
                                    {"url": "http://file", "plan_name": "plan"},
                                    MagicMock(),
                                    1,
                                    1,
                                )

    assert stats["records_extracted"] == 5

