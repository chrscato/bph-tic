import pytest
from unittest.mock import patch, MagicMock
import yaml
from tic_mrf_scraper.__main__ import main, load_config

def test_load_config():
    """Test loading enhanced configuration options."""
    config = {
        "endpoints": {
            "centene_fidelis": "https://example.com/index.json"
        },
        "cpt_whitelist": ["99213", "99214"],
        "processing": {
            "file_types": ["in_network_rates"],
            "max_files_per_payer": 5,
            "max_records_per_file": 100000,
            "batch_size": 1000
        },
        "logging": {
            "level": "INFO",
            "structured": True
        },
        "s3": {
            "bucket": "test-bucket",
            "prefix": "tic-mrf-enhanced"
        }
    }
    
    with patch("builtins.open", MagicMock()) as mock_open:
        mock_open.return_value.__enter__.return_value.read.return_value = yaml.dump(config)
        loaded_config = load_config("config.yaml")
        
        # Verify endpoints
        assert "centene_fidelis" in loaded_config["endpoints"]
        assert loaded_config["endpoints"]["centene_fidelis"] == "https://example.com/index.json"
        
        # Verify CPT whitelist
        assert "99213" in loaded_config["cpt_whitelist"]
        assert "99214" in loaded_config["cpt_whitelist"]
        
        # Verify processing options
        assert loaded_config["processing"]["file_types"] == ["in_network_rates"]
        assert loaded_config["processing"]["max_files_per_payer"] == 5
        assert loaded_config["processing"]["max_records_per_file"] == 100000
        assert loaded_config["processing"]["batch_size"] == 1000
        
        # Verify logging options
        assert loaded_config["logging"]["level"] == "INFO"
        assert loaded_config["logging"]["structured"] is True
        
        # Verify S3 settings
        assert loaded_config["s3"]["bucket"] == "test-bucket"
        assert loaded_config["s3"]["prefix"] == "tic-mrf-enhanced"

@patch("tic_mrf_scraper.__main__.load_config")
@patch("tic_mrf_scraper.__main__.list_mrf_blobs_enhanced")
@patch("tic_mrf_scraper.__main__.process_mrf_file")
def test_main_with_enhanced_options(mock_process, mock_list_blobs, mock_load_config):
    """Test main function with enhanced processing options."""
    # Mock configuration
    config = {
        "endpoints": {
            "centene_fidelis": "https://example.com/index.json"
        },
        "cpt_whitelist": ["99213"],
        "processing": {
            "file_types": ["in_network_rates"],
            "max_files_per_payer": 2,
            "max_records_per_file": 1000,
            "batch_size": 100
        }
    }
    mock_load_config.return_value = config
    
    # Mock blob listing
    mock_list_blobs.return_value = [
        {"url": "https://example.com/file1.json", "type": "in_network_rates"},
        {"url": "https://example.com/file2.json", "type": "in_network_rates"},
        {"url": "https://example.com/file3.json", "type": "in_network_rates"}
    ]
    
    # Mock processing
    mock_process.return_value = {"processed": 100, "skipped": 0}
    
    # Run main with test arguments
    with patch("sys.argv", ["script.py", "--config", "config.yaml", "--output", "output"]):
        main()
    
    # Verify blob listing was called with correct options
    mock_list_blobs.assert_called_once()
    call_args = mock_list_blobs.call_args[1]
    assert call_args["file_types"] == ["in_network_rates"]
    
    # Verify processing was called for each blob (limited by max_files_per_payer)
    assert mock_process.call_count == 2  # Should only process 2 files due to max_files_per_payer
    
    # Verify processing calls had correct arguments
    for call in mock_process.call_args_list:
        args, kwargs = call
        assert kwargs["max_records"] == 1000
        assert kwargs["batch_size"] == 100

@patch("tic_mrf_scraper.__main__.load_config")
@patch("tic_mrf_scraper.__main__.analyze_endpoint")
def test_main_analyze_only(mock_analyze, mock_load_config):
    """Test main function in analyze-only mode."""
    # Mock configuration
    config = {
        "endpoints": {
            "centene_fidelis": "https://example.com/index.json"
        },
        "processing": {
            "file_types": ["in_network_rates"]
        }
    }
    mock_load_config.return_value = config
    
    # Mock analysis
    mock_analyze.return_value = {
        "total_files": 10,
        "file_types": {"in_network_rates": 8, "provider_references": 2}
    }
    
    # Run main with analyze-only flag
    with patch("sys.argv", ["script.py", "--config", "config.yaml", "--analyze-only"]):
        main()
    
    # Verify analysis was called
    mock_analyze.assert_called_once()
    call_args = mock_analyze.call_args[1]
    assert call_args["file_types"] == ["in_network_rates"] 