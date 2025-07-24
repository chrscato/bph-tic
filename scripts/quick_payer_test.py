#!/usr/bin/env python3
"""
Quick Payer Test - Rapid testing and validation for new payer handlers.

This script provides quick testing capabilities for developing new payer handlers
without running the full ETL pipeline.

Usage:
    python scripts/quick_payer_test.py --payer-name "new_payer" --index-url "https://..."
"""

import os
import sys
import json
import argparse
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record


class QuickPayerTest:
    """Quick testing utility for new payer handlers."""
    
    def __init__(self):
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging for quick testing."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def test_payer_handler(self, payer_name: str, index_url: str, 
                          max_files: int = 2, max_records: int = 50) -> Dict[str, Any]:
        """
        Quick test of a payer handler with sample data.
        
        Args:
            payer_name: Name of the payer
            index_url: URL to the payer's MRF index
            max_files: Maximum number of files to test
            max_records: Maximum number of records per file to test
        
        Returns:
            Test results summary
        """
        self.logger.info(f"🧪 Quick testing payer: {payer_name}")
        
        results = {
            "payer_name": payer_name,
            "test_timestamp": datetime.now().isoformat(),
            "files_tested": 0,
            "total_records": 0,
            "successful_records": 0,
            "errors": [],
            "warnings": [],
            "sample_output": [],
            "processing_rate": 0.0
        }
        
        try:
            # Get handler
            handler = get_handler(payer_name)
            self.logger.info(f"✅ Handler loaded: {type(handler).__name__}")
            
            # Get MRF files
            mrf_files = list_mrf_blobs_enhanced(index_url)
            test_files = mrf_files[:max_files]
            
            self.logger.info(f"📁 Testing {len(test_files)} files from {len(mrf_files)} total")
            
            import time
            start_time = time.time()
            
            for i, file_info in enumerate(test_files):
                self.logger.info(f"Testing file {i+1}/{len(test_files)}: {file_info.get('name', 'unknown')}")
                
                try:
                    file_records = 0
                    file_success = 0
                    
                    record_count = 0
                    for record in stream_parse_enhanced(file_info["url"], payer_name):
                        if record_count >= max_records:
                            break
                        record_count += 1
                        try:
                            # Test the handler
                            processed_records = handler.parse_in_network(record)
                            
                            # Validate and count
                            for processed_record in processed_records:
                                if self._validate_record(processed_record):
                                    file_success += 1
                                    if len(results["sample_output"]) < 5:
                                        results["sample_output"].append({
                                            "original_keys": list(record.keys()),
                                            "processed_keys": list(processed_record.keys()),
                                            "has_in_network": "in_network" in processed_record,
                                            "in_network_count": len(processed_record.get("in_network", []))
                                        })
                            
                            file_records += 1
                            results["total_records"] += 1
                            
                        except Exception as e:
                            results["errors"].append(f"Record processing error: {e}")
                    
                    results["successful_records"] += file_success
                    results["files_tested"] += 1
                    
                    self.logger.info(f"  ✅ File {i+1}: {file_success}/{file_records} records successful")
                    
                except Exception as e:
                    error_msg = f"File processing error: {e}"
                    results["errors"].append(error_msg)
                    self.logger.error(f"  ❌ File {i+1}: {error_msg}")
            
            # Calculate processing rate
            elapsed_time = time.time() - start_time
            if elapsed_time > 0:
                results["processing_rate"] = results["total_records"] / elapsed_time
            
            # Calculate success rate
            if results["total_records"] > 0:
                success_rate = (results["successful_records"] / results["total_records"]) * 100
                results["success_rate"] = success_rate
                self.logger.info(f"📊 Success rate: {success_rate:.1f}%")
                self.logger.info(f"⚡ Processing rate: {results['processing_rate']:.1f} records/sec")
            
        except Exception as e:
            results["errors"].append(f"Test setup error: {e}")
            self.logger.error(f"❌ Test setup failed: {e}")
        
        return results
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Basic validation of a processed record."""
        # Check for basic structure
        if not isinstance(record, dict):
            return False
        
        # Check for in_network section
        if "in_network" not in record:
            return False
        
        # Check that in_network is a list
        if not isinstance(record["in_network"], list):
            return False
        
        # Basic validation passed
        return True
    
    def test_normalization(self, payer_name: str, index_url: str, 
                          max_records: int = 10) -> Dict[str, Any]:
        """
        Test the full normalization pipeline for a payer.
        
        Args:
            payer_name: Name of the payer
            index_url: URL to the payer's MRF index
            max_records: Maximum number of records to test
        
        Returns:
            Normalization test results
        """
        self.logger.info(f"🔄 Testing normalization for payer: {payer_name}")
        
        results = {
            "payer_name": payer_name,
            "test_timestamp": datetime.now().isoformat(),
            "records_tested": 0,
            "normalized_records": 0,
            "errors": [],
            "sample_normalized": []
        }
        
        try:
            # Get handler
            handler = get_handler(payer_name)
            
            # Get a sample file
            mrf_files = list_mrf_blobs_enhanced(index_url)
            if not mrf_files:
                results["errors"].append("No MRF files found")
                return results
            
            sample_file = mrf_files[0]
            self.logger.info(f"Testing normalization with file: {sample_file.get('name', 'unknown')}")
            
            record_count = 0
            for record in stream_parse_enhanced(sample_file["url"], payer_name):
                if record_count >= max_records:
                    break
                record_count += 1
                try:
                    # Process with handler
                    processed_records = handler.parse_in_network(record)
                    
                    for processed_record in processed_records:
                        try:
                            # Test normalization
                            normalized = normalize_tic_record(processed_record)
                            
                            if normalized:
                                results["normalized_records"] += 1
                                
                                # Save sample normalized record
                                if len(results["sample_normalized"]) < 3:
                                    results["sample_normalized"].append({
                                        "payer_name": normalized.get("payer_name"),
                                        "service_code": normalized.get("service_code"),
                                        "negotiated_rate": normalized.get("negotiated_rate"),
                                        "effective_date": normalized.get("effective_date"),
                                        "provider_count": len(normalized.get("providers", [])),
                                        "organization_count": len(normalized.get("organizations", []))
                                    })
                            
                            results["records_tested"] += 1
                            
                        except Exception as e:
                            results["errors"].append(f"Normalization error: {e}")
                    
                except Exception as e:
                    results["errors"].append(f"Processing error: {e}")
            
            # Calculate success rate
            if results["records_tested"] > 0:
                success_rate = (results["normalized_records"] / results["records_tested"]) * 100
                results["normalization_success_rate"] = success_rate
                self.logger.info(f"📊 Normalization success rate: {success_rate:.1f}%")
            
        except Exception as e:
            results["errors"].append(f"Normalization test setup error: {e}")
            self.logger.error(f"❌ Normalization test failed: {e}")
        
        return results
    
    def compare_payers(self, payer1: str, payer2: str, index_url1: str, index_url2: str) -> Dict[str, Any]:
        """
        Compare two payers to identify structural differences.
        
        Args:
            payer1: Name of first payer
            payer2: Name of second payer
            index_url1: URL to first payer's MRF index
            index_url2: URL to second payer's MRF index
        
        Returns:
            Comparison results
        """
        self.logger.info(f"🔍 Comparing payers: {payer1} vs {payer2}")
        
        comparison = {
            "payer1": payer1,
            "payer2": payer2,
            "comparison_timestamp": datetime.now().isoformat(),
            "structural_differences": [],
            "field_differences": [],
            "sample_comparisons": []
        }
        
        try:
            # Get handlers
            handler1 = get_handler(payer1)
            handler2 = get_handler(payer2)
            
            # Get sample files
            files1 = list_mrf_blobs_enhanced(index_url1)[:1]
            files2 = list_mrf_blobs_enhanced(index_url2)[:1]
            
            if not files1 or not files2:
                comparison["errors"] = ["Could not fetch sample files"]
                return comparison
            
            # Analyze structure of sample records
            sample1 = self._analyze_structure(files1[0], handler1)
            sample2 = self._analyze_structure(files2[0], handler2)
            
            # Compare structures
            comparison["structural_differences"] = self._compare_structures(sample1, sample2)
            comparison["field_differences"] = self._compare_fields(sample1, sample2)
            
            # Save sample comparisons
            comparison["sample_comparisons"] = [
                {"payer": payer1, "structure": sample1},
                {"payer": payer2, "structure": sample2}
            ]
            
        except Exception as e:
            comparison["errors"] = [f"Comparison failed: {e}"]
            self.logger.error(f"❌ Comparison failed: {e}")
        
        return comparison
    
    def _analyze_structure(self, file_info: Dict[str, Any], handler) -> Dict[str, Any]:
        """Analyze the structure of a sample file."""
        structure = {
            "record_types": set(),
            "field_patterns": {},
            "nested_structures": {},
            "sample_record": None
        }
        
        try:
            record_count = 0
            for record in stream_parse_enhanced(file_info["url"], payer_name):
                if record_count >= 5:
                    break
                record_count += 1
                # Process with handler
                processed_records = handler.parse_in_network(record)
                
                for processed_record in processed_records:
                    # Analyze structure
                    if "in_network" in processed_record:
                        structure["record_types"].add("in_network")
                        
                        for item in processed_record["in_network"]:
                            # Analyze provider groups
                            if "provider_groups" in item:
                                for pg in item["provider_groups"]:
                                    for key in pg.keys():
                                        structure["field_patterns"][key] = structure["field_patterns"].get(key, 0) + 1
                            
                            # Analyze negotiated rates
                            if "negotiated_rates" in item:
                                for rate in item["negotiated_rates"]:
                                    for key in rate.keys():
                                        structure["field_patterns"][key] = structure["field_patterns"].get(key, 0) + 1
                
                # Save first sample record
                if not structure["sample_record"]:
                    structure["sample_record"] = processed_record
                
                break  # Only analyze first record for structure
        
        except Exception as e:
            structure["error"] = str(e)
        
        # Convert sets to lists for JSON serialization
        structure["record_types"] = list(structure["record_types"])
        
        return structure
    
    def _compare_structures(self, struct1: Dict[str, Any], struct2: Dict[str, Any]) -> List[str]:
        """Compare two structures and return differences."""
        differences = []
        
        # Compare record types
        types1 = set(struct1.get("record_types", []))
        types2 = set(struct2.get("record_types", []))
        
        if types1 != types2:
            differences.append(f"Record types differ: {types1} vs {types2}")
        
        # Compare field patterns
        fields1 = set(struct1.get("field_patterns", {}).keys())
        fields2 = set(struct2.get("field_patterns", {}).keys())
        
        if fields1 != fields2:
            differences.append(f"Field patterns differ: {fields1} vs {fields2}")
        
        return differences
    
    def _compare_fields(self, struct1: Dict[str, Any], struct2: Dict[str, Any]) -> List[str]:
        """Compare field usage between two structures."""
        differences = []
        
        fields1 = struct1.get("field_patterns", {})
        fields2 = struct2.get("field_patterns", {})
        
        all_fields = set(fields1.keys()) | set(fields2.keys())
        
        for field in all_fields:
            count1 = fields1.get(field, 0)
            count2 = fields2.get(field, 0)
            
            if count1 != count2:
                differences.append(f"Field '{field}' usage differs: {count1} vs {count2}")
        
        return differences
    
    def generate_test_report(self, test_results: Dict[str, Any], output_file: str = None) -> str:
        """
        Generate a formatted test report.
        
        Args:
            test_results: Results from testing
            output_file: Optional output file path
        
        Returns:
            Path to the generated report
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"quick_test_{test_results['payer_name']}_{timestamp}.json"
        
        # Save detailed results
        with open(output_file, 'w') as f:
            json.dump(test_results, f, indent=2)
        
        # Print summary
        print(f"\n📊 Quick Test Summary for {test_results['payer_name']}")
        print("=" * 50)
        print(f"Files tested: {test_results.get('files_tested', 0)}")
        print(f"Total records: {test_results.get('total_records', 0)}")
        print(f"Successful records: {test_results.get('successful_records', 0)}")
        
        if test_results.get('total_records', 0) > 0:
            success_rate = (test_results['successful_records'] / test_results['total_records']) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        if test_results.get('processing_rate', 0) > 0:
            print(f"Processing rate: {test_results['processing_rate']:.1f} records/sec")
        
        if test_results.get('errors'):
            print(f"\n❌ Errors ({len(test_results['errors'])}):")
            for error in test_results['errors'][:3]:  # Show first 3 errors
                print(f"  - {error}")
        
        if test_results.get('warnings'):
            print(f"\n⚠️ Warnings ({len(test_results['warnings'])}):")
            for warning in test_results['warnings'][:3]:  # Show first 3 warnings
                print(f"  - {warning}")
        
        print(f"\n📄 Detailed results saved to: {output_file}")
        
        return output_file


def main():
    """Main CLI interface for quick payer testing."""
    parser = argparse.ArgumentParser(description="Quick Payer Test")
    parser.add_argument("--payer-name", required=True, help="Name of the payer")
    parser.add_argument("--index-url", required=True, help="URL to the payer's MRF index")
    parser.add_argument("--test-type", choices=["handler", "normalization", "compare"], 
                       default="handler", help="Type of test to run")
    parser.add_argument("--compare-payer", help="Second payer for comparison")
    parser.add_argument("--compare-url", help="Second payer's index URL")
    parser.add_argument("--max-files", type=int, default=2, help="Maximum files to test")
    parser.add_argument("--max-records", type=int, default=50, help="Maximum records per file")
    parser.add_argument("--output", help="Output file for results")
    
    args = parser.parse_args()
    
    # Create test instance
    tester = QuickPayerTest()
    
    if args.test_type == "handler":
        results = tester.test_payer_handler(args.payer_name, args.index_url, 
                                         args.max_files, args.max_records)
        tester.generate_test_report(results, args.output)
        
    elif args.test_type == "normalization":
        results = tester.test_normalization(args.payer_name, args.index_url, args.max_records)
        tester.generate_test_report(results, args.output)
        
    elif args.test_type == "compare":
        if not args.compare_payer or not args.compare_url:
            print("❌ --compare-payer and --compare-url are required for comparison tests")
            return
        
        results = tester.compare_payers(args.payer_name, args.compare_payer, 
                                     args.index_url, args.compare_url)
        tester.generate_test_report(results, args.output)


if __name__ == "__main__":
    main() 