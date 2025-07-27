#!/usr/bin/env python3
"""Intelligent Payer Integration - Analyze and auto-generate handlers for new payers.

This script takes the output from analyze_payer_structure.py and intelligently:
1. Analyzes the structure patterns
2. Generates appropriate handler code
3. Updates production configuration
4. Tests the integration
"""

import json
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import re
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tic_mrf_scraper.payers import get_handler, PayerHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record

class IntelligentPayerIntegration:
    """Intelligent payer integration system."""
    
    def __init__(self, analysis_file: str, payer_name: str, index_url: str):
        self.analysis_file = analysis_file
        self.payer_name = payer_name
        self.index_url = index_url
        self.analysis_data = self._load_analysis()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _load_analysis(self) -> Dict[str, Any]:
        """Load the analysis data from analyze_payer_structure.py output."""
        with open(self.analysis_file, 'r') as f:
            return json.load(f)
    
    def analyze_structure_patterns(self) -> Dict[str, Any]:
        """Analyze structure patterns to determine handler complexity."""
        patterns = {
            "handler_complexity": "standard",
            "custom_requirements": [],
            "provider_structure": "standard",
            "rate_structure": "standard",
            "compression_handling": "standard",
            "recommendations": []
        }
        
        # Get payer analysis
        payer_analysis = self.analysis_data.get(self.payer_name, {})
        toc_analysis = payer_analysis.get("table_of_contents", {})
        mrf_analysis = payer_analysis.get("in_network_mrf", {})
        
        # Analyze TOC structure
        if toc_analysis.get("structure_type") == "legacy_blobs":
            patterns["custom_requirements"].append("legacy_blobs_structure")
            patterns["recommendations"].append("Use enhanced blob listing for legacy structure")
        
        # Analyze MRF structure
        if mrf_analysis.get("structure_type") == "standard_in_network":
            # Check top-level structure for provider_references
            top_level_keys = mrf_analysis.get("top_level_keys", [])
            if "provider_references" in top_level_keys:
                patterns["custom_requirements"].append("top_level_provider_references")
                patterns["recommendations"].append("Handle provider_references at top level")
                patterns["provider_structure"] = "top_level_providers"
            
            # Check billing code types for non-standard codes
            billing_code_types = mrf_analysis.get("billing_code_types", {})
            non_standard_types = [k for k in billing_code_types.keys() if k not in ["CPT", "HCPCS", "ICD-10"]]
            if non_standard_types:
                patterns["custom_requirements"].append(f"non_standard_billing_codes: {non_standard_types}")
                patterns["recommendations"].append(f"Handle non-standard billing codes: {non_standard_types}")
            
            # Analyze sample items for complex structures
            sample_items = mrf_analysis.get("sample_items", [])
            if sample_items:
                first_item = sample_items[0]
                
                # Check for negotiated_rates structure
                if "negotiated_rates" in first_item.get("keys", []):
                    negotiated_rates_count = first_item.get("negotiated_rates_count", 0)
                    if negotiated_rates_count > 0:
                        patterns["custom_requirements"].append("nested_negotiated_rates")
                        patterns["recommendations"].append("Process nested negotiated_rates arrays")
                        patterns["rate_structure"] = "nested_rates"
                
                # Check rate structure for provider references
                rate_structure = first_item.get("rate_structure", {})
                if rate_structure.get("has_provider_references"):
                    patterns["custom_requirements"].append("rate_level_provider_references")
                    patterns["recommendations"].append("Handle provider references in rate structure")
                
                # Check price structure for service codes
                price_structure = first_item.get("price_structure", {})
                if "service_codes" in price_structure:
                    service_codes = price_structure.get("service_codes", [])
                    if isinstance(service_codes, list) and len(service_codes) > 0:
                        patterns["custom_requirements"].append("service_codes_array")
                        patterns["recommendations"].append("Handle service_codes as array")
                
                # Check for covered_services field
                if "covered_services" in first_item.get("keys", []):
                    patterns["custom_requirements"].append("covered_services_field")
                    patterns["recommendations"].append("Handle covered_services field")
                
                # Check for complex provider structure
                if rate_structure.get("has_provider_groups"):
                    patterns["provider_structure"] = "nested_providers"
                    patterns["custom_requirements"].append("nested_provider_structure")
                    patterns["recommendations"].append("Handle nested provider arrays in provider_groups")
                
                # Check rate field names
                if price_structure.get("rate_field") == "negotiated_price":
                    patterns["rate_structure"] = "negotiated_price_field"
                    patterns["custom_requirements"].append("negotiated_price_mapping")
                    patterns["recommendations"].append("Map negotiated_price to negotiated_rate")
        
        # Determine complexity based on multiple factors
        complexity_score = 0
        
        # Add points for each complexity factor
        if len(patterns["custom_requirements"]) > 0:
            complexity_score += len(patterns["custom_requirements"])
        
        # Add points for non-standard billing codes
        if any("non_standard_billing_codes" in req for req in patterns["custom_requirements"]):
            complexity_score += 2
        
        # Add points for nested structures
        if any("nested" in req for req in patterns["custom_requirements"]):
            complexity_score += 2
        
        # Add points for provider complexity
        if patterns["provider_structure"] != "standard":
            complexity_score += 1
        
        # Add points for rate complexity
        if patterns["rate_structure"] != "standard":
            complexity_score += 1
        
        # Determine final complexity
        if complexity_score >= 4:
            patterns["handler_complexity"] = "complex"
        elif complexity_score >= 2:
            patterns["handler_complexity"] = "moderate"
        else:
            patterns["handler_complexity"] = "standard"
        
        return patterns
    
    def generate_handler_code(self, patterns: Dict[str, Any]) -> str:
        """Generate handler code based on structure patterns."""
        
        handler_class_name = f"{self.payer_name.title()}Handler"
        
        # Base handler template
        handler_code = f'''from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("{self.payer_name}")
class {handler_class_name}(PayerHandler):
    """Handler for {self.payer_name.title()} MRF files.
    
    Generated based on structure analysis:
    - Complexity: {patterns["handler_complexity"]}
    - Provider structure: {patterns["provider_structure"]}
    - Rate structure: {patterns["rate_structure"]}
    - Custom requirements: {", ".join(patterns["custom_requirements"])}
    """

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse {self.payer_name} in_network records with {patterns["handler_complexity"]} structure."""
        results = []
        
        # Extract basic fields
        billing_code = record.get("billing_code", "")
        billing_code_type = record.get("billing_code_type", "")
        description = record.get("description", "")
        
        # Handle different complexity levels
        if patterns["handler_complexity"] == "complex":
            results = self._parse_complex_structure(record, billing_code, billing_code_type, description)
        elif patterns["handler_complexity"] == "moderate":
            results = self._parse_moderate_structure(record, billing_code, billing_code_type, description)
        else:
            results = self._parse_standard_structure(record, billing_code, billing_code_type, description)
        
        return results
'''
        
        # Add complexity-specific methods
        if patterns["handler_complexity"] == "complex":
            handler_code += self._generate_complex_parsing_methods(patterns)
        elif patterns["handler_complexity"] == "moderate":
            handler_code += self._generate_moderate_parsing_methods(patterns)
        else:
            handler_code += self._generate_standard_parsing_methods()
        
        return handler_code

    def _generate_complex_parsing_methods(self, patterns: Dict[str, Any]) -> str:
        """Generate methods for complex structure parsing."""
        methods = f'''
    
    def _parse_complex_structure(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> List[Dict[str, Any]]:
        """Parse complex structure with nested rates and provider references."""
        results = []
        
        # Handle nested negotiated_rates
        negotiated_rates = record.get("negotiated_rates", [])
        for rate_group in negotiated_rates:
            negotiated_prices = rate_group.get("negotiated_prices", [])
            provider_references = rate_group.get("provider_references", [])
            
            # Process each negotiated price
            for price in negotiated_prices:
                negotiated_rate = price.get("negotiated_rate")
                negotiated_type = price.get("negotiated_type", "")
                billing_class = price.get("billing_class", "")
                service_codes = price.get("service_code", [])
                if isinstance(service_codes, str):
                    service_codes = [service_codes]
                
                # Process each provider reference
                for provider_ref in provider_references:
                    provider_group_id = provider_ref.get("provider_group_id", "")
                    provider_groups = provider_ref.get("provider_groups", [])
                    
                    # Create normalized record
                    normalized_record = {{
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "description": description,
                        "negotiated_rate": negotiated_rate,
                        "negotiated_type": negotiated_type,
                        "billing_class": billing_class,
                        "service_codes": service_codes,
                        "provider_group_id": provider_group_id,
                        "provider_groups": provider_groups,
                        "payer_name": "{self.payer_name}"
                    }}
                    
                    results.append(normalized_record)
        
        return results
    
    def get_provider_references(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract provider references from complex structure."""
        provider_refs = data.get("provider_references", [])
        results = []
        
        for ref in provider_refs:
            provider_group_id = ref.get("provider_group_id", "")
            provider_groups = ref.get("provider_groups", [])
            
            # Process provider groups
            for group in provider_groups:
                providers = group.get("providers", [])
                for provider in providers:
                    provider_info = {{
                        "provider_group_id": provider_group_id,
                        "provider_npi": provider.get("npi"),
                        "provider_tin": provider.get("tin"),
                        "provider_name": provider.get("name", ""),
                        "payer_name": "{self.payer_name}"
                    }}
                    results.append(provider_info)
        
        return results
'''
        return methods

    def _generate_moderate_parsing_methods(self, patterns: Dict[str, Any]) -> str:
        """Generate methods for moderate complexity parsing."""
        methods = f'''
    
    def _parse_moderate_structure(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> List[Dict[str, Any]]:
        """Parse moderate complexity structure."""
        results = []
        
        # Handle service codes array if present
        service_codes = record.get("service_codes", [])
        if isinstance(service_codes, str):
            service_codes = [service_codes]
        
        # Handle covered services if present
        covered_services = record.get("covered_services", {{}})
        
        # Create normalized record
        normalized_record = {{
            "billing_code": billing_code,
            "billing_code_type": billing_code_type,
            "description": description,
            "service_codes": service_codes,
            "covered_services": covered_services,
            "payer_name": "{self.payer_name}"
        }}
        
        # Add rate information if available
        if "negotiated_rate" in record:
            normalized_record["negotiated_rate"] = record["negotiated_rate"]
        
        results.append(normalized_record)
        return results
'''
        return methods

    def _generate_standard_parsing_methods(self) -> str:
        """Generate methods for standard structure parsing."""
        methods = f'''
    
    def _parse_standard_structure(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> List[Dict[str, Any]]:
        """Parse standard structure."""
        return [record]
'''
        return methods
    
    def _generate_nested_provider_code(self) -> str:
        """Generate code for handling nested provider structures."""
        return '''
        # Handle nested provider structures
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                if "provider_groups" in rate_group:
                    normalized_groups = []
                    for pg in rate_group["provider_groups"]:
                        if "providers" in pg and pg["providers"]:
                            # Extract NPIs from nested providers
                            npis = []
                            for provider in pg["providers"]:
                                if "npi" in provider:
                                    npis.append(provider["npi"])
                            
                            # Create normalized provider group
                            normalized_groups.append({
                                "npi": npis[0] if npis else "",
                                "tin": pg.get("tin", ""),
                                "npi_list": npis
                            })
                        else:
                            # Standard provider group
                            normalized_groups.append(pg)
                    
                    rate_group["provider_groups"] = normalized_groups
'''
    
    def _generate_rate_mapping_code(self) -> str:
        """Generate code for mapping negotiated_price to negotiated_rate."""
        return '''
        # Map negotiated_price to negotiated_rate
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                if "negotiated_prices" in rate_group:
                    for price in rate_group["negotiated_prices"]:
                        if "negotiated_price" in price and "negotiated_rate" not in price:
                            price["negotiated_rate"] = price["negotiated_price"]
'''
    
    def _generate_service_codes_code(self) -> str:
        """Generate code for handling service_codes arrays."""
        return '''
        # Handle service_codes as array
        if "service_codes" in record and isinstance(record["service_codes"], list):
            # Ensure service_code is set from first service_codes item
            if record["service_codes"] and not record.get("service_code"):
                record["service_code"] = record["service_codes"][0]
'''
    
    def _generate_legacy_blobs_code(self) -> str:
        """Generate code for handling legacy blobs structure."""
        return '''
        # Handle legacy blobs structure if needed
        # This is typically handled at the file listing level
        pass
'''
    
    def create_handler_file(self, handler_code: str) -> str:
        """Create the handler file in the payers directory."""
        handler_path = Path("src/tic_mrf_scraper/payers") / f"{self.payer_name}.py"
        
        with open(handler_path, 'w') as f:
            f.write(handler_code)
        
        self.logger.info(f"Created handler file: {handler_path}")
        return str(handler_path)
    
    def update_production_config(self) -> bool:
        """Update production_config.yaml with the new payer."""
        config_path = Path("production_config.yaml")
        
        if not config_path.exists():
            self.logger.error("production_config.yaml not found")
            return False
        
        # Read current config
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Add payer to endpoints
        if "payer_endpoints" not in config:
            config["payer_endpoints"] = {}
        
        config["payer_endpoints"][self.payer_name] = self.index_url
        
        # Write updated config
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        self.logger.info(f"Updated production_config.yaml with {self.payer_name}")
        return True
    
    def test_integration(self) -> Dict[str, Any]:
        """Test the integration by processing a small sample."""
        test_results = {
            "success": False,
            "files_processed": 0,
            "records_processed": 0,
            "errors": [],
            "warnings": []
        }
        
        try:
            # Test handler import
            handler = get_handler(self.payer_name)
            test_results["handler_import"] = "success"
            
            # Test file listing
            mrf_files = handler.list_mrf_files(self.index_url)
            rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
            
            if not rate_files:
                test_results["errors"].append("No rate files found")
                return test_results
            
            # Test processing first file
            test_file = rate_files[0]
            test_results["files_processed"] = 1
            
            # Process a small sample
            record_count = 0
            max_records = 10
            
            for record in stream_parse_enhanced(
                test_file["url"], 
                self.payer_name, 
                test_file.get("provider_reference_url"),
                handler
            ):
                if record_count >= max_records:
                    break
                
                # Test normalization
                normalized = normalize_tic_record(record, set(), self.payer_name)
                if normalized:
                    test_results["records_processed"] += 1
                
                record_count += 1
            
            test_results["success"] = True
            test_results["sample_size"] = record_count
            
        except Exception as e:
            test_results["errors"].append(f"Integration test failed: {str(e)}")
        
        return test_results
    
    def run_full_integration(self) -> Dict[str, Any]:
        """Run the complete intelligent integration process."""
        self.logger.info(f"Starting intelligent integration for {self.payer_name}")
        
        results = {
            "payer_name": self.payer_name,
            "index_url": self.index_url,
            "timestamp": datetime.now().isoformat(),
            "steps_completed": [],
            "errors": [],
            "warnings": []
        }
        
        try:
            # Step 1: Analyze structure patterns
            self.logger.info("Step 1: Analyzing structure patterns...")
            patterns = self.analyze_structure_patterns()
            results["structure_analysis"] = patterns
            results["steps_completed"].append("structure_analysis")
            
            # Step 2: Generate handler code
            self.logger.info("Step 2: Generating handler code...")
            handler_code = self.generate_handler_code(patterns)
            results["handler_code"] = handler_code
            results["steps_completed"].append("handler_generation")
            
            # Step 3: Create handler file
            self.logger.info("Step 3: Creating handler file...")
            handler_path = self.create_handler_file(handler_code)
            results["handler_path"] = handler_path
            results["steps_completed"].append("handler_creation")
            
            # Step 4: Update production config
            self.logger.info("Step 4: Updating production config...")
            config_updated = self.update_production_config()
            if config_updated:
                results["steps_completed"].append("config_update")
            else:
                results["errors"].append("Failed to update production config")
            
            # Step 5: Test integration
            self.logger.info("Step 5: Testing integration...")
            test_results = self.test_integration()
            results["test_results"] = test_results
            results["steps_completed"].append("integration_test")
            
            if test_results["success"]:
                self.logger.info("[SUCCESS] Integration successful!")
            else:
                results["errors"].extend(test_results["errors"])
                self.logger.warning("[WARNING] Integration completed with issues")
            
        except Exception as e:
            results["errors"].append(f"Integration failed: {str(e)}")
            self.logger.error(f"Integration failed: {str(e)}")
        
        return results

    def generate_diagnostic_report(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive diagnostic report for troubleshooting."""
        diagnostic = {
            "integration_summary": {
                "payer_name": self.payer_name,
                "index_url": self.index_url,
                "timestamp": datetime.now().isoformat(),
                "steps_completed": results.get("steps_completed", []),
                "total_steps": 5,
                "success_rate": f"{len(results.get('steps_completed', []))}/5"
            },
            "structure_analysis": {
                "analysis_file": self.analysis_file,
                "analysis_data_keys": list(self.analysis_data.keys()),
                "payer_analysis_keys": list(self.analysis_data.get(self.payer_name, {}).keys()),
                "toc_structure": self.analysis_data.get(self.payer_name, {}).get("table_of_contents", {}).get("structure_type"),
                "mrf_structure": self.analysis_data.get(self.payer_name, {}).get("in_network_mrf", {}).get("structure_type"),
                "file_counts": self.analysis_data.get(self.payer_name, {}).get("table_of_contents", {}).get("file_counts", {}),
                "sample_billing_codes": self._extract_sample_billing_codes(),
                "complexity_factors": results.get("structure_analysis", {}).get("custom_requirements", [])
            },
            "handler_generation": {
                "handler_path": results.get("handler_path", "Not created"),
                "handler_complexity": results.get("structure_analysis", {}).get("handler_complexity", "unknown"),
                "custom_requirements": results.get("structure_analysis", {}).get("custom_requirements", []),
                "recommendations": results.get("structure_analysis", {}).get("recommendations", [])
            },
            "configuration": {
                "config_updated": "config_update" in results.get("steps_completed", []),
                "production_config_path": "production_config.yaml",
                "payer_added_to_config": self.payer_name in self._get_production_config().get("payer_endpoints", {})
            },
            "testing": {
                "test_results": results.get("test_results", {}),
                "tests_passed": results.get("test_results", {}).get("tests_passed", 0),
                "tests_failed": results.get("test_results", {}).get("tests_failed", 0),
                "test_errors": results.get("test_results", {}).get("errors", [])
            },
            "troubleshooting": {
                "common_issues": self._identify_common_issues(results),
                "next_steps": self._generate_next_steps(results),
                "validation_checks": self._generate_validation_checks(results)
            }
        }
        return diagnostic

    def _extract_sample_billing_codes(self) -> List[Dict[str, Any]]:
        """Extract sample billing codes from analysis for reference."""
        sample_codes = []
        mrf_analysis = self.analysis_data.get(self.payer_name, {}).get("in_network_mrf", {})
        sample_items = mrf_analysis.get("sample_items", [])
        
        for item in sample_items[:3]:  # Limit to first 3 for brevity
            sample_codes.append({
                "billing_code": item.get("billing_code"),
                "billing_code_type": item.get("billing_code_type"),
                "description": item.get("description", "")[:100],  # Truncate long descriptions
                "negotiated_rates_count": item.get("negotiated_rates_count", 0)
            })
        return sample_codes

    def _get_production_config(self) -> Dict[str, Any]:
        """Get current production config."""
        config_path = Path("production_config.yaml")
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return {}

    def _identify_common_issues(self, results: Dict[str, Any]) -> List[str]:
        """Identify potential issues based on results."""
        issues = []
        
        if "config_update" not in results.get("steps_completed", []):
            issues.append("Production config not updated - check file permissions")
        
        if results.get("test_results", {}).get("tests_failed", 0) > 0:
            issues.append("Integration tests failed - review handler implementation")
        
        if not results.get("structure_analysis", {}).get("custom_requirements"):
            issues.append("No custom requirements detected - handler may be too basic")
        
        return issues

    def _generate_next_steps(self, results: Dict[str, Any]) -> List[str]:
        """Generate actionable next steps."""
        steps = []
        
        if results.get("test_results", {}).get("success"):
            steps.append("✅ Handler ready for production use")
            steps.append("Run full pipeline test with sample data")
            steps.append("Monitor processing statistics for quality")
        else:
            steps.append("🔧 Review handler implementation")
            steps.append("Check test error details")
            steps.append("Consider manual handler development")
        
        return steps

    def _generate_validation_checks(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate validation checks for troubleshooting."""
        checks = [
            {
                "check": "Handler file exists",
                "status": "PASS" if Path(results.get("handler_path", "")).exists() else "FAIL",
                "path": results.get("handler_path", "Not found")
            },
            {
                "check": "Handler imports successfully",
                "status": "PASS" if results.get("test_results", {}).get("tests_passed", 0) > 0 else "FAIL",
                "details": f"Tests passed: {results.get('test_results', {}).get('tests_passed', 0)}"
            },
            {
                "check": "Production config updated",
                "status": "PASS" if "config_update" in results.get("steps_completed", []) else "FAIL",
                "details": f"Payer in config: {self.payer_name in self._get_production_config().get('payer_endpoints', {})}"
            },
            {
                "check": "Structure analysis completed",
                "status": "PASS" if "structure_analysis" in results.get("steps_completed", []) else "FAIL",
                "details": f"Complexity: {results.get('structure_analysis', {}).get('handler_complexity', 'unknown')}"
            }
        ]
        return checks


def main():
    parser = argparse.ArgumentParser(description="Intelligent Payer Integration")
    parser.add_argument("--analysis-file", required=True, help="Path to analyze_payer_structure.py output")
    parser.add_argument("--payer-name", required=True, help="Name of the payer")
    parser.add_argument("--index-url", required=True, help="Index URL for the payer")
    parser.add_argument("--output-file", help="Output file for results")
    parser.add_argument("--diagnostic-file", help="Output file for diagnostic report")
    
    args = parser.parse_args()
    
    # Run integration
    integrator = IntelligentPayerIntegration(args.analysis_file, args.payer_name, args.index_url)
    results = integrator.run_full_integration()
    
    # Generate diagnostic report
    diagnostic = integrator.generate_diagnostic_report(results)
    
    # Save results
    if args.output_file:
        with open(args.output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to: {args.output_file}")
    
    # Save diagnostic report
    diagnostic_file = args.diagnostic_file or f"diagnostic_report_{args.payer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(diagnostic_file, 'w') as f:
        json.dump(diagnostic, f, indent=2)
    print(f"Diagnostic report saved to: {diagnostic_file}")
    
    # Print concise summary
    print(f"\n{'='*60}")
    print(f"INTELLIGENT INTEGRATION RESULTS FOR {args.payer_name.upper()}")
    print(f"{'='*60}")
    
    # Integration status
    success_rate = diagnostic["integration_summary"]["success_rate"]
    print(f"Integration Status: {success_rate}")
    
    # Key metrics
    print(f"Handler Complexity: {diagnostic['handler_generation']['handler_complexity']}")
    print(f"Tests Passed: {diagnostic['testing']['tests_passed']}")
    print(f"Tests Failed: {diagnostic['testing']['tests_failed']}")
    
    # Structure info
    toc_structure = diagnostic["structure_analysis"]["toc_structure"]
    mrf_structure = diagnostic["structure_analysis"]["mrf_structure"]
    print(f"TOC Structure: {toc_structure}")
    print(f"MRF Structure: {mrf_structure}")
    
    # Sample billing codes
    sample_codes = diagnostic["structure_analysis"]["sample_billing_codes"]
    if sample_codes:
        print(f"Sample Billing Codes: {len(sample_codes)} found")
        for code in sample_codes[:2]:  # Show first 2
            print(f"  - {code['billing_code']} ({code['billing_code_type']})")
    
    # Issues and next steps
    issues = diagnostic["troubleshooting"]["common_issues"]
    if issues:
        print(f"\nPotential Issues ({len(issues)}):")
        for issue in issues:
            print(f"  ⚠️  {issue}")
    
    next_steps = diagnostic["troubleshooting"]["next_steps"]
    if next_steps:
        print(f"\nNext Steps:")
        for step in next_steps:
            print(f"  {step}")
    
    # Validation checks
    checks = diagnostic["troubleshooting"]["validation_checks"]
    print(f"\nValidation Checks:")
    for check in checks:
        status_icon = "✅" if check["status"] == "PASS" else "❌"
        print(f"  {status_icon} {check['check']}: {check['status']}")
        if "details" in check:
            print(f"      Details: {check['details']}")
    
    print(f"\nDetailed diagnostic report available in: {diagnostic_file}")
    
    # Print quick reference summary
    print(f"\n{'='*40}")
    print("QUICK REFERENCE")
    print(f"{'='*40}")
    print(f"Handler: {diagnostic['handler_generation']['handler_path']}")
    print(f"Complexity: {diagnostic['handler_generation']['handler_complexity']}")
    print(f"Config Updated: {diagnostic['configuration']['config_updated']}")
    print(f"Tests: {diagnostic['testing']['tests_passed']}/{diagnostic['testing']['tests_passed'] + diagnostic['testing']['tests_failed']}")
    
    if diagnostic["troubleshooting"]["common_issues"]:
        print(f"Issues: {len(diagnostic['troubleshooting']['common_issues'])} found")
    else:
        print("Issues: None detected")


if __name__ == "__main__":
    main() 