#!/usr/bin/env python3
"""
Payer Development Workflow - A systematic approach to adding new payers to the ETL pipeline.

This script provides a complete workflow for:
1. Analyzing new payer MRF samples
2. Developing custom handlers
3. Testing and validating the integration
4. Deploying to production

Usage:
    python scripts/payer_development_workflow.py --help
"""

import os
import sys
import json
import yaml
import argparse
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tic_mrf_scraper.payers import get_handler, PayerHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.diagnostics import identify_index, detect_compression


class PayerDevelopmentWorkflow:
    """Systematic workflow for adding new payers to the ETL pipeline."""
    
    def __init__(self, work_dir: str = "payer_development"):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.work_dir / "samples").mkdir(exist_ok=True)
        (self.work_dir / "handlers").mkdir(exist_ok=True)
        (self.work_dir / "tests").mkdir(exist_ok=True)
        (self.work_dir / "reports").mkdir(exist_ok=True)
        
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging for the development workflow."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.work_dir / "development.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_payer_sample(self, payer_name: str, index_url: str, 
                           sample_size: int = 3) -> Dict[str, Any]:
        """
        Step 1: Analyze a new payer's MRF structure and create a development report.
        
        Args:
            payer_name: Name of the payer (e.g., "new_payer")
            index_url: URL to the payer's MRF index
            sample_size: Number of files to sample for analysis
        
        Returns:
            Analysis report with structure details and recommendations
        """
        self.logger.info(f"🔍 Analyzing payer: {payer_name}")
        
        # Create payer-specific directory
        payer_dir = self.work_dir / "samples" / payer_name
        payer_dir.mkdir(exist_ok=True)
        
        # Get MRF file list
        try:
            mrf_files = list_mrf_blobs_enhanced(index_url)
            self.logger.info(f"Found {len(mrf_files)} MRF files")
        except Exception as e:
            self.logger.error(f"Failed to fetch MRF index: {e}")
            return {"error": str(e)}
        
        # Sample files for analysis
        sample_files = mrf_files[:sample_size]
        
        analysis_report = {
            "payer_name": payer_name,
            "index_url": index_url,
            "total_files": len(mrf_files),
            "sample_files": len(sample_files),
            "analysis_timestamp": datetime.now().isoformat(),
            "file_analysis": [],
            "structure_patterns": {},
            "recommendations": []
        }
        
        # Analyze each sample file
        for i, file_info in enumerate(sample_files):
            self.logger.info(f"Analyzing sample file {i+1}/{len(sample_files)}: {file_info.get('name', 'unknown')}")
            
            file_analysis = self._analyze_single_file(file_info, payer_name)
            analysis_report["file_analysis"].append(file_analysis)
        
        # Identify common patterns
        analysis_report["structure_patterns"] = self._identify_patterns(analysis_report["file_analysis"])
        
        # Generate recommendations
        analysis_report["recommendations"] = self._generate_recommendations(analysis_report)
        
        # Save analysis report
        report_path = payer_dir / f"{payer_name}_analysis.json"
        with open(report_path, 'w') as f:
            json.dump(analysis_report, f, indent=2)
        
        self.logger.info(f"✅ Analysis complete. Report saved to: {report_path}")
        return analysis_report
    
    def _analyze_single_file(self, file_info: Dict[str, Any], payer_name: str) -> Dict[str, Any]:
        """Analyze a single MRF file for structure patterns."""
        file_analysis = {
            "file_name": file_info.get("name", "unknown"),
            "file_size": file_info.get("size", 0),
            "compression": file_info.get("compression", "unknown"),
            "structure_analysis": {},
            "sample_records": [],
            "issues": []
        }
        
        try:
            # Try to parse a small sample of the file
            sample_records = []
            record_count = 0
            
            for record in stream_parse_enhanced(file_info["url"], max_records=10):
                sample_records.append(record)
                record_count += 1
                
                if record_count >= 10:
                    break
            
            file_analysis["sample_records"] = sample_records
            file_analysis["structure_analysis"] = self._analyze_record_structure(sample_records)
            
        except Exception as e:
            file_analysis["issues"].append(f"Failed to parse file: {e}")
        
        return file_analysis
    
    def _analyze_record_structure(self, records: List[Dict]) -> Dict[str, Any]:
        """Analyze the structure of parsed records."""
        if not records:
            return {}
        
        structure = {
            "record_types": set(),
            "field_patterns": {},
            "nested_structures": {},
            "provider_patterns": [],
            "rate_patterns": []
        }
        
        for record in records:
            # Identify record types
            if "in_network" in record:
                structure["record_types"].add("in_network")
            if "provider_references" in record:
                structure["record_types"].add("provider_references")
            if "provider_groups" in record:
                structure["record_types"].add("provider_groups")
            
            # Analyze in_network structure
            if "in_network" in record:
                in_network = record["in_network"]
                for item in in_network:
                    # Analyze provider groups
                    if "provider_groups" in item:
                        for pg in item["provider_groups"]:
                            if "npi" in pg:
                                structure["provider_patterns"].append("direct_npi")
                            elif "tin" in pg:
                                structure["provider_patterns"].append("tin_reference")
                    
                    # Analyze negotiated rates
                    if "negotiated_rates" in item:
                        for rate in item["negotiated_rates"]:
                            rate_structure = {
                                "has_service_code": "service_code" in rate,
                                "has_rate": "negotiated_rate" in rate,
                                "has_effective_date": "effective_date" in rate,
                                "has_expiration_date": "expiration_date" in rate
                            }
                            structure["rate_patterns"].append(rate_structure)
        
        # Convert sets to lists for JSON serialization
        structure["record_types"] = list(structure["record_types"])
        structure["provider_patterns"] = list(set(structure["provider_patterns"]))
        
        return structure
    
    def _identify_patterns(self, file_analyses: List[Dict]) -> Dict[str, Any]:
        """Identify common patterns across all analyzed files."""
        patterns = {
            "common_record_types": set(),
            "provider_patterns": set(),
            "rate_patterns": set(),
            "compression_types": set(),
            "file_size_range": {"min": float('inf'), "max": 0}
        }
        
        for analysis in file_analyses:
            # Aggregate record types
            if "structure_analysis" in analysis:
                sa = analysis["structure_analysis"]
                patterns["common_record_types"].update(sa.get("record_types", []))
                patterns["provider_patterns"].update(sa.get("provider_patterns", []))
                patterns["rate_patterns"].update(sa.get("rate_patterns", []))
            
            # Track file sizes
            size = analysis.get("file_size", 0)
            patterns["file_size_range"]["min"] = min(patterns["file_size_range"]["min"], size)
            patterns["file_size_range"]["max"] = max(patterns["file_size_range"]["max"], size)
            
            # Track compression
            patterns["compression_types"].add(analysis.get("compression", "unknown"))
        
        # Convert sets to lists
        for key in ["common_record_types", "provider_patterns", "compression_types"]:
            patterns[key] = list(patterns[key])
        
        return patterns
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on the analysis."""
        recommendations = []
        patterns = analysis.get("structure_patterns", {})
        
        # Check for standard structure
        if "in_network" in patterns.get("common_record_types", []):
            recommendations.append("✅ Standard in_network structure detected")
        else:
            recommendations.append("⚠️ Non-standard structure - may need custom handler")
        
        # Check provider patterns
        provider_patterns = patterns.get("provider_patterns", [])
        if "direct_npi" in provider_patterns:
            recommendations.append("✅ Direct NPI references found")
        if "tin_reference" in provider_patterns:
            recommendations.append("⚠️ TIN references found - may need provider lookup")
        
        # Check rate patterns
        rate_patterns = patterns.get("rate_patterns", [])
        if rate_patterns:
            sample_rate = rate_patterns[0]
            if sample_rate.get("has_service_code") and sample_rate.get("has_rate"):
                recommendations.append("✅ Standard rate structure detected")
            else:
                recommendations.append("⚠️ Non-standard rate structure - may need custom parsing")
        
        # Compression recommendations
        compression_types = patterns.get("compression_types", [])
        if "gzip" in compression_types:
            recommendations.append("✅ Standard gzip compression")
        elif "unknown" in compression_types:
            recommendations.append("⚠️ Unknown compression - may need custom decompression")
        
        return recommendations
    
    def create_handler_template(self, payer_name: str, analysis_report: Dict[str, Any]) -> str:
        """
        Step 2: Create a custom handler template based on the analysis.
        
        Args:
            payer_name: Name of the payer
            analysis_report: Analysis report from step 1
        
        Returns:
            Path to the created handler file
        """
        self.logger.info(f"📝 Creating handler template for: {payer_name}")
        
        # Create handler file
        handler_path = self.work_dir / "handlers" / f"{payer_name}_handler.py"
        
        # Generate handler code based on analysis
        handler_code = self._generate_handler_code(payer_name, analysis_report)
        
        with open(handler_path, 'w') as f:
            f.write(handler_code)
        
        self.logger.info(f"✅ Handler template created: {handler_path}")
        return str(handler_path)
    
    def _generate_handler_code(self, payer_name: str, analysis_report: Dict[str, Any]) -> str:
        """Generate handler code based on analysis results."""
        patterns = analysis_report.get("structure_patterns", {})
        recommendations = analysis_report.get("recommendations", [])
        
        # Determine handler complexity
        needs_custom_parsing = any("⚠️" in rec for rec in recommendations)
        
        handler_code = f'''from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("{payer_name}")
class {payer_name.title()}Handler(PayerHandler):
    """Handler for {payer_name.title()} payer MRF files."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Custom parsing for {payer_name} in_network records.
        
        Analysis findings:
        - Record types: {patterns.get('common_record_types', [])}
        - Provider patterns: {patterns.get('provider_patterns', [])}
        - Compression: {patterns.get('compression_types', [])}
        
        Recommendations:
        {chr(10).join(f"        - {rec}" for rec in recommendations)}
        """
        # TODO: Implement custom parsing logic based on analysis
        # 
        # Common customizations:
        # 1. Handle non-standard provider group structures
        # 2. Normalize rate formats
        # 3. Handle special field mappings
        # 4. Deal with compression issues
        
        # Example customizations (uncomment and modify as needed):
        
        # if "negotiated_rates" in record:
        #     for group in record.get("negotiated_rates", []):
        #         # Custom rate processing
        #         pass
        
        # if "provider_groups" in record:
        #     for group in record.get("provider_groups", []):
        #         # Custom provider group processing
        #         pass
        
        return [record]
'''
        
        return handler_code
    
    def test_handler(self, payer_name: str, index_url: str, 
                    handler_path: str, test_size: int = 5) -> Dict[str, Any]:
        """
        Step 3: Test the custom handler with sample data.
        
        Args:
            payer_name: Name of the payer
            index_url: URL to the payer's MRF index
            handler_path: Path to the handler file
            test_size: Number of files to test
        
        Returns:
            Test results and validation report
        """
        self.logger.info(f"🧪 Testing handler for: {payer_name}")
        
        # Import the handler
        try:
            # Add handler directory to path temporarily
            handler_dir = Path(handler_path).parent
            sys.path.insert(0, str(handler_dir))
            
            # Import the handler module
            handler_module = __import__(f"{payer_name}_handler")
            
            # Get the handler class
            handler_class = getattr(handler_module, f"{payer_name.title()}Handler")
            handler = handler_class()
            
        except Exception as e:
            self.logger.error(f"Failed to import handler: {e}")
            return {"error": f"Handler import failed: {e}"}
        
        # Test the handler
        test_results = {
            "payer_name": payer_name,
            "test_timestamp": datetime.now().isoformat(),
            "files_tested": 0,
            "records_processed": 0,
            "errors": [],
            "warnings": [],
            "success_rate": 0.0,
            "sample_output": []
        }
        
        try:
            # Get MRF files
            mrf_files = list_mrf_blobs_enhanced(index_url)
            test_files = mrf_files[:test_size]
            
            total_records = 0
            successful_records = 0
            
            for file_info in test_files:
                self.logger.info(f"Testing file: {file_info.get('name', 'unknown')}")
                
                try:
                    file_records = 0
                    file_success = 0
                    
                    for record in stream_parse_enhanced(file_info["url"], max_records=20):
                        try:
                            # Test the handler
                            processed_records = handler.parse_in_network(record)
                            
                            # Validate the output
                            for processed_record in processed_records:
                                if self._validate_processed_record(processed_record):
                                    file_success += 1
                                    if len(test_results["sample_output"]) < 3:
                                        test_results["sample_output"].append(processed_record)
                            
                            file_records += 1
                            total_records += 1
                            
                        except Exception as e:
                            test_results["errors"].append(f"Record processing error: {e}")
                    
                    successful_records += file_success
                    test_results["files_tested"] += 1
                    
                except Exception as e:
                    test_results["errors"].append(f"File processing error: {e}")
            
            test_results["records_processed"] = total_records
            test_results["success_rate"] = (successful_records / total_records * 100) if total_records > 0 else 0
            
        except Exception as e:
            test_results["errors"].append(f"Test setup error: {e}")
        
        # Save test results
        test_report_path = self.work_dir / "tests" / f"{payer_name}_test_results.json"
        with open(test_report_path, 'w') as f:
            json.dump(test_results, f, indent=2)
        
        self.logger.info(f"✅ Test complete. Results saved to: {test_report_path}")
        return test_results
    
    def _validate_processed_record(self, record: Dict[str, Any]) -> bool:
        """Validate that a processed record has the expected structure."""
        # Basic validation - check for required fields
        if "in_network" not in record:
            return False
        
        # Check that in_network items have basic structure
        for item in record.get("in_network", []):
            if not isinstance(item, dict):
                return False
            
            # Should have either negotiated_rates or provider_groups
            if "negotiated_rates" not in item and "provider_groups" not in item:
                return False
        
        return True
    
    def integrate_to_production(self, payer_name: str, handler_path: str, 
                              index_url: str, config_file: str = "production_config.yaml") -> Dict[str, Any]:
        """
        Step 4: Integrate the new payer into the production pipeline.
        
        Args:
            payer_name: Name of the payer
            handler_path: Path to the handler file
            index_url: URL to the payer's MRF index
            config_file: Path to the production config file
        
        Returns:
            Integration results
        """
        self.logger.info(f"🚀 Integrating {payer_name} into production pipeline")
        
        integration_results = {
            "payer_name": payer_name,
            "integration_timestamp": datetime.now().isoformat(),
            "steps_completed": [],
            "errors": [],
            "warnings": []
        }
        
        try:
            # Step 1: Copy handler to production
            production_handler_path = Path("src/tic_mrf_scraper/payers") / f"{payer_name}.py"
            
            # Read the handler file
            with open(handler_path, 'r') as f:
                handler_content = f.read()
            
            # Write to production location
            with open(production_handler_path, 'w') as f:
                f.write(handler_content)
            
            integration_results["steps_completed"].append("Handler copied to production")
            
            # Step 2: Update production config
            config_path = Path(config_file)
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                
                # Add payer to endpoints
                if "payer_endpoints" not in config:
                    config["payer_endpoints"] = {}
                
                config["payer_endpoints"][payer_name] = index_url
                
                # Write updated config
                with open(config_path, 'w') as f:
                    yaml.dump(config, f, default_flow_style=False)
                
                integration_results["steps_completed"].append("Production config updated")
            else:
                integration_results["warnings"].append(f"Config file {config_file} not found")
            
            # Step 3: Test integration
            try:
                # Test that the handler can be imported
                from tic_mrf_scraper.payers import get_handler
                handler = get_handler(payer_name)
                integration_results["steps_completed"].append("Handler import test passed")
                
            except Exception as e:
                integration_results["errors"].append(f"Handler import test failed: {e}")
            
        except Exception as e:
            integration_results["errors"].append(f"Integration failed: {e}")
        
        # Save integration report
        integration_report_path = self.work_dir / "reports" / f"{payer_name}_integration.json"
        with open(integration_report_path, 'w') as f:
            json.dump(integration_results, f, indent=2)
        
        self.logger.info(f"✅ Integration complete. Report saved to: {integration_report_path}")
        return integration_results
    
    def run_full_workflow(self, payer_name: str, index_url: str, 
                         auto_integrate: bool = False) -> Dict[str, Any]:
        """
        Run the complete development workflow for a new payer.
        
        Args:
            payer_name: Name of the payer
            index_url: URL to the payer's MRF index
            auto_integrate: Whether to automatically integrate to production
        
        Returns:
            Complete workflow results
        """
        self.logger.info(f"🔄 Starting full workflow for payer: {payer_name}")
        
        workflow_results = {
            "payer_name": payer_name,
            "workflow_timestamp": datetime.now().isoformat(),
            "steps": {}
        }
        
        try:
            # Step 1: Analyze payer sample
            self.logger.info("Step 1: Analyzing payer sample...")
            analysis = self.analyze_payer_sample(payer_name, index_url)
            workflow_results["steps"]["analysis"] = analysis
            
            if "error" in analysis:
                self.logger.error(f"Analysis failed: {analysis['error']}")
                return workflow_results
            
            # Step 2: Create handler template
            self.logger.info("Step 2: Creating handler template...")
            handler_path = self.create_handler_template(payer_name, analysis)
            workflow_results["steps"]["handler_creation"] = {"handler_path": handler_path}
            
            # Step 3: Test handler
            self.logger.info("Step 3: Testing handler...")
            test_results = self.test_handler(payer_name, index_url, handler_path)
            workflow_results["steps"]["testing"] = test_results
            
            # Step 4: Integrate to production (if requested)
            if auto_integrate:
                self.logger.info("Step 4: Integrating to production...")
                integration_results = self.integrate_to_production(payer_name, handler_path, index_url)
                workflow_results["steps"]["integration"] = integration_results
            
            self.logger.info("✅ Full workflow completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Workflow failed: {e}")
            workflow_results["error"] = str(e)
        
        # Save complete workflow results
        workflow_report_path = self.work_dir / "reports" / f"{payer_name}_full_workflow.json"
        with open(workflow_report_path, 'w') as f:
            json.dump(workflow_results, f, indent=2)
        
        return workflow_results


def main():
    """Main CLI interface for the payer development workflow."""
    parser = argparse.ArgumentParser(description="Payer Development Workflow")
    parser.add_argument("--payer-name", required=True, help="Name of the payer")
    parser.add_argument("--index-url", required=True, help="URL to the payer's MRF index")
    parser.add_argument("--workflow", choices=["analyze", "create-handler", "test", "integrate", "full"], 
                       default="full", help="Workflow step to run")
    parser.add_argument("--auto-integrate", action="store_true", 
                       help="Automatically integrate to production after testing")
    parser.add_argument("--sample-size", type=int, default=3, 
                       help="Number of files to sample for analysis")
    parser.add_argument("--test-size", type=int, default=5, 
                       help="Number of files to test")
    parser.add_argument("--work-dir", default="payer_development", 
                       help="Working directory for development")
    
    args = parser.parse_args()
    
    # Create workflow instance
    workflow = PayerDevelopmentWorkflow(args.work_dir)
    
    if args.workflow == "analyze":
        analysis = workflow.analyze_payer_sample(args.payer_name, args.index_url, args.sample_size)
        print(f"Analysis complete. Check {args.work_dir}/samples/{args.payer_name}/")
        
    elif args.workflow == "create-handler":
        analysis = workflow.analyze_payer_sample(args.payer_name, args.index_url, args.sample_size)
        handler_path = workflow.create_handler_template(args.payer_name, analysis)
        print(f"Handler created: {handler_path}")
        
    elif args.workflow == "test":
        handler_path = f"{args.work_dir}/handlers/{args.payer_name}_handler.py"
        if not os.path.exists(handler_path):
            print(f"Handler not found: {handler_path}")
            print("Run with --workflow create-handler first")
            return
        test_results = workflow.test_handler(args.payer_name, args.index_url, handler_path, args.test_size)
        print(f"Test complete. Check {args.work_dir}/tests/")
        
    elif args.workflow == "integrate":
        handler_path = f"{args.work_dir}/handlers/{args.payer_name}_handler.py"
        if not os.path.exists(handler_path):
            print(f"Handler not found: {handler_path}")
            return
        integration_results = workflow.integrate_to_production(args.payer_name, handler_path, args.index_url)
        print("Integration complete. Check the reports directory for details.")
        
    elif args.workflow == "full":
        workflow_results = workflow.run_full_workflow(args.payer_name, args.index_url, args.auto_integrate)
        print(f"Full workflow complete. Check {args.work_dir}/reports/ for detailed results.")


if __name__ == "__main__":
    main() 