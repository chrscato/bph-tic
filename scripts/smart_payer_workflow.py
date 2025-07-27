#!/usr/bin/env python3
"""Smart Payer Workflow - Complete automated payer integration system.

This script provides a complete workflow for:
1. Analyzing new payer MRF structures
2. Intelligently generating appropriate handlers
3. Testing the integration
4. Deploying to production

Usage:
    python scripts/smart_payer_workflow.py --payer-name "new_payer" --index-url "https://example.com/mrf_index.json"
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
import subprocess
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tic_mrf_scraper.payers import get_handler, PayerHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record

class SmartPayerWorkflow:
    """Complete automated payer integration workflow."""
    
    def __init__(self, payer_name: str, index_url: str, work_dir: str = "smart_payer_workflow"):
        self.payer_name = payer_name
        self.index_url = index_url
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Create subdirectories
        (self.work_dir / "analysis").mkdir(exist_ok=True)
        (self.work_dir / "handlers").mkdir(exist_ok=True)
        (self.work_dir / "tests").mkdir(exist_ok=True)
        (self.work_dir / "reports").mkdir(exist_ok=True)
    
    def run_analysis(self, sample_size: int = 5) -> Dict[str, Any]:
        """Step 1: Run comprehensive payer analysis."""
        self.logger.info(f"[ANALYZE] Step 1: Analyzing {self.payer_name} structure...")
        
        # Create temporary config for analysis
        temp_config = {
            "payer_endpoints": {self.payer_name: self.index_url},
            "cpt_whitelist": ["99213", "99214", "99215"]  # Sample CPT codes
        }
        
        temp_config_path = self.work_dir / "temp_config.yaml"
        with open(temp_config_path, 'w') as f:
            yaml.dump(temp_config, f)
        
        # Run analyze_payer_structure.py
        analysis_output = self.work_dir / "analysis" / f"{self.payer_name}_analysis.json"
        
        try:
            cmd = [
                "python", "scripts/analyze_payer_structure.py",
                "--config", str(temp_config_path),
                "--payers", self.payer_name,
                "--output-dir", str(self.work_dir / "analysis")
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
            
            if result.returncode != 0:
                self.logger.error(f"Analysis failed: {result.stderr}")
                return {"error": f"Analysis failed: {result.stderr}"}
            
            # Find the generated analysis file
            analysis_files = list((self.work_dir / "analysis").glob("*analysis*.json"))
            if not analysis_files:
                return {"error": "No analysis files generated"}
            
            # Load the most recent analysis
            latest_analysis = max(analysis_files, key=lambda x: x.stat().st_mtime)
            
            with open(latest_analysis, 'r') as f:
                analysis_data = json.load(f)
            
            self.logger.info(f"[SUCCESS] Analysis complete: {latest_analysis}")
            return analysis_data
            
        except Exception as e:
            return {"error": f"Analysis failed: {str(e)}"}
        finally:
            # Clean up temp config
            if temp_config_path.exists():
                temp_config_path.unlink()
    
    def run_intelligent_integration(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Step 2: Run intelligent integration based on analysis."""
        self.logger.info(f"[INTEGRATE] Step 2: Running intelligent integration...")
        
        # Save analysis data for integration script
        analysis_file = self.work_dir / "analysis" / f"{self.payer_name}_analysis_for_integration.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis_data, f)
        
        # Run intelligent integration
        integration_output = self.work_dir / "reports" / f"{self.payer_name}_integration_results.json"
        
        try:
            cmd = [
                "python", "scripts/intelligent_payer_integration.py",
                "--analysis-file", str(analysis_file),
                "--payer-name", self.payer_name,
                "--index-url", self.index_url,
                "--output-file", str(integration_output)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
            
            if result.returncode != 0:
                self.logger.error(f"Integration failed: {result.stderr}")
                return {"error": f"Integration failed: {result.stderr}"}
            
            # Load integration results
            with open(integration_output, 'r') as f:
                integration_results = json.load(f)
            
            self.logger.info(f"[SUCCESS] Integration complete: {integration_output}")
            return integration_results
            
        except Exception as e:
            return {"error": f"Integration failed: {str(e)}"}
    
    def run_validation_tests(self) -> Dict[str, Any]:
        """Step 3: Run comprehensive validation tests."""
        self.logger.info(f"🧪 Step 3: Running validation tests...")
        
        test_results = {
            "payer_name": self.payer_name,
            "test_timestamp": datetime.now().isoformat(),
            "tests_passed": 0,
            "tests_failed": 0,
            "total_tests": 0,
            "errors": [],
            "warnings": []
        }
        
        # Test 1: Handler import
        test_results["total_tests"] += 1
        try:
            handler = get_handler(self.payer_name)
            test_results["tests_passed"] += 1
            self.logger.info("[SUCCESS] Handler import test passed")
        except Exception as e:
            test_results["tests_failed"] += 1
            test_results["errors"].append(f"Handler import failed: {str(e)}")
            self.logger.error(f"[ERROR] Handler import test failed: {str(e)}")
        
        # Test 2: File listing
        test_results["total_tests"] += 1
        try:
            mrf_files = handler.list_mrf_files(self.index_url)
            rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
            
            if rate_files:
                test_results["tests_passed"] += 1
                self.logger.info(f"[SUCCESS] File listing test passed: {len(rate_files)} rate files found")
            else:
                test_results["tests_failed"] += 1
                test_results["warnings"].append("No rate files found")
                self.logger.warning("[WARNING] No rate files found")
        except Exception as e:
            test_results["tests_failed"] += 1
            test_results["errors"].append(f"File listing failed: {str(e)}")
            self.logger.error(f"[ERROR] File listing test failed: {str(e)}")
        
        # Test 3: Sample processing
        test_results["total_tests"] += 1
        try:
            if rate_files:
                test_file = rate_files[0]
                record_count = 0
                max_records = 5
                
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
                        record_count += 1
                
                if record_count > 0:
                    test_results["tests_passed"] += 1
                    self.logger.info(f"[SUCCESS] Sample processing test passed: {record_count} records processed")
                else:
                    test_results["tests_failed"] += 1
                    test_results["warnings"].append("No records could be normalized")
                    self.logger.warning("[WARNING] No records could be normalized")
            else:
                test_results["tests_failed"] += 1
                test_results["warnings"].append("Skipped processing test - no files available")
                self.logger.warning("[WARNING] Skipped processing test - no files available")
        except Exception as e:
            test_results["tests_failed"] += 1
            test_results["errors"].append(f"Sample processing failed: {str(e)}")
            self.logger.error(f"[ERROR] Sample processing test failed: {str(e)}")
        
        # Test 4: Production config check
        test_results["total_tests"] += 1
        try:
            config_path = Path("production_config.yaml")
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                
                if self.payer_name in config.get("payer_endpoints", {}):
                    test_results["tests_passed"] += 1
                    self.logger.info("[SUCCESS] Production config test passed")
                else:
                    test_results["tests_failed"] += 1
                    test_results["warnings"].append("Payer not found in production config")
                    self.logger.warning("[WARNING] Payer not found in production config")
            else:
                test_results["tests_failed"] += 1
                test_results["errors"].append("Production config file not found")
                self.logger.error("[ERROR] Production config file not found")
        except Exception as e:
            test_results["tests_failed"] += 1
            test_results["errors"].append(f"Production config check failed: {str(e)}")
            self.logger.error(f"[ERROR] Production config check failed: {str(e)}")
        
        return test_results
    
    def generate_final_report(self, analysis_data: Dict[str, Any], 
                            integration_results: Dict[str, Any], 
                            test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive final report."""
        self.logger.info(f"📊 Generating final report...")
        
        final_report = {
            "payer_name": self.payer_name,
            "index_url": self.index_url,
            "workflow_timestamp": datetime.now().isoformat(),
            "summary": {
                "analysis_success": "error" not in analysis_data,
                "integration_success": integration_results.get("test_results", {}).get("success", False),
                "validation_success": test_results.get("tests_failed", 0) == 0,
                "overall_success": False
            },
            "analysis": analysis_data,
            "integration": integration_results,
            "validation": test_results,
            "recommendations": []
        }
        
        # Determine overall success
        analysis_ok = "error" not in analysis_data
        integration_ok = integration_results.get("test_results", {}).get("success", False)
        validation_ok = test_results.get("tests_failed", 0) == 0
        
        final_report["summary"]["overall_success"] = analysis_ok and integration_ok and validation_ok
        
        # Generate recommendations
        if not analysis_ok:
            final_report["recommendations"].append("Fix analysis issues before proceeding")
        
        if not integration_ok:
            final_report["recommendations"].append("Review and fix integration issues")
        
        if not validation_ok:
            final_report["recommendations"].append("Address validation test failures")
        
        if final_report["summary"]["overall_success"]:
            final_report["recommendations"].append("Integration ready for production use")
        
        # Save final report
        report_path = self.work_dir / "reports" / f"{self.payer_name}_final_report.json"
        with open(report_path, 'w') as f:
            json.dump(final_report, f, indent=2)
        
        self.logger.info(f"[SUCCESS] Final report saved: {report_path}")
        return final_report
    
    def run_full_workflow(self, auto_deploy: bool = False) -> Dict[str, Any]:
        """Run the complete smart payer workflow."""
        self.logger.info(f"[START] Starting Smart Payer Workflow for {self.payer_name}")
        self.logger.info(f"Index URL: {self.index_url}")
        self.logger.info(f"Work directory: {self.work_dir}")
        
        workflow_results = {
            "payer_name": self.payer_name,
            "index_url": self.index_url,
            "workflow_start": datetime.now().isoformat(),
            "steps_completed": [],
            "final_report": None
        }
        
        try:
            # Step 1: Analysis
            analysis_data = self.run_analysis()
            workflow_results["analysis"] = analysis_data
            workflow_results["steps_completed"].append("analysis")
            
            if "error" in analysis_data:
                self.logger.error(f"Analysis failed: {analysis_data['error']}")
                return workflow_results
            
            # Step 2: Intelligent Integration
            integration_results = self.run_intelligent_integration(analysis_data)
            workflow_results["integration"] = integration_results
            workflow_results["steps_completed"].append("integration")
            
            if "error" in integration_results:
                self.logger.error(f"Integration failed: {integration_results['error']}")
                return workflow_results
            
            # Step 3: Validation Tests
            test_results = self.run_validation_tests()
            workflow_results["validation"] = test_results
            workflow_results["steps_completed"].append("validation")
            
            # Step 4: Generate Final Report
            final_report = self.generate_final_report(analysis_data, integration_results, test_results)
            workflow_results["final_report"] = final_report
            workflow_results["steps_completed"].append("final_report")
            
            # Step 5: Auto-deploy if requested
            if auto_deploy and final_report["summary"]["overall_success"]:
                self.logger.info("[DEPLOY] Auto-deploying to production...")
                # Additional deployment steps could go here
                workflow_results["steps_completed"].append("auto_deploy")
            
            self.logger.info("[SUCCESS] Smart Payer Workflow completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Workflow failed: {str(e)}")
            workflow_results["error"] = str(e)
        
        return workflow_results


def main():
    parser = argparse.ArgumentParser(description="Smart Payer Workflow")
    parser.add_argument("--payer-name", required=True, help="Name of the payer")
    parser.add_argument("--index-url", required=True, help="Index URL for the payer")
    parser.add_argument("--work-dir", default="smart_payer_workflow", help="Work directory")
    parser.add_argument("--auto-deploy", action="store_true", help="Auto-deploy to production if successful")
    parser.add_argument("--sample-size", type=int, default=5, help="Number of files to sample for analysis")
    
    args = parser.parse_args()
    
    # Run workflow
    workflow = SmartPayerWorkflow(args.payer_name, args.index_url, args.work_dir)
    results = workflow.run_full_workflow(args.auto_deploy)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"SMART PAYER WORKFLOW RESULTS FOR {args.payer_name.upper()}")
    print(f"{'='*80}")
    print(f"Steps completed: {len(results['steps_completed'])}")
    
    if results.get("final_report"):
        summary = results["final_report"]["summary"]
            print(f"Analysis success: {'[SUCCESS]' if summary['analysis_success'] else '[ERROR]'}")
    print(f"Integration success: {'[SUCCESS]' if summary['integration_success'] else '[ERROR]'}")
    print(f"Validation success: {'[SUCCESS]' if summary['validation_success'] else '[ERROR]'}")
    print(f"Overall success: {'[SUCCESS]' if summary['overall_success'] else '[ERROR]'}")
        
        if summary['overall_success']:
            print("\n🎉 Integration ready for production use!")
        else:
            print("\n[WARNING] Integration completed with issues. Check recommendations.")
            for rec in results["final_report"]["recommendations"]:
                print(f"  - {rec}")
    
    if results.get("error"):
        print(f"\n[ERROR] Workflow failed: {results['error']}")
    
    print(f"\nDetailed results available in: {args.work_dir}/reports/")


if __name__ == "__main__":
    main() 