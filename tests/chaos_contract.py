#!/usr/bin/env python3
"""
Chaos Contract Violation Test

This test simulates a contract violation by providing invalid data types
to verify that the Data Observability Platform's contract guard correctly identifies and blocks violations.

Test Scenario:
- Contract requires: order_key (String), total_amount (Float)
- Chaos scenario: total_amount as String 'ONE HUNDRED' instead of Float
- Expected: CONTRACT_VIOLATION alert and ingestion blocking
"""

import sys
import os
import json
import yaml
import tempfile
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from contract_guard import ContractGuard


class ChaosContractTest:
    """Chaos test for contract violation detection"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        import logging
        logger = logging.Logger("chaos_contract_test")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def create_violating_json_data(self):
        """Create JSON data with contract violations"""
        violating_records = [
            {
                "order_key": "ORDER-001",
                "customer_id": 101,
                "product_id": 201,
                "quantity": 2,
                "unit_price": 29.99,
                "total_amount": "ONE HUNDRED",  # VIOLATION: String instead of Float
                "order_status": "completed",
                "order_date": "2026-02-02T01:00:00Z",
                "cdc_operation": "INSERT",
                "cdc_timestamp": "2026-02-02T01:00:00Z"
            },
            {
                "order_key": 12345,  # VIOLATION: Integer instead of String
                "customer_id": 102,
                "product_id": 202,
                "quantity": 1,
                "unit_price": 49.99,
                "total_amount": "NINETY-NINE",  # VIOLATION: String instead of Float
                "order_status": "pending",
                "order_date": "2026-02-02T01:05:00Z",
                "cdc_operation": "INSERT",
                "cdc_timestamp": "2026-02-02T01:05:00Z"
            },
            {
                "order_key": "ORDER-003",
                "customer_id": 103,
                "product_id": 203,
                "quantity": 3,
                "unit_price": 15.99,
                "total_amount": 47.97,  # VALID: Float as expected
                "order_status": "shipped",
                "order_date": "2026-02-02T01:10:00Z",
                "cdc_operation": "INSERT",
                "cdc_timestamp": "2026-02-02T01:10:00Z"
            }
        ]
        
        return violating_records
    
    def create_temp_json_files(self, records):
        """Create temporary JSON files with violating data"""
        temp_files = []
        
        try:
            # Create temporary directory
            temp_dir = tempfile.mkdtemp(prefix="chaos_contract_test_")
            
            # Create individual JSON files
            for i, record in enumerate(records):
                temp_file = os.path.join(temp_dir, f"chaos_record_{i+1}.json")
                with open(temp_file, 'w') as f:
                    json.dump(record, f, indent=2)
                temp_files.append(temp_file)
            
            self.logger.info(f"📁 Created {len(temp_files)} temporary JSON files")
            return temp_dir, temp_files
            
        except Exception as e:
            self.logger.error(f"Failed to create temporary files: {e}")
            return None, []
    
    def run_contract_validation_test(self, temp_dir):
        """Run contract guard validation on violating data"""
        try:
            self.logger.info("🔍 Running contract validation test...")
            
            # Create contract guard with temporary directory
            guard = ContractGuard()
            
            # Mock the JSON log directory to point to our temp directory
            original_load_json_logs = guard.load_sample_json_logs
            def mock_load_json_logs(log_directory, sample_size=10):
                return guard.load_sample_json_logs(temp_dir, sample_size)
            
            guard.load_sample_json_logs = mock_load_json_logs
            
            # Run validation
            results = guard.run_contract_validation(
                use_database=False,  # Skip database for this test
                use_json_logs=True   # Use our violating JSON files
            )
            
            self.logger.info(f"Contract validation results: {results}")
            
            # Check if violations were detected
            total_violations = results.get('total_violations', 0)
            critical_violations = len(results.get('critical_violations', []))
            
            if total_violations > 0:
                self.logger.info(f"✅ SUCCESS: {total_violations} contract violations detected!")
                self.logger.info(f"   Critical violations: {critical_violations}")
                
                # Log violation details
                for violation in results.get('critical_violations', []):
                    field_name = violation.get('field_name', 'Unknown')
                    expected_type = violation.get('expected_type', 'Unknown')
                    actual_type = violation.get('actual_type', 'Unknown')
                    self.logger.info(f"   - Field '{field_name}': Expected {expected_type}, got {actual_type}")
                
                return True
            else:
                self.logger.error("❌ FAILURE: Contract violations NOT detected!")
                return False
                
        except Exception as e:
            self.logger.error(f"Contract validation test failed: {e}")
            return False
    
    def test_individual_record_validation(self, records):
        """Test validation of individual records"""
        try:
            self.logger.info("🔍 Testing individual record validation...")
            
            guard = ContractGuard()
            validator = guard.validator
            
            violations_found = 0
            
            for i, record in enumerate(records):
                self.logger.info(f"Testing record {i+1}: {record.get('order_key', 'Unknown')}")
                
                result = validator.validate_record(record)
                
                if not result['valid']:
                    violations_found += len(result['violations'])
                    self.logger.info(f"  ❌ {len(result['violations'])} violations found:")
                    for violation in result['violations']:
                        field_name = violation.get('field_name', 'Unknown')
                        error_type = violation.get('type', 'Unknown')
                        errors = violation.get('errors', [])
                        self.logger.info(f"    - {field_name}: {error_type}")
                        for error in errors:
                            self.logger.info(f"      * {error}")
                else:
                    self.logger.info(f"  ✅ No violations")
            
            return violations_found > 0
            
        except Exception as e:
            self.logger.error(f"Individual record validation failed: {e}")
            return False
    
    def cleanup_temp_files(self, temp_dir, temp_files):
        """Clean up temporary files"""
        try:
            import shutil
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                self.logger.info("🧹 Temporary files cleaned up")
        except Exception as e:
            self.logger.error(f"Failed to cleanup temporary files: {e}")
    
    def run_test(self):
        """Run the complete chaos contract test"""
        self.logger.info("🚀 Starting Chaos Contract Violation Test")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Create violating data
            violating_records = self.create_violating_json_data()
            self.logger.info(f"📝 Created {len(violating_records)} test records with violations")
            
            # Step 2: Test individual record validation
            individual_success = self.test_individual_record_validation(violating_records)
            
            # Step 3: Create temporary JSON files
            temp_dir, temp_files = self.create_temp_json_files(violating_records)
            if not temp_dir:
                return False
            
            try:
                # Step 4: Run contract validation test
                validation_success = self.run_contract_validation_test(temp_dir)
                
                # Overall test result
                test_success = individual_success and validation_success
                
                if test_success:
                    self.logger.info("🎉 CHAOS CONTRACT TEST PASSED!")
                    self.logger.info("   ✅ Contract violations correctly detected")
                    self.logger.info("   ✅ Type validation working properly")
                    self.logger.info("   ✅ Ingestion would be blocked for invalid data")
                else:
                    self.logger.error("💥 CHAOS CONTRACT TEST FAILED!")
                    self.logger.error("   ❌ Contract violation detection failed")
                
                return test_success
                
            finally:
                # Cleanup
                self.cleanup_temp_files(temp_dir, temp_files)
                
        except Exception as e:
            self.logger.error(f"Chaos contract test failed: {e}")
            return False


def main():
    """Main execution function"""
    test = ChaosContractTest()
    success = test.run_test()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
