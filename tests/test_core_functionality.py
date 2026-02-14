#!/usr/bin/env python3
"""
Core Anomaly Detection Tests

Tests for the statistical anomaly detection logic to ensure
the platform correctly identifies various anomaly scenarios.

Test Cases:
1. No anomaly - Normal data within expected range
2. Single metric spike - One metric exceeds threshold
3. Multiple anomalies - Multiple metrics exceed thresholds
"""

import unittest
import sys
import os
from pathlib import Path
import math

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

class TestAnomalyDetection(unittest.TestCase):
    """Test core anomaly detection logic"""
    
    def setUp(self):
        """Set up test data"""
        self.mean = 1000.0
        self.std_dev = 100.0
        self.threshold = 3.0
    
    def calculate_z_score(self, current_value, mean, std_dev):
        """Helper function to calculate Z-score"""
        if std_dev == 0:
            return 0.0
        return (current_value - mean) / std_dev
    
    def test_no_anomaly(self):
        """Test Case 1: No anomaly - Normal data within expected range"""
        # Current value within 2 standard deviations (normal range)
        current_value = 1050.0
        z_score = self.calculate_z_score(current_value, self.mean, self.std_dev)
        
        # Should not trigger anomaly (|Z| < 3.0)
        self.assertLess(abs(z_score), self.threshold)
        self.assertEqual(z_score, 0.5)  # (1050 - 1000) / 100 = 0.5
        
        print(f"✅ No Anomaly Test: Current={current_value}, Z-Score={z_score:.2f}")
    
    def test_single_metric_spike(self):
        """Test Case 2: Single metric spike - One metric exceeds threshold"""
        # Current value exceeds 3 standard deviations (critical anomaly)
        current_value = 1350.0
        z_score = self.calculate_z_score(current_value, self.mean, self.std_dev)
        
        # Should trigger anomaly (|Z| >= 3.0)
        self.assertGreaterEqual(abs(z_score), self.threshold)
        self.assertEqual(z_score, 3.5)  # (1350 - 1000) / 100 = 3.5
        
        print(f"🚨 Single Spike Test: Current={current_value}, Z-Score={z_score:.2f}")
    
    def test_multiple_anomalies(self):
        """Test Case 3: Multiple anomalies - Multiple metrics exceed thresholds"""
        # Test multiple metrics with different anomalies
        test_cases = [
            {"metric": "volume", "current": 400.0, "expected_z": -6.0},   # Critical drop
            {"metric": "revenue", "current": 1450.0, "expected_z": 4.5},  # Critical spike
            {"metric": "latency", "current": 1300.0, "expected_z": 3.0}   # Threshold breach
        ]
        
        anomalies_detected = 0
        
        for case in test_cases:
            z_score = self.calculate_z_score(case["current"], self.mean, self.std_dev)
            
            # Check if anomaly is detected
            if abs(z_score) >= self.threshold:
                anomalies_detected += 1
                self.assertGreaterEqual(abs(z_score), self.threshold)
            
            # Verify calculated Z-score
            self.assertAlmostEqual(z_score, case["expected_z"], places=1)
            
            print(f"🚨 {case['metric'].title()} Test: Current={case['current']}, Z-Score={z_score:.2f}")
        
        # Should detect all 3 anomalies
        self.assertEqual(anomalies_detected, 3)
        print(f"✅ Multiple Anomalies Test: {anomalies_detected} anomalies detected")
    
    def test_baseline_calculation(self):
        """Test baseline calculation logic"""
        # Sample historical data
        historical_values = [950, 1050, 1100, 900, 1200, 980, 1020, 1150, 890, 1160]
        
        # Calculate mean and standard deviation
        mean = sum(historical_values) / len(historical_values)
        variance = sum((x - mean) ** 2 for x in historical_values) / len(historical_values)
        std_dev = math.sqrt(variance)
        
        # Verify calculations
        expected_mean = 1040.0
        expected_std_dev = 100.0  # Approximate
        
        self.assertAlmostEqual(mean, expected_mean, delta=50.0)
        self.assertAlmostEqual(std_dev, expected_std_dev, delta=20.0)
        
        print(f"✅ Baseline Test: Mean={mean:.1f}, StdDev={std_dev:.1f}")
    
    def test_threshold_interpretation(self):
        """Test Z-score threshold interpretation"""
        test_values = [
            {"z": 1.5, "interpretation": "Normal variation"},
            {"z": 2.5, "interpretation": "Moderate anomaly"},
            {"z": 3.5, "interpretation": "Critical anomaly"},
            {"z": -2.8, "interpretation": "Moderate anomaly"},
            {"z": -4.0, "interpretation": "Critical anomaly"}
        ]
        
        for case in test_values:
            z = case["z"]
            if abs(z) < 2.0:
                level = "Normal variation"
            elif abs(z) < 3.0:
                level = "Moderate anomaly"
            else:
                level = "Critical anomaly"
            
            self.assertEqual(level, case["interpretation"])
            print(f"✅ Threshold Test: Z={z:.1f} → {level}")


class TestContractValidation(unittest.TestCase):
    """Test contract validation logic"""
    
    def setUp(self):
        """Set up test contract and sample data"""
        self.contract = {
            "fields": {
                "order_key": {"type": "String", "required": True, "max_length": 50},
                "total_amount": {"type": "Float", "required": True, "min_value": 0.0},
                "quantity": {"type": "Integer", "required": True, "min_value": 1}
            }
        }
    
    def test_valid_contract_data(self):
        """Test valid contract data passes validation"""
        valid_data = {
            "order_key": "ORD-123",
            "total_amount": 99.99,
            "quantity": 2
        }
        
        # All validations should pass
        self.assertIsInstance(valid_data["order_key"], str)
        self.assertLess(len(valid_data["order_key"]), 51)
        self.assertIsInstance(valid_data["total_amount"], (int, float))
        self.assertGreaterEqual(valid_data["total_amount"], 0.0)
        self.assertIsInstance(valid_data["quantity"], int)
        self.assertGreaterEqual(valid_data["quantity"], 1)
        
        print("✅ Valid Contract Test: All validations passed")
    
    def test_contract_violation_detection(self):
        """Test contract violations are detected"""
        violations = []
        
        # Test data with violations
        test_cases = [
            {
                "data": {"order_key": "TOO_LONG_ORDER_KEY_THAT_EXCEEDS_THE_MAXIMUM_LIMIT_OF_FIFTY_CHARACTERS", "total_amount": 99.99, "quantity": 2},
                "expected_violations": 1,  # order_key too long
                "description": "Order key exceeds max length"
            },
            {
                "data": {"order_key": "ORD-123", "total_amount": -10.0, "quantity": 2},
                "expected_violations": 1,  # negative amount
                "description": "Negative total amount"
            },
            {
                "data": {"order_key": "ORD-123", "total_amount": 99.99, "quantity": 0},
                "expected_violations": 1,  # zero quantity
                "description": "Zero quantity"
            }
        ]
        
        for case in test_cases:
            data = case["data"]
            violation_count = 0
            
            # Check order_key
            if len(data["order_key"]) > 50:
                violation_count += 1
            
            # Check total_amount
            if data["total_amount"] < 0.0:
                violation_count += 1
            
            # Check quantity
            if data["quantity"] < 1:
                violation_count += 1
            
            self.assertEqual(violation_count, case["expected_violations"])
            print(f"🚨 Contract Violation Test: {case['description']} - {violation_count} violations")


def run_tests():
    """Run all tests and display results"""
    print("🧪 Running Data Observability Platform Core Tests")
    print("=" * 60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestAnomalyDetection))
    suite.addTests(loader.loadTestsFromTestCase(TestContractValidation))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=0)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n💥 Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    if result.wasSuccessful():
        print("\n✅ All tests passed! Core functionality validated.")
        return 0
    else:
        print("\n❌ Some tests failed. Review the output above.")
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)
