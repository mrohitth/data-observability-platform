#!/usr/bin/env python3
"""
Chaos Volume Drop Test

This test simulates a 90% volume drop to verify that the Data Observability Platform's
anomaly detection engine correctly identifies and alerts on Z-score anomalies.

Test Scenario:
- Normal baseline: ~20 records per hour
- Chaos scenario: Only 2 records (90% drop)
- Expected: Z-Score > 3.0 and CRITICAL VOLUME_ANOMALY alert
"""

import sys
import os
import json
import yaml
import psycopg2
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from profiler import MetricsProfiler
from detector import DetectionEngine


class ChaosVolumeTest:
    """Chaos test for volume drop detection"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        import logging
        logger = logging.getLogger("chaos_volume_test")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def load_config(self):
        """Load database configuration"""
        with open("config/databases.yaml", 'r') as f:
            return yaml.safe_load(f)
    
    def get_database_connection(self, config):
        """Get database connection"""
        try:
            conn = psycopg2.connect(
                config['databases']['cdc_history_db']['connection_string']
            )
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return None
    
    def setup_baseline(self, conn):
        """Setup baseline with normal volume (~20 records per hour)"""
        try:
            cursor = conn.cursor()
            
            # Clear existing test data
            cursor.execute("DELETE FROM monitoring.baselines WHERE metric_name = 'hourly_ingestion_rate'")
            
            # Insert baseline for normal volume
            cursor.execute("""
                INSERT INTO monitoring.baselines 
                (metric_name, source_database, table_name, mean_value, std_deviation, sample_size)
                VALUES ('hourly_ingestion_rate', 'cdc_history_db', 'dim_orders_history', 20.0, 5.0, 100)
            """)
            
            conn.commit()
            self.logger.info("✅ Baseline setup: 20.0 ± 5.0 records per hour")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup baseline: {e}")
            return False
    
    def simulate_volume_drop(self, conn):
        """Simulate volume drop with only 2 records"""
        try:
            cursor = conn.cursor()
            
            # Clear existing dim_orders_history records
            cursor.execute("DELETE FROM dim_orders_history WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '2 hours'")
            
            # Insert only 2 records (90% drop from baseline)
            test_records = [
                (1001, 101, 201, 2, 29.99, 59.98, 'completed', datetime.now(timezone.utc) - timedelta(minutes=55)),
                (1002, 102, 202, 1, 49.99, 49.99, 'pending', datetime.now(timezone.utc) - timedelta(minutes=30))
            ]
            
            for record in test_records:
                order_key, customer_id, product_id, quantity, unit_price, total_amount, order_status, order_date = record
                cursor.execute("""
                    INSERT INTO dim_orders_history 
                    (order_key, customer_id, product_id, quantity, unit_price, total_amount, 
                     order_status, order_date, valid_from, valid_to, is_current, cdc_operation, 
                     cdc_timestamp, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_key, customer_id, product_id, quantity, unit_price, total_amount, 
                      order_status, order_date, order_date, None, True, 'INSERT', 
                      datetime.now(timezone.utc), datetime.now(timezone.utc)))
            
            conn.commit()
            self.logger.info("🔥 Volume drop simulated: Only 2 records (90% drop from baseline)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to simulate volume drop: {e}")
            return False
    
    def run_detection_test(self):
        """Run the detection engine to verify anomaly detection"""
        try:
            self.logger.info("🔍 Running detection engine test...")
            
            detector = DetectionEngine()
            results = detector.run_detection()
            
            self.logger.info(f"Detection results: {results}")
            
            # Check if volume anomaly was detected
            if results.get('volume_anomaly', False):
                self.logger.info("✅ SUCCESS: Volume anomaly detected!")
                return True
            else:
                self.logger.error("❌ FAILURE: Volume anomaly NOT detected!")
                return False
                
        except Exception as e:
            self.logger.error(f"Detection test failed: {e}")
            return False
    
    def verify_alert_in_database(self, config):
        """Verify that alert was logged to database"""
        try:
            conn = psycopg2.connect(config['databases']['cdc_history_db']['connection_string'])
            cursor = conn.cursor()
            
            # Check for VOLUME_ANOMALY alerts in last hour
            cursor.execute("""
                SELECT alert_type, description, alert_timestamp, details
                FROM monitoring.alerts 
                WHERE alert_type = 'VOLUME_ANOMALY' 
                AND alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                ORDER BY alert_timestamp DESC
                LIMIT 5
            """)
            
            alerts = cursor.fetchall()
            conn.close()
            
            if alerts:
                self.logger.info(f"✅ Found {len(alerts)} VOLUME_ANOMALY alerts in database")
                for alert in alerts:
                    alert_type, description, timestamp, details = alert
                    self.logger.info(f"  - {description} at {timestamp}")
                return True
            else:
                self.logger.error("❌ No VOLUME_ANOMALY alerts found in database")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to verify alerts in database: {e}")
            return False
    
    def cleanup_test_data(self, conn):
        """Clean up test data"""
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM monitoring.baselines WHERE metric_name = 'hourly_ingestion_rate'")
            cursor.execute("DELETE FROM monitoring.alerts WHERE alert_type = 'VOLUME_ANOMALY'")
            cursor.execute("DELETE FROM dim_orders_history WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '2 hours'")
            conn.commit()
            self.logger.info("🧹 Test data cleaned up")
        except Exception as e:
            self.logger.error(f"Failed to cleanup: {e}")
    
    def run_test(self):
        """Run the complete chaos volume test"""
        self.logger.info("🚀 Starting Chaos Volume Drop Test")
        self.logger.info("=" * 60)
        
        try:
            # Load configuration
            config = self.load_config()
            
            # Get database connection
            conn = self.get_database_connection(config)
            if not conn:
                return False
            
            try:
                # Step 1: Setup baseline
                if not self.setup_baseline(conn):
                    return False
                
                # Step 2: Simulate volume drop
                if not self.simulate_volume_drop(conn):
                    return False
                
                # Step 3: Run detection test
                detection_success = self.run_detection_test()
                
                # Step 4: Verify database alerts
                db_success = self.verify_alert_in_database(config)
                
                # Overall test result
                test_success = detection_success and db_success
                
                if test_success:
                    self.logger.info("🎉 CHAOS VOLUME TEST PASSED!")
                    self.logger.info("   ✅ Volume anomaly correctly detected")
                    self.logger.info("   ✅ Critical alert properly logged")
                else:
                    self.logger.error("💥 CHAOS VOLUME TEST FAILED!")
                    self.logger.error("   ❌ Anomaly detection or alerting failed")
                
                return test_success
                
            finally:
                # Cleanup
                self.cleanup_test_data(conn)
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Chaos volume test failed: {e}")
            return False


def main():
    """Main execution function"""
    test = ChaosVolumeTest()
    success = test.run_test()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
