"""
Detection Engine for Data Observability & Reliability

Detection Logic:
- Volume Anomaly Detection: Z-Score > 3 threshold for volume anomalies
- Freshness Monitoring: Time-since-last-record > 30 minutes for stale data
- Alert Idempotency: Unique alerts stored in monitoring.alerts table
- Critical Alerting: High-visibility console banners for immediate attention

Reliability First: Comprehensive logging, error handling, and structured alerting
"""

import yaml
import logging
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import statistics
import math
import os
from termcolor import colored, cprint


class CriticalAlertBanner:
    """High-visibility alert banner for critical data quality issues"""
    
    @staticmethod
    def print_critical_alert(alert_type: str, description: str, severity: str = "CRITICAL", 
                          details: Optional[Dict] = None):
        """
        Print a high-visibility critical alert banner to console
        
        Idempotency: Unique alert content prevents alert fatigue
        """
        # Create the alert border
        border = "!" * 80
        
        # Print the alert with colors for maximum visibility
        print("\n" + "=" * 80)
        cprint(border, 'red', attrs=['bold'])
        cprint(f"🚨 {severity} DATA RELIABILITY ALERT 🚨", 'red', attrs=['bold', 'blink'])
        cprint(border, 'red', attrs=['bold'])
        cprint(f"ALERT TYPE: {alert_type}", 'yellow', attrs=['bold'])
        cprint(f"TIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}", 'yellow')
        cprint(f"DESCRIPTION: {description}", 'white', attrs=['bold'])
        
        if details:
            cprint("ADDITIONAL DETAILS:", 'cyan')
            for key, value in details.items():
                cprint(f"  • {key}: {value}", 'cyan')
        
        cprint(border, 'red', attrs=['bold'])
        cprint("🔥 IMMEDIATE ACTION REQUIRED 🔥", 'red', attrs=['bold', 'blink'])
        cprint(border, 'red', attrs=['bold'])
        print("=" * 80 + "\n")
        
        # Also print to stderr for log capture
        error_msg = f"CRITICAL ALERT: {alert_type} - {description}"
        print(error_msg, file=sys.stderr)


class DatabaseConnection:
    """Database connection manager for detection operations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for detection operations"""
        logger = logging.getLogger(f"{self.config['name']}_detector")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def connect(self) -> bool:
        """Establish database connection with error handling"""
        try:
            self.connection = psycopg2.connect(
                self.config['connection_string'],
                connect_timeout=self.config.get('timeout', 30)
            )
            self.logger.info(f"Successfully connected to {self.config['name']}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to {self.config['name']}: {str(e)}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute SQL query with error handling and logging"""
        if not self.connection:
            if not self.connect():
                raise ConnectionError(f"Cannot connect to {self.config['name']}")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            self.logger.info(f"Query executed successfully on {self.config['name']}, returned {len(results)} rows")
            return results
            
        except Exception as e:
            self.logger.error(f"Query failed on {self.config['name']}: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info(f"Connection closed to {self.config['name']}")


class DetectionEngine:
    """
    Multi-source detection engine for data observability and reliability
    
    Detection Logic: Implements Z-Score volume anomaly detection and freshness monitoring
    Reliability First: Idempotent alerting with comprehensive logging
    """
    
    def __init__(self, config_path: str = "observability_configs/databases.yaml"):
        self.logger = self._setup_logger()
        self.config = self._load_config(config_path)
        self.cdc_db = None
        self.alert_banner = CriticalAlertBanner()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for detection engine"""
        logger = logging.getLogger("detection_engine")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration with error handling"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                self.logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def initialize_connection(self) -> bool:
        """Initialize CDC database connection for detection operations"""
        try:
            self.cdc_db = DatabaseConnection(self.config['databases']['cdc_history_db'])
            connected = self.cdc_db.connect()
            
            if connected:
                self.logger.info("CDC database connection established for detection")
                return True
            else:
                self.logger.error("Failed to establish CDC database connection")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection initialization failed: {str(e)}")
            return False
    
    def create_alerts_table(self) -> bool:
        """Create monitoring.alerts table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS monitoring.alerts (
            id SERIAL PRIMARY KEY,
            alert_type VARCHAR(50) NOT NULL,
            alert_severity VARCHAR(20) NOT NULL DEFAULT 'CRITICAL',
            description TEXT NOT NULL,
            source_table VARCHAR(100),
            metric_value DECIMAL(15,4),
            threshold_value DECIMAL(15,4),
            z_score DECIMAL(10,4),
            details JSONB,
            alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_timestamp TIMESTAMP,
            UNIQUE(alert_type, source_table, alert_timestamp)
        )
        """
        
        try:
            if not self.cdc_db.connection:
                if not self.cdc_db.connect():
                    raise ConnectionError("Cannot connect to database")
            
            cursor = self.cdc_db.connection.cursor()
            cursor.execute(create_table_query)
            cursor.close()
            
            self.logger.info("Alerts table created/verified successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create alerts table: {str(e)}")
            return False
    
    def get_baseline_metrics(self, metric_name: str, source_table: str) -> Optional[Tuple[float, float, int]]:
        """
        Retrieve baseline metrics for Z-Score calculation
        
        Detection Logic: Fetch mean, std_dev, and sample_size from monitoring.baselines
        Returns: Tuple of (mean, std_dev, sample_size) or None if not found
        """
        query = """
        SELECT mean_value, std_deviation, sample_size
        FROM monitoring.baselines
        WHERE metric_name = %s AND table_name = %s
        ORDER BY calculation_timestamp DESC
        LIMIT 1
        """
        
        try:
            results = self.cdc_db.execute_query(query, (metric_name, source_table))
            if results:
                mean, std_dev, sample_size = results[0]
                self.logger.info(f"Retrieved baseline for {metric_name}: mean={mean}, std_dev={std_dev}")
                return float(mean), float(std_dev), int(sample_size)
            else:
                self.logger.warning(f"No baseline found for {metric_name} on {source_table}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to retrieve baseline metrics: {str(e)}")
            return None
    
    def get_current_volume_metrics(self) -> Optional[Tuple[int, datetime]]:
        """
        Get current volume metrics for anomaly detection
        
        Detection Logic: Count records from last hour in dim_orders_history
        Returns: Tuple of (current_count, latest_timestamp) or None
        """
        query = """
        WITH current_metrics AS (
            SELECT 
                COUNT(*) as current_count,
                MAX(created_at) as latest_timestamp
            FROM dim_orders_history
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        )
        SELECT current_count, latest_timestamp
        FROM current_metrics
        """
        
        try:
            results = self.cdc_db.execute_query(query)
            if results and results[0][0] > 0:
                current_count, latest_timestamp = results[0]
                self.logger.info(f"Current volume metrics: {current_count} records, latest: {latest_timestamp}")
                return int(current_count), latest_timestamp
            else:
                self.logger.warning("No current volume data found")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get current volume metrics: {str(e)}")
            return None
    
    def calculate_z_score(self, current_value: float, mean: float, std_dev: float) -> float:
        """
        Calculate Z-Score for volume anomaly detection
        
        Detection Logic: Standard Z-Score calculation for statistical anomaly detection
        Returns: Z-Score value (higher = more anomalous)
        """
        if std_dev == 0:
            return 0.0
        
        z_score = abs((current_value - mean) / std_dev)
        self.logger.info(f"Z-Score calculation: current={current_value}, mean={mean}, std_dev={std_dev}, z_score={z_score}")
        return z_score
    
    def log_alert(self, alert_type: str, description: str, source_table: Optional[str] = None,
                 metric_value: Optional[float] = None, threshold_value: Optional[float] = None,
                 z_score: Optional[float] = None, details: Optional[Dict] = None) -> bool:
        """
        Log alert to monitoring.alerts table
        
        Idempotency: UNIQUE constraint prevents duplicate alerts
        """
        insert_query = """
        INSERT INTO monitoring.alerts 
        (alert_type, alert_severity, description, source_table, metric_value, threshold_value, z_score, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            import json
            details_json = json.dumps(details) if details else None
            
            if not self.cdc_db.connection:
                if not self.cdc_db.connect():
                    raise ConnectionError("Cannot connect to database")
            
            cursor = self.cdc_db.connection.cursor()
            cursor.execute(insert_query, 
                             (alert_type, "CRITICAL", description, source_table,
                              metric_value, threshold_value, z_score, details_json))
            cursor.close()
            
            self.logger.info(f"Alert logged to database: {alert_type}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to log alert: {str(e)}")
            return False
    
    def check_volume_anomaly(self) -> bool:
        """
        Volume Check: Compare current ingestion counts to baselines
        
        Detection Logic: Z-Score > 3 indicates significant volume anomaly
        Alerting: Logs VOLUME_ANOMALY to monitoring.alerts table
        """
        try:
            self.logger.info("Starting volume anomaly detection")
            
            # Get baseline metrics
            baseline = self.get_baseline_metrics("hourly_ingestion_rate", "dim_orders_history")
            if not baseline:
                self.logger.warning("Cannot perform volume check: no baseline available")
                return False
            
            mean, std_dev, sample_size = baseline
            
            # Get current metrics
            current_metrics = self.get_current_volume_metrics()
            if not current_metrics:
                self.logger.warning("Cannot perform volume check: no current data")
                return False
            
            current_count, latest_timestamp = current_metrics
            
            # Calculate Z-Score
            z_score = self.calculate_z_score(current_count, mean, std_dev)
            
            # Check threshold
            if z_score > 3.0:
                # Log anomaly to database
                alert_details = {
                    "current_count": current_count,
                    "baseline_mean": mean,
                    "baseline_std_dev": std_dev,
                    "sample_size": sample_size,
                    "z_score": z_score,
                    "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None
                }
                
                self.log_alert(
                    alert_type="VOLUME_ANOMALY",
                    description=f"Volume anomaly detected: {current_count} records (Z-Score: {z_score:.2f})",
                    source_table="dim_orders_history",
                    metric_value=float(current_count),
                    threshold_value=mean + (3.0 * std_dev),
                    z_score=z_score,
                    details=alert_details
                )
                
                # Print critical alert banner
                self.alert_banner.print_critical_alert(
                    alert_type="VOLUME_ANOMALY",
                    description=f"Ingestion volume anomaly detected! Current: {current_count} records, Expected: ~{mean:.0f} ± {std_dev:.0f}",
                    details=alert_details
                )
                
                self.logger.error(f"Volume anomaly detected: Z-Score {z_score:.2f} > 3.0")
                return True
            else:
                self.logger.info(f"Volume check passed: Z-Score {z_score:.2f} <= 3.0")
                return False
                
        except Exception as e:
            self.logger.error(f"Volume anomaly detection failed: {str(e)}")
            return False
    
    def get_freshness_metrics(self) -> Optional[datetime]:
        """
        Get freshness metrics for staleness detection
        
        Detection Logic: Get max cdc_timestamp from dim_orders_history
        Returns: Latest timestamp or None if no data
        """
        query = """
        SELECT MAX(cdc_timestamp) as latest_cdc_timestamp
        FROM dim_orders_history
        """
        
        try:
            results = self.cdc_db.execute_query(query)
            if results and results[0][0]:
                latest_timestamp = results[0][0]
                self.logger.info(f"Latest CDC timestamp: {latest_timestamp}")
                return latest_timestamp
            else:
                self.logger.warning("No CDC timestamps found")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get freshness metrics: {str(e)}")
            return None
    
    def check_freshness_anomaly(self) -> bool:
        """
        Freshness Check: Check time since last record
        
        Detection Logic: Time-since-last-record > 30 minutes indicates stale data
        Alerting: Logs STALE_DATA_FLOW alert to monitoring.alerts table
        """
        try:
            self.logger.info("Starting freshness anomaly detection")
            
            # Get latest CDC timestamp
            latest_timestamp = self.get_freshness_metrics()
            if not latest_timestamp:
                self.logger.warning("Cannot perform freshness check: no timestamp data")
                return False
            
            # Calculate time since last record
            now = datetime.now(timezone.utc)
            
            # Ensure latest_timestamp has timezone info
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
            
            time_since_last = now - latest_timestamp
            minutes_since_last = time_since_last.total_seconds() / 60
            
            # Check threshold (30 minutes)
            if minutes_since_last > 30:
                # Log anomaly to database
                alert_details = {
                    "latest_timestamp": latest_timestamp.isoformat(),
                    "current_timestamp": now.isoformat(),
                    "minutes_since_last": minutes_since_last,
                    "threshold_minutes": 30
                }
                
                self.log_alert(
                    alert_type="STALE_DATA_FLOW",
                    description=f"Data flow stale: {minutes_since_last:.1f} minutes since last record",
                    source_table="dim_orders_history",
                    metric_value=float(minutes_since_last),
                    threshold_value=30.0,
                    details=alert_details
                )
                
                # Print critical alert banner
                self.alert_banner.print_critical_alert(
                    alert_type="STALE_DATA_FLOW",
                    description=f"Data flow is stale! No new records for {minutes_since_last:.1f} minutes (threshold: 30 minutes)",
                    details=alert_details
                )
                
                self.logger.error(f"Freshness anomaly detected: {minutes_since_last:.1f} minutes > 30 minutes")
                return True
            else:
                self.logger.info(f"Freshness check passed: {minutes_since_last:.1f} minutes <= 30 minutes")
                return False
                
        except Exception as e:
            self.logger.error(f"Freshness anomaly detection failed: {str(e)}")
            return False
    
    def log_alert(self, alert_type: str, description: str, source_table: Optional[str] = None,
                 metric_value: Optional[float] = None, threshold_value: Optional[float] = None,
                 z_score: Optional[float] = None, details: Optional[Dict] = None) -> bool:
        """
        Log alert to monitoring.alerts table
        
        Idempotency: UNIQUE constraint prevents duplicate alerts
        """
        insert_query = """
        INSERT INTO monitoring.alerts 
        (alert_type, alert_severity, description, source_table, metric_value, threshold_value, z_score, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_type, source_table, alert_timestamp) DO NOTHING
        """
        
        try:
            import json
            details_json = json.dumps(details) if details else None
            
            self.cdc_db.execute_query(insert_query, 
                                     (alert_type, "CRITICAL", description, source_table,
                                      metric_value, threshold_value, z_score, details_json))
            self.logger.info(f"Alert logged to database: {alert_type}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to log alert: {str(e)}")
            return False
    
    def run_detection(self) -> Dict[str, bool]:
        """
        Main detection execution method
        
        Detection Logic: Orchestrates volume and freshness anomaly detection
        Returns: Dictionary with detection results
        """
        results = {
            "volume_anomaly": False,
            "freshness_anomaly": False,
            "total_anomalies": 0
        }
        
        try:
            self.logger.info("Starting detection engine execution")
            
            # Initialize connection
            if not self.initialize_connection():
                self.logger.error("Failed to initialize database connection")
                return results
            
            # Create alerts table
            if not self.create_alerts_table():
                self.logger.error("Failed to create alerts table")
                return results
            
            # Run volume anomaly detection
            volume_result = self.check_volume_anomaly()
            results["volume_anomaly"] = volume_result
            
            # Run freshness anomaly detection
            freshness_result = self.check_freshness_anomaly()
            results["freshness_anomaly"] = freshness_result
            
            # Calculate total anomalies
            results["total_anomalies"] = sum([volume_result, freshness_result])
            
            if results["total_anomalies"] == 0:
                self.logger.info("Detection completed: No anomalies detected")
            else:
                self.logger.error(f"Detection completed: {results['total_anomalies']} anomalies detected")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Detection execution failed: {str(e)}")
            return results
        finally:
            # Cleanup connection
            if self.cdc_db:
                self.cdc_db.close()
    
    def generate_detection_summary(self, results: Dict[str, bool]) -> str:
        """Generate detection summary report"""
        summary = f"""
# Detection Engine Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

## Detection Results
- **Volume Anomaly**: {'🚨 DETECTED' if results['volume_anomaly'] else '✅ Normal'}
- **Freshness Anomaly**: {'🚨 DETECTED' if results['freshness_anomaly'] else '✅ Normal'}
- **Total Anomalies**: {results['total_anomalies']}

## Detection Logic Applied
- **Volume Anomalies**: Z-Score > 3.0 threshold
- **Freshness Monitoring**: Time-since-last-record > 30 minutes
- **Alert Idempotency**: Unique alerts in monitoring.alerts table

## Next Steps
{'⚠️  IMMEDIATE ACTION REQUIRED' if results['total_anomalies'] > 0 else '✅ All systems normal'}
"""
        return summary


def main():
    """Main execution function"""
    detector = DetectionEngine()
    
    try:
        # Run detection
        results = detector.run_detection()
        
        # Generate and display summary
        summary = detector.generate_detection_summary(results)
        print(summary)
        
        # Save summary to file
        with open("detection_summary.md", "w") as f:
            f.write(summary)
        
        # Exit with error code if anomalies detected
        if results['total_anomalies'] > 0:
            print(f"Detection completed with {results['total_anomalies']} anomalies detected!")
            return 1
        else:
            print("Detection completed successfully - no anomalies detected!")
            return 0
            
    except Exception as e:
        print(f"Detection execution error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
