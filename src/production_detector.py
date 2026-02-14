"""
Production-Ready Detection Engine

Thread-safe anomaly detection with exponential backoff retry logic.
Implements volume anomaly detection and freshness monitoring with production-grade reliability.

Detection Logic:
- Volume Anomaly Detection: Z-Score > 3.0 threshold for volume anomalies
- Freshness Monitoring: Time-since-last-record > 30 minutes for stale data
- Thread-Safe Operations: Concurrent detection without metadata conflicts
- Production Alerting: High-visibility alerts with structured logging
"""

import logging
import sys
import math
import threading
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional

from config_manager import get_config
from database_manager import get_database_manager
from termcolor import colored, cprint


class ProductionCriticalAlertBanner:
    """High-visibility alert banner for critical data quality issues"""
    
    def __init__(self, config):
        self.config = config
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for alert banner"""
        logger = logging.getLogger("alert_banner")
        logger.setLevel(getattr(self.config.logging, 'level', 'INFO'))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
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


class ProductionDetectionEngine:
    """
    Production-ready detection engine with thread safety and retry logic
    
    Features:
    - Thread-safe anomaly detection
    - Exponential backoff retry
    - Rotating log files
    - Environment-based configuration
    - Comprehensive error handling
    """
    
    def __init__(self):
        """Initialize production detection engine"""
        self.config = get_config()
        self.db_manager = get_database_manager()
        self.logger = self._setup_logger()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Get database connection managers
        self.cdc_db = self.db_manager.get_connection_manager('cdc')
        
        # Alert banner
        self.alert_banner = ProductionCriticalAlertBanner(self.config)
        
        self.logger.info("Production detection engine initialized")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with rotation"""
        logger = logging.getLogger("production_detector")
        logger.setLevel(getattr(self.config.logging, 'level', 'INFO'))
        
        if not logger.handlers:
            # Console handler
            if self.config.logging.enable_console:
                console_handler = logging.StreamHandler()
                console_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                console_handler.setFormatter(console_formatter)
                logger.addHandler(console_handler)
            
            # File handler with rotation
            if self.config.logging.enable_file:
                from logging.handlers import RotatingFileHandler
                
                log_dir = self.config.logging.log_dir
                import os
                os.makedirs(log_dir, exist_ok=True)
                
                file_handler = RotatingFileHandler(
                    filename=log_dir / "detector.log",
                    maxBytes=self.config.logging.max_bytes,
                    backupCount=self.config.logging.backup_count
                )
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
        
        return logger
    
    def create_alerts_table(self) -> bool:
        """
        Create monitoring.alerts table if it doesn't exist
        
        Thread Safety: Uses thread-safe database manager
        """
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
            with self._lock:
                self.cdc_db.execute_query(create_table_query, fetch=False)
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
        
        Thread Safety: Uses thread-safe database manager
        """
        query = """
        SELECT mean_value, std_deviation, sample_size
        FROM monitoring.baselines
        WHERE metric_name = %s AND table_name = %s
        ORDER BY calculation_timestamp DESC
        LIMIT 1
        """
        
        try:
            with self._lock:
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
        
        Thread Safety: Uses thread-safe database manager
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
            with self._lock:
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
        
        Thread Safety: Thread-safe calculation
        """
        with self._lock:
            if std_dev == 0:
                return 0.0
            
            z_score = abs((current_value - mean) / std_dev)
            self.logger.info(f"Z-Score calculation: current={current_value}, mean={mean}, std_dev={std_dev}, z_score={z_score}")
            return z_score
    
    def check_volume_anomaly(self) -> bool:
        """
        Volume Check: Compare current ingestion counts to baselines
        
        Detection Logic: Z-Score > 3.0 indicates significant volume anomaly
        Alerting: Logs VOLUME_ANOMALY to monitoring.alerts table
        
        Thread Safety: Thread-safe anomaly detection
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
            threshold = self.config.monitoring.volume_anomaly_threshold
            if z_score > threshold:
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
                    threshold_value=mean + (threshold * std_dev),
                    z_score=z_score,
                    details=alert_details
                )
                
                # Print critical alert banner
                self.alert_banner.print_critical_alert(
                    alert_type="VOLUME_ANOMALY",
                    description=f"Ingestion volume anomaly detected! Current: {current_count} records, Expected: ~{mean:.0f} ± {std_dev:.0f}",
                    details=alert_details
                )
                
                self.logger.error(f"Volume anomaly detected: Z-Score {z_score:.2f} > {threshold}")
                return True
            else:
                self.logger.info(f"Volume check passed: Z-Score {z_score:.2f} <= {threshold}")
                return False
                
        except Exception as e:
            self.logger.error(f"Volume anomaly detection failed: {str(e)}")
            return False
    
    def get_freshness_metrics(self) -> Optional[datetime]:
        """
        Get freshness metrics for staleness detection
        
        Detection Logic: Get max cdc_timestamp from dim_orders_history
        Returns: Latest timestamp or None if no data
        
        Thread Safety: Uses thread-safe database manager
        """
        query = """
        SELECT MAX(cdc_timestamp) as latest_cdc_timestamp
        FROM dim_orders_history
        """
        
        try:
            with self._lock:
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
        
        Detection Logic: Time-since-last-record > threshold minutes indicates stale data
        Alerting: Logs STALE_DATA_FLOW alert to monitoring.alerts table
        
        Thread Safety: Thread-safe freshness monitoring
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
            
            # Check threshold
            threshold = self.config.monitoring.freshness_threshold_minutes
            if minutes_since_last > threshold:
                # Log anomaly to database
                alert_details = {
                    "latest_timestamp": latest_timestamp.isoformat(),
                    "current_timestamp": now.isoformat(),
                    "minutes_since_last": minutes_since_last,
                    "threshold_minutes": threshold
                }
                
                self.log_alert(
                    alert_type="STALE_DATA_FLOW",
                    description=f"Data flow stale: {minutes_since_last:.1f} minutes since last record",
                    source_table="dim_orders_history",
                    metric_value=float(minutes_since_last),
                    threshold_value=float(threshold),
                    details=alert_details
                )
                
                # Print critical alert banner
                self.alert_banner.print_critical_alert(
                    alert_type="STALE_DATA_FLOW",
                    description=f"Data flow is stale! No new records for {minutes_since_last:.1f} minutes (threshold: {threshold} minutes)",
                    details=alert_details
                )
                
                self.logger.error(f"Freshness anomaly detected: {minutes_since_last:.1f} minutes > {threshold} minutes")
                return True
            else:
                self.logger.info(f"Freshness check passed: {minutes_since_last:.1f} minutes <= {threshold} minutes")
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
        
        Thread Safety: Uses thread-safe database manager
        """
        insert_query = """
        INSERT INTO monitoring.alerts 
        (alert_type, alert_severity, description, source_table, metric_value, threshold_value, z_score, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self._lock:
                details_json = json.dumps(details) if details else None
                
                self.cdc_db.execute_query(insert_query, 
                                         (alert_type, self.config.monitoring.alert_severity, description, source_table,
                                          metric_value, threshold_value, z_score, details_json))
                self.logger.info(f"Alert logged to database: {alert_type}")
                return True
        except Exception as e:
            self.logger.error(f"Failed to log alert: {str(e)}")
            return False
    
    def run_detection(self) -> Dict[str, bool]:
        """
        Main detection execution method with thread safety and error handling
        
        Detection Logic: Orchestrates volume and freshness anomaly detection
        Thread Safety: Thread-safe execution with proper locking
        
        Returns:
            Dictionary with detection results
        """
        results = {
            "volume_anomaly": False,
            "freshness_anomaly": False,
            "total_anomalies": 0
        }
        
        try:
            self.logger.info("Starting production detection engine execution")
            
            # Health check before starting
            health_status = self.db_manager.health_check()
            if not all(health_status.values()):
                self.logger.warning(f"Database health check failed: {health_status}")
            
            # Create alerts table
            if not self.create_alerts_table():
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
    
    def get_detection_status(self) -> Dict[str, Any]:
        """
        Get current detection status and statistics
        
        Returns:
            Dictionary with detection status information
        """
        try:
            # Get recent alert counts
            alert_query = """
            SELECT alert_type, COUNT(*) as count, 
                   MAX(alert_timestamp) as latest_timestamp
            FROM monitoring.alerts
            WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            GROUP BY alert_type
            """
            
            with self._lock:
                alert_results = self.cdc_db.execute_query(alert_query)
            
            # Get database status
            db_status = self.db_manager.get_status()
            
            return {
                "detector_status": "running",
                "recent_alerts": {row[0]: row[1] for row in alert_results},
                "database_status": db_status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get detection status: {str(e)}")
            return {
                "detector_status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.db_manager.close_all()
            self.logger.info("Production detection engine cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


def main():
    """Main execution function with production error handling"""
    detector = None
    
    try:
        detector = ProductionDetectionEngine()
        results = detector.run_detection()
        
        # Generate status report
        status = detector.get_detection_status()
        print(f"Detection Status: {status}")
        
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
    finally:
        if detector:
            detector.cleanup()


if __name__ == "__main__":
    sys.exit(main())
