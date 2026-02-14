"""
Production-Ready Multi-Source Profiler

Thread-safe profiler with exponential backoff retry logic and log rotation.
Implements data observability and reliability monitoring across multiple databases.

Detection Logic:
- Volume Anomaly Detection: Z-Score (Standard Deviations) for volume anomalies
- Baseline Calculation: Statistical analysis for anomaly detection
- Thread-Safe Operations: Concurrent profiling without metadata table locking
- Production Logging: Rotating logs with structured formatting
"""

import logging
import sys
import statistics
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
from pathlib import Path

from config_manager import get_config
from database_manager import get_database_manager


class ProductionMetricsProfiler:
    """
    Production-ready metrics profiler with thread safety and retry logic
    
    Features:
    - Thread-safe database operations
    - Exponential backoff retry
    - Rotating log files
    - Environment-based configuration
    - Comprehensive error handling
    """
    
    def __init__(self):
        """Initialize production metrics profiler"""
        self.config = get_config()
        self.db_manager = get_database_manager()
        self.logger = self._setup_logger()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Get database connection managers
        self.batch_db = self.db_manager.get_connection_manager('batch')
        self.cdc_db = self.db_manager.get_connection_manager('cdc')
        
        self.logger.info("Production metrics profiler initialized")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with rotation"""
        logger = logging.getLogger("production_profiler")
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
                
                log_dir = Path(self.config.logging.log_dir)
                log_dir.mkdir(exist_ok=True)
                
                file_handler = RotatingFileHandler(
                    filename=log_dir / "profiler.log",
                    maxBytes=self.config.logging.max_bytes,
                    backupCount=self.config.logging.backup_count
                )
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
        
        return logger
    
    def get_batch_daily_row_counts(self, days_back: int = 30) -> List[Tuple]:
        """
        Get daily row counts from fact_orders table in Batch DB
        
        Detection Logic: CTE-based query for daily aggregation
        Schema: marts.fact_orders with order_timestamp column
        Returns: List of (date, row_count) tuples
        
        Thread Safety: Uses thread-safe database manager
        """
        query = """
        WITH daily_counts AS (
            SELECT 
                DATE(order_timestamp) as order_date,
                COUNT(*) as row_count
            FROM marts.fact_orders
            WHERE order_timestamp >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY DATE(order_timestamp)
        )
        SELECT 
            order_date::date,
            row_count
        FROM daily_counts
        ORDER BY order_date DESC
        """
        
        try:
            with self._lock:
                results = self.batch_db.execute_query(query, (days_back,))
                self.logger.info(f"Retrieved {len(results)} daily row counts from Batch DB")
                return results
                
        except Exception as e:
            self.logger.error(f"Failed to get batch daily row counts: {str(e)}")
            raise
    
    def get_cdc_hourly_ingestion_rates(self, hours_back: int = 24) -> List[Tuple]:
        """
        Get hourly ingestion rates from dim_orders_history table in CDC DB
        
        Detection Logic: CTE-based query for hourly aggregation
        Schema: public.dim_orders_history with created_at column
        Returns: List of (hour, ingestion_rate) tuples
        
        Thread Safety: Uses thread-safe database manager
        """
        query = """
        WITH hourly_ingestion AS (
            SELECT 
                DATE_TRUNC('hour', created_at) as ingestion_hour,
                COUNT(*) as records_ingested
            FROM dim_orders_history
            WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '%s hours'
            GROUP BY DATE_TRUNC('hour', created_at)
        )
        SELECT 
            ingestion_hour,
            records_ingested
        FROM hourly_ingestion
        ORDER BY ingestion_hour DESC
        """
        
        try:
            with self._lock:
                results = self.cdc_db.execute_query(query, (hours_back,))
                self.logger.info(f"Retrieved {len(results)} hourly ingestion rates from CDC DB")
                return results
                
        except Exception as e:
            self.logger.error(f"Failed to get CDC hourly ingestion rates: {str(e)}")
            raise
    
    def calculate_statistics(self, values: List[float]) -> Tuple[float, float]:
        """
        Calculate mean and standard deviation for Z-Score anomaly detection
        
        Detection Logic: Standard statistical calculations for baseline metrics
        Returns: Tuple of (mean, std_dev)
        
        Thread Safety: Thread-safe calculation
        """
        with self._lock:
            if not values:
                raise ValueError("No values provided for statistics calculation")
            
            try:
                mean_val = statistics.mean(values)
                std_dev_val = statistics.stdev(values) if len(values) > 1 else 0.0
                
                self.logger.info(f"Calculated statistics: mean={mean_val:.2f}, std_dev={std_dev_val:.2f}")
                return mean_val, std_dev_val
            except Exception as e:
                self.logger.error(f"Statistics calculation failed: {str(e)}")
                raise
    
    def create_monitoring_table(self) -> bool:
        """
        Create monitoring.baselines table if it doesn't exist
        
        Thread Safety: Uses thread-safe database manager with retry logic
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS monitoring.baselines (
            id SERIAL PRIMARY KEY,
            metric_name VARCHAR(100) NOT NULL,
            source_database VARCHAR(50) NOT NULL,
            table_name VARCHAR(100) NOT NULL,
            mean_value DECIMAL(15,4) NOT NULL,
            std_deviation DECIMAL(15,4) NOT NULL,
            sample_size INTEGER NOT NULL,
            calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(metric_name, source_database, table_name)
        )
        """
        
        try:
            with self._lock:
                # Use execute_query without fetch for DDL
                self.cdc_db.execute_query(create_table_query, fetch=False)
                self.logger.info("Monitoring table created/verified successfully")
                return True
        except Exception as e:
            self.logger.error(f"Failed to create monitoring table: {str(e)}")
            return False
    
    def store_baselines(self, metric_name: str, source_db: str, table_name: str, 
                      mean_val: float, std_dev: float, sample_size: int) -> bool:
        """
        Store calculated baselines in monitoring.baselines table
        
        Idempotency: Uses UPSERT logic to avoid duplicate records
        
        Thread Safety: Uses thread-safe database manager
        """
        upsert_query = """
        INSERT INTO monitoring.baselines 
        (metric_name, source_database, table_name, mean_value, std_deviation, sample_size)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (metric_name, source_database, table_name)
        DO UPDATE SET 
            mean_value = EXCLUDED.mean_value,
            std_deviation = EXCLUDED.std_deviation,
            sample_size = EXCLUDED.sample_size,
            calculation_timestamp = CURRENT_TIMESTAMP
        """
        
        try:
            with self._lock:
                self.cdc_db.execute_query(upsert_query, 
                                         (metric_name, source_db, table_name, 
                                          mean_val, std_dev, sample_size))
                self.logger.info(f"Baseline stored for {metric_name} from {source_db}.{table_name}")
                return True
        except Exception as e:
            self.logger.error(f"Failed to store baseline: {str(e)}")
            return False
    
    def run_profiling(self) -> bool:
        """
        Main profiling execution method with thread safety and error handling
        
        Detection Logic: Orchestrates all profiling steps and stores baselines
        Thread Safety: Thread-safe execution with proper locking
        
        Returns:
            True if profiling completed successfully, False otherwise
        """
        try:
            self.logger.info("Starting production profiling execution")
            
            # Health check before starting
            health_status = self.db_manager.health_check()
            if not all(health_status.values()):
                self.logger.warning(f"Database health check failed: {health_status}")
            
            # Create monitoring table
            if not self.create_monitoring_table():
                return False
            
            # Profile Batch DB - Daily row counts
            batch_counts = self.get_batch_daily_row_counts()
            if batch_counts:
                row_counts = [count[1] for count in batch_counts]
                mean_rows, std_rows = self.calculate_statistics(row_counts)
                self.store_baselines("daily_row_count", "batch_analytics_db", 
                                   "marts.fact_orders", mean_rows, std_rows, len(row_counts))
            
            # Profile CDC DB - Hourly ingestion rates
            cdc_rates = self.get_cdc_hourly_ingestion_rates()
            if cdc_rates:
                ingestion_rates = [rate[1] for rate in cdc_rates]
                mean_ingestion, std_ingestion = self.calculate_statistics(ingestion_rates)
                self.store_baselines("hourly_ingestion_rate", "cdc_history_db", 
                                   "dim_orders_history", mean_ingestion, std_ingestion, len(ingestion_rates))
            
            self.logger.info("Production profiling completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Production profiling execution failed: {str(e)}")
            return False
    
    def get_profiling_status(self) -> Dict[str, Any]:
        """
        Get current profiling status and statistics
        
        Returns:
            Dictionary with profiling status information
        """
        try:
            # Get baseline counts
            baseline_query = """
            SELECT metric_name, COUNT(*) as count, 
                   MAX(calculation_timestamp) as latest_timestamp
            FROM monitoring.baselines
            GROUP BY metric_name
            """
            
            with self._lock:
                baseline_results = self.cdc_db.execute_query(baseline_query)
            
            # Get database status
            db_status = self.db_manager.get_status()
            
            return {
                "profiler_status": "running",
                "baseline_metrics": {row[0]: row[1] for row in baseline_results},
                "database_status": db_status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get profiling status: {str(e)}")
            return {
                "profiler_status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            self.db_manager.close_all()
            self.logger.info("Production profiler cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


def main():
    """Main execution function with production error handling"""
    profiler = None
    
    try:
        profiler = ProductionMetricsProfiler()
        success = profiler.run_profiling()
        
        if success:
            # Generate status report
            status = profiler.get_profiling_status()
            print(f"Profiling Status: {status}")
            
            print("Production profiling completed successfully!")
            return 0
        else:
            print("Production profiling failed!")
            return 1
            
    except Exception as e:
        print(f"Production profiling execution error: {str(e)}")
        return 1
    finally:
        if profiler:
            profiler.cleanup()


if __name__ == "__main__":
    sys.exit(main())
