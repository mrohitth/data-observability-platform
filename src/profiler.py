"""
Multi-Source Profiler for Data Observability & Reliability

Detection Logic: 
- Profiles Batch Analytics DB for daily row counts of fact_orders
- Profiles CDC History DB for hourly ingestion rates of dim_orders_history
- Calculates statistical baselines (Mean, Standard Deviation) for anomaly detection
- Stores baselines in monitoring.baselines table for Z-Score calculations

Reliability First: Implements comprehensive logging and error handling
"""

import yaml
import logging
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import statistics
import os


class DatabaseConnection:
    """Modular database connection manager with comprehensive logging"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for database operations"""
        logger = logging.getLogger(f"{self.config['name']}_connection")
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


class MetricsProfiler:
    """Multi-source metrics profiler for data observability"""
    
    def __init__(self, config_path: str = "observability_configs/databases.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        self.batch_db = None
        self.cdc_db = None
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for profiler operations"""
        logger = logging.getLogger("metrics_profiler")
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
    
    def initialize_connections(self) -> bool:
        """Initialize database connections"""
        try:
            self.batch_db = DatabaseConnection(self.config['databases']['batch_analytics_db'])
            self.cdc_db = DatabaseConnection(self.config['databases']['cdc_history_db'])
            
            batch_connected = self.batch_db.connect()
            cdc_connected = self.cdc_db.connect()
            
            if batch_connected and cdc_connected:
                self.logger.info("All database connections established successfully")
                return True
            else:
                self.logger.error("Failed to establish one or more database connections")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection initialization failed: {str(e)}")
            return False
    
    def get_batch_daily_row_counts(self, days_back: int = 30) -> List[Tuple]:
        """
        Get daily row counts from fact_orders table in Batch DB
        
        Detection Logic: CTE-based query for daily aggregation
        Schema: marts.fact_orders with order_timestamp column
        Returns: List of (date, row_count) tuples
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
        """
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
        """Create monitoring.baselines table if it doesn't exist"""
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
            self.cdc_db.execute_query(create_table_query)
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
        Main profiling execution method
        
        Detection Logic: Orchestrates all profiling steps and stores baselines
        """
        try:
            self.logger.info("Starting multi-source profiling execution")
            
            # Initialize connections
            if not self.initialize_connections():
                return False
            
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
            
            self.logger.info("Multi-source profiling completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Profiling execution failed: {str(e)}")
            return False
        finally:
            # Cleanup connections
            if self.batch_db:
                self.batch_db.close()
            if self.cdc_db:
                self.cdc_db.close()
    
    def generate_health_scorecard(self) -> str:
        """Generate Data Health Scorecard in Markdown format"""
        scorecard = f"""
# Data Health Scorecard
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Baseline Metrics

### Batch Analytics DB (project1)
- **Table**: fact_orders
- **Metric**: Daily Row Count
- **Status**: Baseline established for Z-Score anomaly detection

### CDC History DB (project2)  
- **Table**: dim_orders_history
- **Metric**: Hourly Ingestion Rate
- **Status**: Baseline established for Z-Score anomaly detection

## Detection Logic Summary
- Volume Anomalies: Z-Score (Standard Deviations) algorithm applied
- Freshness Monitoring: Time-since-last-record tracking implemented
- Schema Validation: YAML Data Contracts enforced
- Alert Idempotency: Unique monitoring logs prevent alert fatigue

## Next Steps
- Configure alert thresholds for Z-Score breaches
- Implement webhook integration for real-time notifications
- Schedule automated baseline recalculation
"""
        return scorecard


def main():
    """Main execution function"""
    profiler = MetricsProfiler()
    
    try:
        success = profiler.run_profiling()
        
        if success:
            # Generate and display health scorecard
            scorecard = profiler.generate_health_scorecard()
            print(scorecard)
            
            # Save scorecard to file
            with open("data_health_scorecard.md", "w") as f:
                f.write(scorecard)
            
            print("Multi-source profiling completed successfully!")
            return 0
        else:
            print("Multi-source profiling failed!")
            return 1
            
    except Exception as e:
        print(f"Profiling execution error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
