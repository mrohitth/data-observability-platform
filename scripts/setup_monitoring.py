#!/usr/bin/env python3
"""
Database Setup Script

Creates the monitoring schema and tables required for Data Observability Platform testing.
"""

import sys
import os
import yaml
import psycopg2
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))


def setup_monitoring_schema():
    """Setup monitoring schema and tables"""
    print("🔧 Setting up monitoring schema...")
    
    # Load configuration
    with open("config/databases.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    try:
        # Connect to CDC database
        conn = psycopg2.connect(
            config['databases']['cdc_history_db']['connection_string']
        )
        cursor = conn.cursor()
        
        # Create monitoring schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS monitoring")
        
        # Create baselines table
        cursor.execute("""
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
        """)
        
        # Create alerts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monitoring.alerts (
                id SERIAL PRIMARY KEY,
                alert_type VARCHAR(50) NOT NULL,
                alert_severity VARCHAR(20) NOT NULL DEFAULT 'CRITICAL',
                description TEXT NOT NULL,
                source_table VARCHAR(100),
                contract_name VARCHAR(100),
                field_name VARCHAR(100),
                expected_type VARCHAR(50),
                actual_type VARCHAR(50),
                metric_value DECIMAL(15,4),
                threshold_value DECIMAL(15,4),
                z_score DECIMAL(10,4),
                details JSONB,
                alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved BOOLEAN DEFAULT FALSE,
                resolved_timestamp TIMESTAMP,
                UNIQUE(alert_type, source_table, contract_name, field_name, alert_timestamp)
            )
        """)
        
        # Create dim_orders_history table for testing
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_orders_history (
                surrogate_key BIGSERIAL PRIMARY KEY,
                order_key INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                total_amount DECIMAL(10,2) NOT NULL,
                order_status VARCHAR(50) NOT NULL,
                order_date TIMESTAMP NOT NULL,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP,
                is_current BOOLEAN DEFAULT TRUE,
                cdc_operation VARCHAR(10) NOT NULL,
                cdc_timestamp TIMESTAMP NOT NULL,
                batch_id VARCHAR(64),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Monitoring schema setup complete!")
        return True
        
    except Exception as e:
        print(f"❌ Failed to setup monitoring schema: {e}")
        return False


if __name__ == "__main__":
    success = setup_monitoring_schema()
    sys.exit(0 if success else 1)
