# Example Data

This directory contains sample input and output data to demonstrate the Data Observability Platform's functionality.

## Directory Structure

```
data/
├── input/
│   ├── sample_metrics.csv      # Sample batch analytics metrics
│   └── sample_cdc_logs.json    # Sample CDC stream data
└── output/
    ├── alerts.csv               # Expected alert outputs
    ├── baselines.csv            # Expected baseline calculations
    └── sample_health_report.md  # Expected portfolio health report
```

## Input Data

### sample_metrics.csv
Contains 30 days of daily order metrics with:
- **order_date**: Date of the order batch
- **daily_orders**: Number of orders processed that day
- **daily_revenue**: Total revenue for the day
- **avg_order_value**: Average order value

**Key Feature**: Day 15 shows a significant volume drop (342 orders vs expected ~1,289) to demonstrate anomaly detection.

### sample_cdc_logs.json
Contains sample CDC (Change Data Capture) logs with:
- **order_key**: Unique order identifier
- **customer_id**: Customer identifier
- **product_id**: Product identifier
- **quantity**: Order quantity
- **unit_price**: Price per unit
- **total_amount**: Total order amount
- **order_status**: Order processing status
- **order_date**: Order timestamp
- **cdc_operation**: Type of CDC operation (INSERT/UPDATE/DELETE)
- **cdc_timestamp**: When the CDC event was captured

## Expected Outputs

### alerts.csv
Demonstrates the types of alerts the platform generates:
- **VOLUME_ANOMALY**: Critical alert for order volume drop
- **FRESHNESS_ANOMALY**: Warning for stale CDC data
- **CONTRACT_VIOLATION**: Critical alert for data type mismatches

### baselines.csv
Shows calculated statistical baselines:
- **daily_order_count**: Mean and standard deviation for order volume
- **hourly_cdc_rate**: Mean and standard deviation for CDC events
- **avg_order_value**: Mean and standard deviation for order values

### sample_health_report.md
Example of the executive portfolio health report showing:
- Overall platform status (ATTENTION in this example)
- Component health scores
- Performance metrics
- Immediate action items
- Trend analysis

## Usage Examples

### Running with Sample Data
```bash
# Use sample data for testing
python src/observability_engine.py

# The platform will process your actual database data
# but these samples show what to expect
```

### Testing Anomaly Detection
```bash
# Run core functionality tests
python tests/test_core_functionality.py

# Run chaos engineering tests
python tests/chaos_volume.py
python tests/chaos_freshness.py
python tests/chaos_contract.py
```

## What This Demonstrates

1. **Statistical Anomaly Detection**: The volume drop in sample_metrics.csv triggers a VOLUME_ANOMALY alert
2. **Freshness Monitoring**: Stale CDC data triggers FRESHNESS_ANOMALY warnings
3. **Contract Validation**: Type mismatches trigger CONTRACT_VIOLATION alerts
4. **Baseline Calculation**: Statistical baselines are automatically calculated from historical data
5. **Executive Reporting**: Comprehensive health reports for stakeholders

## Real-World Context

In production, this platform would:
- Monitor real database tables instead of sample files
- Generate alerts in real-time as anomalies occur
- Update baselines continuously as new data arrives
- Send alerts via webhooks, email, or Slack
- Provide dashboards for monitoring and alert management

The sample data here represents a simplified version of what the platform processes in enterprise environments with thousands of metrics and millions of records.
