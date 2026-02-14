#!/usr/bin/env python3
"""
Portfolio Health Scorecard Generator

This script aggregates monitoring data from all observability components
to generate a comprehensive portfolio health report for the Data Observability Platform governance layer.

Detection Logic: Aggregates baselines, alerts, and validation results to calculate
overall platform reliability and component-specific health metrics.

Usage:
    python scripts/generate_scorecard.py
"""

import yaml
import logging
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import os
from pathlib import Path
import json


class DatabaseConnection:
    """Database connection manager for scorecard generation"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for scorecard operations"""
        logger = logging.Logger("scorecard_generator")
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


class PortfolioHealthCalculator:
    """
    Calculates portfolio health metrics from monitoring data
    
    Detection Logic: Aggregates baselines, alerts, and validation results
    to compute comprehensive health scores for the Data Observability Platform governance layer
    """
    
    def __init__(self, config_path: str = "config/databases.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        self.cdc_db = None
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for health calculations"""
        logger = logging.Logger("portfolio_health_calculator")
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
        """Load configuration"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                self.logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def initialize_connection(self) -> bool:
        """Initialize CDC database connection"""
        try:
            self.cdc_db = DatabaseConnection(self.config['databases']['cdc_history_db'])
            connected = self.cdc_db.connect()
            
            if connected:
                self.logger.info("CDC database connection established for scorecard generation")
                return True
            else:
                self.logger.error("Failed to establish CDC database connection")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection initialization failed: {str(e)}")
            return False
    
    def get_baseline_metrics(self) -> Dict[str, Any]:
        """
        Retrieve baseline metrics for health scoring
        
        Detection Logic: Fetch all baselines to understand expected behavior patterns
        Returns: Dictionary with baseline metrics and counts
        """
        query = """
        SELECT 
            metric_name,
            table_name,
            mean_value,
            std_deviation,
            sample_size,
            calculation_timestamp
        FROM monitoring.baselines
        ORDER BY calculation_timestamp DESC
        """
        
        try:
            results = self.cdc_db.execute_query(query)
            
            baselines = {
                'total_baselines': len(results),
                'metrics': {},
                'last_updated': None
            }
            
            for row in results:
                metric_name, table_name, mean_val, std_dev, sample_size, timestamp = row
                
                baselines['metrics'][f"{metric_name}_{table_name}"] = {
                    'metric_name': metric_name,
                    'table_name': table_name,
                    'mean_value': float(mean_val),
                    'std_deviation': float(std_dev),
                    'sample_size': int(sample_size),
                    'last_updated': timestamp
                }
                
                if not baselines['last_updated'] or timestamp > baselines['last_updated']:
                    baselines['last_updated'] = timestamp
            
            self.logger.info(f"Retrieved {baselines['total_baselines']} baseline metrics")
            return baselines
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve baseline metrics: {str(e)}")
            return {'total_baselines': 0, 'metrics': {}, 'last_updated': None}
    
    def get_alert_metrics(self) -> Dict[str, Any]:
        """
        Retrieve alert metrics for reliability scoring
        
        Detection Logic: Analyze alert patterns to calculate platform reliability
        Returns: Dictionary with alert counts and types
        """
        # Get alerts from last 24 hours
        recent_alerts_query = """
        SELECT 
            alert_type,
            alert_severity,
            COUNT(*) as alert_count,
            MAX(alert_timestamp) as latest_alert
        FROM monitoring.alerts
        WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        GROUP BY alert_type, alert_severity
        ORDER BY alert_count DESC
        """
        
        # Get total alerts for reliability calculation
        total_alerts_query = """
        SELECT 
            COUNT(*) as total_alerts,
            MIN(alert_timestamp) as earliest_alert,
            MAX(alert_timestamp) as latest_alert
        FROM monitoring.alerts
        """
        
        try:
            recent_results = self.cdc_db.execute_query(recent_alerts_query)
            total_results = self.cdc_db.execute_query(total_alerts_query)
            
            alerts = {
                'recent_24h': {},
                'total_alerts': 0,
                'alert_types': {},
                'critical_alerts': 0,
                'earliest_alert': None,
                'latest_alert': None
            }
            
            # Process recent alerts
            for row in recent_results:
                alert_type, severity, count, latest = row
                alerts['recent_24h'][alert_type] = {
                    'severity': severity,
                    'count': int(count),
                    'latest': latest
                }
                
                if severity == 'CRITICAL':
                    alerts['critical_alerts'] += int(count)
                
                alerts['alert_types'][alert_type] = alerts['alert_types'].get(alert_type, 0) + int(count)
            
            # Process total alerts
            if total_results:
                total_count, earliest, latest = total_results[0]
                alerts['total_alerts'] = int(total_count)
                alerts['earliest_alert'] = earliest
                alerts['latest_alert'] = latest
            
            self.logger.info(f"Retrieved alert metrics: {alerts['total_alerts']} total, {alerts['critical_alerts']} critical")
            return alerts
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve alert metrics: {str(e)}")
            return {'recent_24h': {}, 'total_alerts': 0, 'alert_types': {}, 'critical_alerts': 0}
    
    def calculate_platform_reliability_score(self, alerts: Dict[str, Any], 
                                            baselines: Dict[str, Any]) -> float:
        """
        Calculate Platform Reliability Score
        
        Detection Logic: Score based on alert frequency vs. baseline stability
        Formula: (1 - (critical_alerts_24h / total_baselines)) * 100
        Returns: Reliability score as percentage (0-100)
        """
        try:
            critical_alerts_24h = alerts.get('critical_alerts', 0)
            total_baselines = baselines.get('total_baselines', 1)
            
            # Base score starts at 100%
            base_score = 100.0
            
            # Deduct points for critical alerts
            if total_baselines > 0:
                alert_penalty = (critical_alerts_24h / max(total_baselines, 1)) * 50
                reliability_score = max(0, base_score - alert_penalty)
            else:
                reliability_score = 50.0  # Neutral score if no baselines
            
            # Additional penalty for high alert volume
            total_alerts_24h = sum(alert.get('count', 0) for alert in alerts.get('recent_24h', {}).values())
            if total_alerts_24h > 10:
                volume_penalty = min(20, (total_alerts_24h - 10) * 2)
                reliability_score = max(0, reliability_score - volume_penalty)
            
            self.logger.info(f"Calculated platform reliability score: {reliability_score:.1f}%")
            return round(reliability_score, 1)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate reliability score: {str(e)}")
            return 50.0  # Default to neutral score
    
    def calculate_batch_layer_status(self, alerts: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Batch Layer Health Status
        
        Detection Logic: Analyze VOLUME_ANOMALY alerts to determine batch layer health
        Returns: Status dictionary with health indicator and details
        """
        try:
            volume_anomalies = alerts.get('recent_24h', {}).get('VOLUME_ANOMALY', {})
            anomaly_count = volume_anomalies.get('count', 0)
            
            if anomaly_count == 0:
                status = 'Healthy'
                health_score = 100.0
                description = "No volume anomalies detected in the last 24 hours"
            elif anomaly_count <= 2:
                status = 'Minor Anomaly'
                health_score = 75.0
                description = f"Minor volume anomalies detected ({anomaly_count} occurrences)"
            else:
                status = 'Anomaly'
                health_score = max(0, 100 - (anomaly_count * 20))
                description = f"Significant volume anomalies detected ({anomaly_count} occurrences)"
            
            return {
                'status': status,
                'health_score': round(health_score, 1),
                'anomaly_count': anomaly_count,
                'latest_anomaly': volume_anomalies.get('latest'),
                'description': description
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate batch layer status: {str(e)}")
            return {'status': 'Unknown', 'health_score': 0.0, 'description': 'Unable to determine status'}
    
    def calculate_cdc_layer_status(self, alerts: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate CDC Layer Health Status
        
        Detection Logic: Analyze STALE_DATA_FLOW alerts to determine CDC layer health
        Returns: Status dictionary with health indicator and details
        """
        try:
            stale_alerts = alerts.get('recent_24h', {}).get('STALE_DATA_FLOW', {})
            stale_count = stale_alerts.get('count', 0)
            
            if stale_count == 0:
                status = 'Healthy'
                health_score = 100.0
                description = "No data flow staleness detected in the last 24 hours"
            elif stale_count <= 1:
                status = 'Minor Staleness'
                health_score = 80.0
                description = f"Minor data flow staleness detected ({stale_count} occurrence)"
            else:
                status = 'Stale'
                health_score = max(0, 100 - (stale_count * 25))
                description = f"Significant data flow staleness detected ({stale_count} occurrences)"
            
            return {
                'status': status,
                'health_score': round(health_score, 1),
                'stale_count': stale_count,
                'latest_stale_alert': stale_alerts.get('latest'),
                'description': description
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate CDC layer status: {str(e)}")
            return {'status': 'Unknown', 'health_score': 0.0, 'description': 'Unable to determine status'}
    
    def calculate_contract_status(self, alerts: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Contract Compliance Status
        
        Detection Logic: Analyze CONTRACT_VIOLATION alerts to determine contract compliance
        Returns: Status dictionary with compliance indicator and details
        """
        try:
            contract_violations = alerts.get('recent_24h', {}).get('CONTRACT_VIOLATION', {})
            violation_count = contract_violations.get('count', 0)
            
            if violation_count == 0:
                status = 'Compliant'
                compliance_score = 100.0
                description = "No contract violations detected in the last 24 hours"
            elif violation_count <= 3:
                status = 'Minor Violation'
                compliance_score = 85.0
                description = f"Minor contract violations detected ({violation_count} occurrences)"
            else:
                status = 'Violated'
                compliance_score = max(0, 100 - (violation_count * 15))
                description = f"Significant contract violations detected ({violation_count} occurrences)"
            
            return {
                'status': status,
                'compliance_score': round(compliance_score, 1),
                'violation_count': violation_count,
                'latest_violation': contract_violations.get('latest'),
                'description': description
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate contract status: {str(e)}")
            return {'status': 'Unknown', 'compliance_score': 0.0, 'description': 'Unable to determine status'}
    
    def generate_portfolio_health_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive portfolio health report
        
        Detection Logic: Aggregates all monitoring data for Data Observability Platform governance layer
        Returns: Complete health report with all metrics and scores
        """
        try:
            self.logger.info("Starting portfolio health report generation")
            
            # Initialize connection
            if not self.initialize_connection():
                self.logger.error("Failed to initialize database connection")
                return {}
            
            # Gather monitoring data
            baselines = self.get_baseline_metrics()
            alerts = self.get_alert_metrics()
            
            # Calculate health scores
            platform_reliability = self.calculate_platform_reliability_score(alerts, baselines)
            batch_status = self.calculate_batch_layer_status(alerts)
            cdc_status = self.calculate_cdc_layer_status(alerts)
            contract_status = self.calculate_contract_status(alerts)
            
            # Compile comprehensive report
            report = {
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'platform_reliability': {
                    'score': platform_reliability,
                    'status': 'Excellent' if platform_reliability >= 90 else 
                             'Good' if platform_reliability >= 75 else
                             'Fair' if platform_reliability >= 60 else 'Poor'
                },
                'batch_layer': batch_status,
                'cdc_layer': cdc_status,
                'contract_compliance': contract_status,
                'monitoring_summary': {
                    'total_baselines': baselines.get('total_baselines', 0),
                    'total_alerts': alerts.get('total_alerts', 0),
                    'critical_alerts_24h': alerts.get('critical_alerts', 0),
                    'alert_types': alerts.get('alert_types', {}),
                    'last_updated': baselines.get('last_updated'),
                    'latest_alert': alerts.get('latest_alert')
                }
            }
            
            self.logger.info(f"Portfolio health report generated: Platform reliability {platform_reliability}%")
            return report
            
        except Exception as e:
            self.logger.error(f"Portfolio health report generation failed: {str(e)}")
            return {}
        finally:
            # Cleanup connection
            if self.cdc_db:
                self.cdc_db.close()
    
    def format_health_report_markdown(self, report: Dict[str, Any]) -> str:
        """
        Format portfolio health report as Markdown
        
        Detection Logic: Creates comprehensive governance report for Data Observability Platform layer
        Returns: Formatted Markdown string
        """
        if not report:
            return "# Portfolio Health Report\n\nUnable to generate report - no data available.\n"
        
        platform = report['platform_reliability']
        batch = report['batch_layer']
        cdc = report['cdc_layer']
        contract = report['contract_compliance']
        summary = report['monitoring_summary']
        
        # Determine overall status
        overall_status = "🟢 HEALTHY"
        if (platform['score'] < 75 or batch['health_score'] < 75 or 
            cdc['health_score'] < 75 or contract['compliance_score'] < 75):
            overall_status = "🟡 ATTENTION"
        if (platform['score'] < 60 or batch['health_score'] < 60 or 
            cdc['health_score'] < 60 or contract['compliance_score'] < 60):
            overall_status = "🔴 CRITICAL"
        
        report_md = f"""# 🛡️ Data Observability Platform Portfolio Health Report

*Generated: {report['generated_at']}*  
*Governance Layer: Data Observability & Reliability*

---

## 📊 Executive Summary

**Overall Platform Status: {overall_status}**

| Component | Status | Health Score | Details |
|-----------|--------|--------------|---------|
| **Platform Reliability** | {platform['status']} | {platform['score']}% | Based on {summary['total_alerts']} total alerts |
| **Batch Layer** | {batch['status']} | {batch['health_score']}% | {batch['anomaly_count']} volume anomalies |
| **CDC Layer** | {cdc['status']} | {cdc['health_score']}% | {cdc['stale_count']} staleness alerts |
| **Contract Compliance** | {contract['status']} | {contract['compliance_score']}% | {contract['violation_count']} contract violations |

---

## 🎯 Platform Reliability Analysis

**Score: {platform['score']}% - Status: {platform['status']}**

### Reliability Factors
- **Baseline Metrics**: {summary['total_baselines']} active baselines
- **Critical Alerts (24h)**: {summary['critical_alerts_24h']}
- **Total Alerts**: {summary['total_alerts']}
- **Alert Distribution**: {', '.join([f'{k}: {v}' for k, v in summary['alert_types'].items()]) if summary['alert_types'] else 'None'}

### Reliability Trend
"""
        
        if platform['score'] >= 90:
            report_md += "✅ **Excellent**: Platform operating within normal parameters\n"
        elif platform['score'] >= 75:
            report_md += "⚠️ **Good**: Minor issues detected, platform stable\n"
        elif platform['score'] >= 60:
            report_md += "🔶 **Fair**: Multiple issues requiring attention\n"
        else:
            report_md += "🔴 **Poor**: Critical issues requiring immediate action\n"
        
        report_md += f"""
---

## 📦 Batch Layer Health

**Status: {batch['status']} - Health Score: {batch['health_score']}%**

### Volume Analysis
- **Volume Anomalies (24h)**: {batch['anomaly_count']}
- **Latest Anomaly**: {batch.get('latest_anomaly', 'None')}
- **Description**: {batch['description']}

### Batch Performance
"""
        
        if batch['health_score'] >= 90:
            report_md += "✅ **Healthy**: Normal volume patterns detected\n"
        elif batch['health_score'] >= 70:
            report_md += "⚠️ **Minor Anomaly**: Slight volume variations within acceptable range\n"
        else:
            report_md += "🔴 **Anomaly**: Significant volume deviations detected\n"
        
        report_md += f"""
---

## 🔄 CDC Layer Health

**Status: {cdc['status']} - Health Score: {cdc['health_score']}%**

### Freshness Analysis
- **Staleness Alerts (24h)**: {cdc['stale_count']}
- **Latest Stale Alert**: {cdc.get('latest_stale_alert', 'None')}
- **Description**: {cdc['description']}

### Data Flow Performance
"""
        
        if cdc['health_score'] >= 90:
            report_md += "✅ **Healthy**: Fresh data flow within acceptable thresholds\n"
        elif cdc['health_score'] >= 75:
            report_md += "⚠️ **Minor Staleness**: Brief delays in data processing\n"
        else:
            report_md += "🔴 **Stale**: Significant data flow delays detected\n"
        
        report_md += f"""
---

## 📋 Contract Compliance

**Status: {contract['status']} - Compliance Score: {contract['compliance_score']}%**

### Schema Drift Analysis
- **Contract Violations (24h)**: {contract['violation_count']}
- **Latest Violation**: {contract.get('latest_violation', 'None')}
- **Description**: {contract['description']}

### Data Quality Enforcement
"""
        
        if contract['compliance_score'] >= 90:
            report_md += "✅ **Compliant**: All contracts validated successfully\n"
        elif contract['compliance_score'] >= 80:
            report_md += "⚠️ **Minor Violation**: Few contract deviations detected\n"
        else:
            report_md += "🔴 **Violated**: Significant contract violations require attention\n"
        
        report_md += f"""
---

## 📈 Monitoring Infrastructure

### Data Collection Status
- **Total Baselines**: {summary['total_baselines']}
- **Last Baseline Update**: {summary.get('last_updated', 'Never')}
- **Total Alerts History**: {summary['total_alerts']}
- **Latest Alert**: {summary.get('latest_alert', 'Never')}

### Alert Breakdown (24h)
"""
        
        if summary['alert_types']:
            for alert_type, count in summary['alert_types'].items():
                report_md += f"- **{alert_type}**: {count}\n"
        else:
            report_md += "- No alerts in the last 24 hours\n"
        
        report_md += f"""
---

## 🎯 Governance Recommendations

### Immediate Actions
"""
        
        recommendations = []
        
        if platform['score'] < 75:
            recommendations.append("- 🔴 **High Priority**: Address critical platform reliability issues")
        
        if batch['health_score'] < 75:
            recommendations.append("- 📊 **Investigate**: Review batch processing volume anomalies")
        
        if cdc['health_score'] < 75:
            recommendations.append("- 🔄 **Optimize**: Investigate CDC data flow staleness")
        
        if contract['compliance_score'] < 75:
            recommendations.append("- 📋 **Enforce**: Review and update data contracts")
        
        if not recommendations:
            recommendations.append("- ✅ **Maintain**: Continue current monitoring practices")
        
        report_md += "\n".join(recommendations)
        
        report_md += f"""

### Long-term Improvements
- 📈 **Enhanced Monitoring**: Consider additional metrics for deeper insights
- 🤖 **Automation**: Implement automated remediation for common issues
- 📊 **Trend Analysis**: Establish longer-term baselines for trend detection
- 🔒 **Contract Evolution**: Regularly review and update data contracts

---

## 🛡️ Data Observability Platform Governance Layer

This report is generated by the **Data Observability Platform** governance layer, which provides:
- **Real-time Monitoring**: Continuous observation of data quality metrics
- **Anomaly Detection**: Proactive identification of data reliability issues  
- **Contract Enforcement**: Automated schema drift prevention
- **Health Scoring**: Quantitative assessment of platform health
- **Governance Reporting**: Executive-ready portfolio health insights

*Next scheduled report: {(datetime.now(timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S UTC')}*
"""
        
        return report_md


def main():
    """Main execution function"""
    calculator = PortfolioHealthCalculator()
    
    try:
        print("🛡️ Data Observability Platform Portfolio Health Report Generator Starting...")
        print("=" * 60)
        
        # Generate portfolio health report
        report = calculator.generate_portfolio_health_report()
        
        if not report:
            print("❌ Failed to generate portfolio health report")
            return 1
        
        # Format and save report
        report_md = calculator.format_health_report_markdown(report)
        
        with open("PORTFOLIO_HEALTH_REPORT.md", "w") as f:
            f.write(report_md)
        
        # Display summary
        platform_score = report['platform_reliability']['score']
        batch_status = report['batch_layer']['status']
        cdc_status = report['cdc_layer']['status']
        contract_status = report['contract_compliance']['status']
        
        print(f"\n📊 Portfolio Health Summary:")
        print(f"   Platform Reliability: {platform_score}%")
        print(f"   Batch Layer: {batch_status}")
        print(f"   CDC Layer: {cdc_status}")
        print(f"   Contract Compliance: {contract_status}")
        print(f"\n📄 Full report saved to: PORTFOLIO_HEALTH_REPORT.md")
        
        # Exit with appropriate code
        if platform_score < 75:
            print(f"\n⚠️  Platform reliability below threshold - attention required")
            return 1
        else:
            print(f"\n✅ Portfolio health report generated successfully")
            return 0
            
    except Exception as e:
        print(f"Portfolio health report generation error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
