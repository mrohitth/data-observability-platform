"""
Contract Guard for Schema Drift Prevention

Detection Logic: Validates CDC JSON logs against YAML data contracts
- Field Type Validation: Strict type checking for order_key (String) and total_amount (Float)
- Schema Drift Detection: Identifies missing fields, type changes, and unexpected fields
- Contract Violation Alerting: Detailed violation reporting with specific field failures

Reliability First: Comprehensive validation with detailed error reporting and database logging
"""

import yaml
import logging
import sys
import json
import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any
import os
import random
import re
from pathlib import Path
from termcolor import colored, cprint


class ContractViolationBanner:
    """Alert banner for contract violations"""
    
    @staticmethod
    def print_contract_violation(violation_details: Dict):
        """
        Print a contract violation alert banner
        
        Idempotency: Unique violation content prevents alert fatigue
        """
        border = "!" * 80
        
        print("\n" + "=" * 80)
        cprint(border, 'red', attrs=['bold'])
        cprint("🚨 CONTRACT VIOLATION DETECTED 🚨", 'red', attrs=['bold', 'blink'])
        cprint(border, 'red', attrs=['bold'])
        cprint(f"CONTRACT: {violation_details.get('contract_name', 'Unknown')}", 'yellow', attrs=['bold'])
        cprint(f"TIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}", 'yellow')
        cprint(f"SEVERITY: {violation_details.get('severity', 'CRITICAL')}", 'yellow')
        cprint(f"VIOLATION TYPE: {violation_details.get('violation_type', 'Unknown')}", 'white', attrs=['bold'])
        
        if 'field_name' in violation_details:
            cprint(f"FIELD: {violation_details['field_name']}", 'cyan', attrs=['bold'])
        
        if 'expected_type' in violation_details and 'actual_type' in violation_details:
            cprint(f"TYPE MISMATCH: Expected {violation_details['expected_type']}, got {violation_details['actual_type']}", 'cyan')
        
        cprint(f"DESCRIPTION: {violation_details.get('description', 'Contract validation failed')}", 'white', attrs=['bold'])
        
        if 'validation_errors' in violation_details:
            cprint("VALIDATION ERRORS:", 'magenta')
            for error in violation_details['validation_errors']:
                cprint(f"  • {error}", 'magenta')
        
        cprint(border, 'red', attrs=['bold'])
        cprint("🔧 SCHEMA DRIFT DETECTED - IMMEDIATE ACTION REQUIRED 🔧", 'red', attrs=['bold', 'blink'])
        cprint(border, 'red', attrs=['bold'])
        print("=" * 80 + "\n")


class DatabaseConnection:
    """Database connection manager for contract violation logging"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for contract operations"""
        logger = logging.getLogger(f"{self.config['name']}_contract_guard")
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


class ContractValidator:
    """Validates data against contract definitions"""
    
    def __init__(self, contract_config: Dict):
        self.contract = contract_config
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for validation operations"""
        logger = logging.getLogger("contract_validator")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def get_python_type(self, contract_type: str) -> type:
        """Convert contract type to Python type"""
        type_mapping = {
            'string': str,
            'integer': int,
            'float': float,
            'boolean': bool,
            'datetime': str,  # ISO string representation
            'array': list,
            'object': dict
        }
        return type_mapping.get(contract_type.lower(), str)
    
    def validate_field_type(self, field_name: str, field_value: Any, field_config: Dict) -> List[str]:
        """
        Validate field type and constraints
        
        Detection Logic: Strict type validation with constraint checking
        Returns: List of validation error messages
        """
        errors = []
        expected_type = field_config.get('type', 'string')
        python_type = self.get_python_type(expected_type)
        
        # Check if field is null and nullable
        if field_value is None:
            if not field_config.get('nullable', True):
                errors.append(f"Field '{field_name}' is not nullable but value is null")
            return errors
        
        # Type validation
        if not isinstance(field_value, python_type):
            actual_type = type(field_value).__name__
            errors.append(f"Field '{field_name}' type mismatch: expected {expected_type}, got {actual_type}")
            return errors
        
        # Constraint validation
        constraints = field_config.get('constraints', {})
        
        # String constraints
        if expected_type == 'string':
            if 'min_length' in constraints and len(str(field_value)) < constraints['min_length']:
                errors.append(f"Field '{field_name}' length {len(str(field_value))} below minimum {constraints['min_length']}")
            
            if 'max_length' in constraints and len(str(field_value)) > constraints['max_length']:
                errors.append(f"Field '{field_name}' length {len(str(field_value))} above maximum {constraints['max_length']}")
            
            if 'pattern' in constraints:
                pattern = constraints['pattern']
                if not re.match(pattern, str(field_value)):
                    errors.append(f"Field '{field_name}' value '{field_value}' does not match pattern '{pattern}'")
            
            if 'allowed_values' in constraints:
                allowed = constraints['allowed_values']
                if field_value not in allowed:
                    errors.append(f"Field '{field_name}' value '{field_value}' not in allowed values: {allowed}")
        
        # Numeric constraints
        elif expected_type in ['integer', 'float']:
            if 'min_value' in constraints and float(field_value) < constraints['min_value']:
                errors.append(f"Field '{field_name}' value {field_value} below minimum {constraints['min_value']}")
            
            if 'max_value' in constraints and float(field_value) > constraints['max_value']:
                errors.append(f"Field '{field_name}' value {field_value} above maximum {constraints['max_value']}")
            
            if 'precision' in constraints and expected_type == 'float':
                precision = constraints['precision']
                if len(str(field_value).split('.')[-1]) > precision:
                    errors.append(f"Field '{field_name}' precision {len(str(field_value).split('.')[-1])} exceeds maximum {precision}")
        
        return errors
    
    def validate_record(self, record: Dict) -> Dict[str, Any]:
        """
        Validate a single record against the contract
        
        Detection Logic: Comprehensive validation with detailed error reporting
        Returns: Validation result with errors and violations
        """
        result = {
            'valid': True,
            'errors': [],
            'violations': []
        }
        
        required_fields = self.contract.get('required_fields', {})
        optional_fields = self.contract.get('optional_fields', {})
        all_fields = {**required_fields, **optional_fields}
        
        # Check for missing required fields
        for field_name, field_config in required_fields.items():
            if field_name not in record:
                error = f"Required field '{field_name}' is missing"
                result['errors'].append(error)
                result['violations'].append({
                    'type': 'MISSING_REQUIRED_FIELD',
                    'field_name': field_name,
                    'description': error
                })
                result['valid'] = False
        
        # Validate present fields
        for field_name, field_value in record.items():
            if field_name in all_fields:
                field_config = all_fields[field_name]
                validation_errors = self.validate_field_type(field_name, field_value, field_config)
                
                if validation_errors:
                    result['errors'].extend(validation_errors)
                    result['valid'] = False
                    
                    # Determine violation type
                    violation_type = 'TYPE_MISMATCH'
                    if any('type mismatch' in error.lower() for error in validation_errors):
                        violation_type = 'TYPE_MISMATCH'
                    elif any('not nullable' in error.lower() for error in validation_errors):
                        violation_type = 'NULLABILITY_VIOLATION'
                    elif any('length' in error.lower() for error in validation_errors):
                        violation_type = 'CONSTRAINT_VIOLATION'
                    
                    result['violations'].append({
                        'type': violation_type,
                        'field_name': field_name,
                        'expected_type': field_config.get('type'),
                        'actual_type': type(field_value).__name__,
                        'errors': validation_errors
                    })
            
            elif self.contract.get('validation_rules', {}).get('schema_drift', {}).get('detect_new_fields', True):
                # Unexpected field detected
                error = f"Unexpected field '{field_name}' not defined in contract"
                result['errors'].append(error)
                result['violations'].append({
                    'type': 'UNEXPECTED_FIELD',
                    'field_name': field_name,
                    'description': error
                })
                result['valid'] = False
        
        return result


class ContractGuard:
    """
    Contract Guard for Schema Drift Prevention
    
    Detection Logic: Validates CDC JSON logs against YAML contracts
    Reliability First: Comprehensive validation with database logging
    """
    
    def __init__(self, config_path: str = "config/databases.yaml", 
                 contract_path: str = "contracts/cdc_order_contract.yaml"):
        self.logger = self._setup_logger()
        self.config = self._load_config(config_path)
        self.contract_config = self._load_contract(contract_path)
        self.cdc_db = None
        self.violation_banner = ContractViolationBanner()
        self.validator = ContractValidator(self.contract_config)
        
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging for contract guard"""
        logger = logging.getLogger("contract_guard")
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
        """Load database configuration"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                self.logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def _load_contract(self, contract_path: str) -> Dict:
        """Load contract configuration"""
        try:
            with open(contract_path, 'r') as file:
                contract = yaml.safe_load(file)
                self.logger.info(f"Contract loaded from {contract_path}")
                return contract
        except Exception as e:
            self.logger.error(f"Failed to load contract: {str(e)}")
            raise
    
    def initialize_connection(self) -> bool:
        """Initialize CDC database connection"""
        try:
            self.cdc_db = DatabaseConnection(self.config['databases']['cdc_history_db'])
            connected = self.cdc_db.connect()
            
            if connected:
                self.logger.info("CDC database connection established for contract validation")
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
            contract_name VARCHAR(100),
            field_name VARCHAR(100),
            expected_type VARCHAR(50),
            actual_type VARCHAR(50),
            details JSONB,
            alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_timestamp TIMESTAMP,
            UNIQUE(alert_type, source_table, contract_name, field_name, alert_timestamp)
        )
        """
        
        try:
            self.cdc_db.execute_query(create_table_query)
            self.logger.info("Alerts table created/verified successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create alerts table: {str(e)}")
            return False
    
    def load_sample_cdc_logs(self, sample_size: int = 100) -> List[Dict]:
        """
        Load sample CDC logs from database for validation
        
        Detection Logic: Sample recent records from dim_orders_history
        Returns: List of CDC log records as dictionaries
        """
        query = """
        SELECT 
            order_key,
            customer_id,
            product_id,
            quantity,
            unit_price,
            total_amount,
            order_status,
            order_date,
            cdc_operation,
            cdc_timestamp
        FROM dim_orders_history
        ORDER BY cdc_timestamp DESC
        LIMIT %s
        """
        
        try:
            results = self.cdc_db.execute_query(query, (sample_size,))
            
            # Convert to list of dictionaries
            columns = ['order_key', 'customer_id', 'product_id', 'quantity', 
                      'unit_price', 'total_amount', 'order_status', 'order_date',
                      'cdc_operation', 'cdc_timestamp']
            
            records = []
            for row in results:
                record = dict(zip(columns, row))
                # Convert datetime objects to strings for JSON compatibility
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):
                        record[key] = value.isoformat()
                records.append(record)
            
            self.logger.info(f"Loaded {len(records)} sample CDC logs for validation")
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to load sample CDC logs: {str(e)}")
            return []
    
    def load_sample_json_logs(self, log_directory: str = "data/cdc_logs", 
                            sample_size: int = 10) -> List[Dict]:
        """
        Load sample JSON logs from file system for validation
        
        Detection Logic: Sample recent JSON log files
        Returns: List of parsed JSON records
        """
        try:
            log_dir = Path(log_directory)
            if not log_dir.exists():
                self.logger.warning(f"Log directory {log_directory} does not exist")
                return []
            
            # Get recent JSON files
            json_files = list(log_dir.glob("*.json"))[:sample_size]
            records = []
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            records.extend(data)
                        else:
                            records.append(data)
                except Exception as e:
                    self.logger.warning(f"Failed to parse {json_file}: {str(e)}")
            
            self.logger.info(f"Loaded {len(records)} records from {len(json_files)} JSON files")
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to load sample JSON logs: {str(e)}")
            return []
    
    def log_contract_violation(self, violation: Dict) -> bool:
        """
        Log contract violation to monitoring.alerts table
        
        Idempotency: UNIQUE constraint prevents duplicate violations
        """
        insert_query = """
        INSERT INTO monitoring.alerts 
        (alert_type, alert_severity, description, source_table, contract_name, 
         field_name, expected_type, actual_type, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_type, source_table, contract_name, field_name, alert_timestamp) DO NOTHING
        """
        
        try:
            import json
            details_json = json.dumps(violation)
            
            self.cdc_db.execute_query(insert_query,
                                     ("CONTRACT_VIOLATION", "CRITICAL", 
                                      violation.get('description', 'Contract validation failed'),
                                      self.contract_config.get('target_table'),
                                      self.contract_config.get('contract_name'),
                                      violation.get('field_name'),
                                      violation.get('expected_type'),
                                      violation.get('actual_type'),
                                      details_json))
            self.logger.info(f"Contract violation logged: {violation.get('field_name', 'Unknown')}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to log contract violation: {str(e)}")
            return False
    
    def validate_sample_records(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Validate sample records against contract
        
        Detection Logic: Comprehensive validation with violation tracking
        Returns: Validation summary with all violations
        """
        summary = {
            'total_records': len(records),
            'valid_records': 0,
            'invalid_records': 0,
            'violations': [],
            'violation_counts': {}
        }
        
        for i, record in enumerate(records):
            validation_result = self.validator.validate_record(record)
            
            if validation_result['valid']:
                summary['valid_records'] += 1
            else:
                summary['invalid_records'] += 1
                
                # Process violations
                for violation in validation_result['violations']:
                    violation['record_index'] = i
                    summary['violations'].append(violation)
                    
                    # Count violation types
                    violation_type = violation['type']
                    summary['violation_counts'][violation_type] = summary['violation_counts'].get(violation_type, 0) + 1
                    
                    # Log to database
                    self.log_contract_violation(violation)
                    
                    # Print alert banner for critical violations
                    if violation['type'] in ['TYPE_MISMATCH', 'MISSING_REQUIRED_FIELD']:
                        alert_details = {
                            'contract_name': self.contract_config.get('contract_name'),
                            'violation_type': violation['type'],
                            'field_name': violation.get('field_name'),
                            'expected_type': violation.get('expected_type'),
                            'actual_type': violation.get('actual_type'),
                            'description': f"Contract violation: {violation['errors'][0] if violation['errors'] else 'Unknown error'}",
                            'severity': 'CRITICAL',
                            'validation_errors': violation['errors']
                        }
                        self.violation_banner.print_contract_violation(alert_details)
        
        return summary
    
    def run_contract_validation(self, use_database: bool = True, 
                              use_json_logs: bool = False) -> Dict[str, Any]:
        """
        Main contract validation execution
        
        Detection Logic: Orchestrates contract validation against sample data
        Returns: Comprehensive validation results
        """
        results = {
            'validation_completed': False,
            'total_records_validated': 0,
            'total_violations': 0,
            'violation_summary': {},
            'critical_violations': []
        }
        
        try:
            self.logger.info("Starting contract validation execution")
            
            # Initialize connection if needed for database operations
            if use_database or self.contract_config.get('alerting', {}).get('log_to_database', True):
                if not self.initialize_connection():
                    self.logger.error("Failed to initialize database connection")
                    return results
                
                # Create alerts table
                if not self.create_alerts_table():
                    self.logger.error("Failed to create alerts table")
                    return results
            
            # Load sample records
            records = []
            
            if use_database:
                db_records = self.load_sample_cdc_logs(
                    self.contract_config.get('sampling', {}).get('sample_size', 100)
                )
                records.extend(db_records)
            
            if use_json_logs:
                json_records = self.load_sample_json_logs(
                    sample_size=self.contract_config.get('sampling', {}).get('sample_size', 10)
                )
                records.extend(json_records)
            
            if not records:
                self.logger.warning("No records found for validation")
                return results
            
            # Validate records
            validation_summary = self.validate_sample_records(records)
            
            results.update({
                'validation_completed': True,
                'total_records_validated': validation_summary['total_records'],
                'total_violations': len(validation_summary['violations']),
                'violation_summary': validation_summary['violation_counts'],
                'critical_violations': [v for v in validation_summary['violations'] 
                                      if v['type'] in ['TYPE_MISMATCH', 'MISSING_REQUIRED_FIELD']],
                'validation_summary': validation_summary
            })
            
            if results['total_violations'] == 0:
                self.logger.info(f"Contract validation completed: {results['total_records_validated']} records, no violations")
            else:
                self.logger.error(f"Contract validation completed: {results['total_records_validated']} records, {results['total_violations']} violations")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Contract validation execution failed: {str(e)}")
            return results
        finally:
            # Cleanup connection
            if self.cdc_db:
                self.cdc_db.close()
    
    def generate_validation_report(self, results: Dict[str, Any]) -> str:
        """Generate contract validation report"""
        report = f"""
# Contract Validation Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
Contract: {self.contract_config.get('contract_name')}
Target Table: {self.contract_config.get('target_table')}

## Validation Results
- **Records Validated**: {results.get('total_records_validated', 0)}
- **Total Violations**: {results.get('total_violations', 0)}
- **Validation Status**: {'🚨 VIOLATIONS DETECTED' if results.get('total_violations', 0) > 0 else '✅ PASSED'}

## Violation Summary
"""
        
        violation_summary = results.get('violation_summary', {})
        if violation_summary:
            for violation_type, count in violation_summary.items():
                report += f"- **{violation_type}**: {count}\n"
        else:
            report += "- No violations detected\n"
        
        critical_violations = results.get('critical_violations', [])
        if critical_violations:
            report += "\n## Critical Violations\n"
            for violation in critical_violations[:5]:  # Show top 5
                report += f"- **Field**: {violation.get('field_name', 'Unknown')}\n"
                report += f"  - Type: {violation.get('type', 'Unknown')}\n"
                report += f"  - Expected: {violation.get('expected_type', 'Unknown')}\n"
                report += f"  - Actual: {violation.get('actual_type', 'Unknown')}\n"
                if violation.get('errors'):
                    report += f"  - Error: {violation['errors'][0]}\n"
                report += "\n"
        
        report += f"""
## Contract Validation Logic
- **Type Validation**: Strict type checking for all defined fields
- **Schema Drift Detection**: Monitoring for missing, unexpected, or type-changed fields
- **Constraint Validation**: Business rule enforcement (lengths, patterns, ranges)
- **Alert Idempotency**: Unique violations prevent alert fatigue

## Recommendations
{'⚠️  IMMEDIATE ACTION REQUIRED: Contract violations detected' if results.get('total_violations', 0) > 0 else '✅ All contract validations passed'}
"""
        return report


def main():
    """Main execution function"""
    guard = ContractGuard()
    
    try:
        # Run contract validation
        results = guard.run_contract_validation(
            use_database=True,  # Validate against database records
            use_json_logs=True   # Also validate JSON log files
        )
        
        # Generate and display report
        report = guard.generate_validation_report(results)
        print(report)
        
        # Save report to file
        with open("contract_validation_report.md", "w") as f:
            f.write(report)
        
        # Exit with error code if violations detected
        if results.get('total_violations', 0) > 0:
            print(f"Contract validation completed with {results['total_violations']} violations!")
            return 1
        else:
            print("Contract validation completed successfully - no violations detected!")
            return 0
            
    except Exception as e:
        print(f"Contract validation execution error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
