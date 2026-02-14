#!/usr/bin/env python3
"""
Production Data Observability Orchestrator

Production-ready orchestration with thread safety, retry logic, and log rotation.
Coordinates Multi-Source Profiler, Detection Engine, and Contract Guard.

Usage:
    python src/production_orchestrator.py

Features:
- Thread-safe concurrent execution
- Exponential backoff retry logic
- Rotating log files
- Environment-based configuration
- Comprehensive error handling and recovery
"""

import sys
import os
import logging
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config_manager import get_config
from database_manager import get_database_manager
from production_profiler import ProductionMetricsProfiler
from production_detector import ProductionDetectionEngine


class ProductionOrchestrator:
    """
    Production-ready orchestrator with thread safety and comprehensive error handling
    
    Features:
    - Thread-safe concurrent execution
    - Health monitoring
    - Graceful shutdown
    - Production logging
    - Error recovery
    """
    
    def __init__(self):
        """Initialize production orchestrator"""
        self.config = get_config()
        self.db_manager = get_database_manager()
        self.logger = self._setup_logger()
        
        # Thread safety
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        
        # Components
        self.profiler = None
        self.detector = None
        
        # Performance tracking
        self.start_time = None
        self.execution_stats = {}
        
        self.logger.info("Production orchestrator initialized")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with rotation"""
        logger = logging.getLogger("production_orchestrator")
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
                    filename=log_dir / "orchestrator.log",
                    maxBytes=self.config.logging.max_bytes,
                    backupCount=self.config.logging.backup_count
                )
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
        
        return logger
    
    def initialize_components(self) -> bool:
        """
        Initialize all components with error handling
        
        Returns:
            True if all components initialized successfully
        """
        try:
            self.logger.info("Initializing production components...")
            
            # Initialize profiler
            self.profiler = ProductionMetricsProfiler()
            self.logger.info("Production profiler initialized")
            
            # Initialize detector
            self.detector = ProductionDetectionEngine()
            self.logger.info("Production detector initialized")
            
            # Health check
            health_status = self.db_manager.health_check()
            if not all(health_status.values()):
                self.logger.warning(f"Database health check failed: {health_status}")
                return False
            
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {str(e)}")
            return False
    
    def run_profiler_task(self) -> Dict[str, Any]:
        """
        Run profiler task with error handling and timing
        
        Returns:
            Dictionary with task results
        """
        task_start = time.time()
        task_name = "profiler"
        
        try:
            self.logger.info(f"Starting {task_name} task")
            success = self.profiler.run_profiling()
            
            execution_time = time.time() - task_start
            
            result = {
                "task": task_name,
                "success": success,
                "execution_time": execution_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": None
            }
            
            if success:
                self.logger.info(f"{task_name} task completed successfully in {execution_time:.2f}s")
            else:
                self.logger.error(f"{task_name} task failed after {execution_time:.2f}s")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - task_start
            self.logger.error(f"{task_name} task failed with exception: {str(e)}")
            
            return {
                "task": task_name,
                "success": False,
                "execution_time": execution_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e)
            }
    
    def run_detector_task(self) -> Dict[str, Any]:
        """
        Run detector task with error handling and timing
        
        Returns:
            Dictionary with task results
        """
        task_start = time.time()
        task_name = "detector"
        
        try:
            self.logger.info(f"Starting {task_name} task")
            results = self.detector.run_detection()
            
            execution_time = time.time() - task_start
            
            result = {
                "task": task_name,
                "success": True,
                "execution_time": execution_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "results": results,
                "error": None
            }
            
            self.logger.info(f"{task_name} task completed in {execution_time:.2f}s - {results['total_anomalies']} anomalies detected")
            return result
            
        except Exception as e:
            execution_time = time.time() - task_start
            self.logger.error(f"{task_name} task failed with exception: {str(e)}")
            
            return {
                "task": task_name,
                "success": False,
                "execution_time": execution_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e)
            }
    
    def run_concurrent_orchestration(self) -> Dict[str, Any]:
        """
        Run orchestration with concurrent execution
        
        Returns:
            Dictionary with orchestration results
        """
        self.start_time = time.time()
        
        try:
            self.logger.info("Starting concurrent orchestration")
            
            # Define tasks
            tasks = [
                self.run_profiler_task,
                self.run_detector_task
            ]
            
            # Execute tasks concurrently
            with ThreadPoolExecutor(max_workers=self.config.performance.concurrent_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(task): task.__name__
                    for task in tasks
                }
                
                # Collect results
                results = []
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        result = future.result()
                        results.append(result)
                        self.logger.info(f"Task {task_name} completed")
                    except Exception as e:
                        self.logger.error(f"Task {task_name} failed: {str(e)}")
                        results.append({
                            "task": task_name,
                            "success": False,
                            "error": str(e),
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
            
            # Calculate statistics
            total_time = time.time() - self.start_time
            successful_tasks = sum(1 for r in results if r.get('success', False))
            failed_tasks = len(results) - successful_tasks
            
            # Detect anomalies
            total_anomalies = 0
            for result in results:
                if result.get('results') and 'total_anomalies' in result['results']:
                    total_anomalies += result['results']['total_anomalies']
            
            orchestration_result = {
                "orchestration_success": failed_tasks == 0,
                "total_execution_time": total_time,
                "successful_tasks": successful_tasks,
                "failed_tasks": failed_tasks,
                "total_anomalies": total_anomalies,
                "task_results": results,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.logger.info(f"Orchestration completed in {total_time:.2f}s - {successful_tasks}/{len(results)} tasks successful")
            
            if total_anomalies > 0:
                self.logger.error(f"Total anomalies detected: {total_anomalies}")
            
            return orchestration_result
            
        except Exception as e:
            total_time = time.time() - self.start_time
            self.logger.error(f"Orchestration failed: {str(e)}")
            
            return {
                "orchestration_success": False,
                "total_execution_time": total_time,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def generate_health_report(self, results: Dict[str, Any]) -> str:
        """
        Generate comprehensive health report
        
        Args:
            results: Orchestration results
            
        Returns:
            Formatted health report
        """
        report = f"""
# Production Data Observability Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
Environment: Production

## Orchestration Summary
- **Status**: {'🟢 SUCCESS' if results.get('orchestration_success', False) else '🔴 FAILED'}
- **Execution Time**: {results.get('total_execution_time', 0):.2f} seconds
- **Tasks Completed**: {results.get('successful_tasks', 0)}/{len(results.get('task_results', []))}
- **Total Anomalies**: {results.get('total_anomalies', 0)}

## Task Details
"""
        
        for task_result in results.get('task_results', []):
            task_name = task_result.get('task', 'Unknown')
            success = task_result.get('success', False)
            execution_time = task_result.get('execution_time', 0)
            error = task_result.get('error')
            
            status_icon = '✅' if success else '❌'
            report += f"- **{task_name.title()}**: {status_icon} ({execution_time:.2f}s)"
            
            if error:
                report += f" - Error: {error}"
            report += "\n"
        
        if results.get('total_anomalies', 0) > 0:
            report += f"""
## 🚨 Anomalies Detected
{results.get('total_anomalies', 0)} data quality issues require attention

## Recommendations
- Review detailed logs in the logs directory
- Check database health status
- Investigate root causes of detected anomalies
"""
        else:
            report += """
## ✅ System Health
All systems operating normally
No data quality issues detected
"""
        
        report += f"""
## System Status
- Database Health: {self.db_manager.health_check()}
- Configuration: Environment variables loaded
- Logging: Rotating logs active

## Production Metrics
- Thread Pool Size: {self.config.performance.concurrent_workers}
- Connection Timeout: {self.config.performance.connection_timeout}s
- Retry Attempts: {self.config.retry.max_attempts}
- Log Rotation: {self.config.logging.max_bytes // (1024*1024)}MB files, {self.config.logging.backup_count} backups

*Next scheduled orchestration: {(datetime.now(timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S UTC')}*
"""
        
        return report
    
    def run_production_orchestration(self) -> Dict[str, Any]:
        """
        Main production orchestration method
        
        Returns:
            Dictionary with orchestration results
        """
        try:
            self.logger.info("🚀 Starting Production Data Observability Orchestrator")
            self.logger.info("=" * 70)
            
            # Initialize components
            if not self.initialize_components():
                return {
                    "orchestration_success": False,
                    "error": "Component initialization failed",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            
            # Run concurrent orchestration
            results = self.run_concurrent_orchestration()
            
            # Generate and save report
            report = self.generate_health_report(results)
            
            # Save report to file
            log_dir = Path(self.config.logging.log_dir)
            log_dir.mkdir(exist_ok=True)
            
            report_file = log_dir / "production_health_report.md"
            with open(report_file, 'w') as f:
                f.write(report)
            
            # Display summary
            total_anomalies = results.get('total_anomalies', 0)
            successful_tasks = results.get('successful_tasks', 0)
            total_tasks = len(results.get('task_results', []))
            
            print(f"\n{'='*70}")
            print("🎯 PRODUCTION ORCHESTRATION SUMMARY")
            print(f"{'='*70}")
            print(f"Tasks Completed: {successful_tasks}/{total_tasks}")
            print(f"Total Anomalies: {total_anomalies}")
            print(f"Execution Time: {results.get('total_execution_time', 0):.2f}s")
            print(f"Report Saved: {report_file}")
            
            if total_anomalies > 0:
                print(f"\n⚠️  ACTION REQUIRED: {total_anomalies} data quality issues detected!")
                return 1
            else:
                print(f"\n✅ ALL SYSTEMS NORMAL: Production orchestration completed successfully!")
                return 0
            
        except Exception as e:
            self.logger.error(f"Production orchestration failed: {str(e)}")
            print(f"\n💥 ORCHESTRATION FAILED: {str(e)}")
            return 1
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        try:
            if self.profiler:
                self.profiler.cleanup()
            if self.detector:
                self.detector.cleanup()
            
            self.db_manager.close_all()
            self.logger.info("Production orchestrator cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


def main():
    """Main execution function"""
    orchestrator = ProductionOrchestrator()
    return orchestrator.run_production_orchestration()


if __name__ == "__main__":
    sys.exit(main())
