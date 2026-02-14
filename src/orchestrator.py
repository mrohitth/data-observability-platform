#!/usr/bin/env python3
"""
Data Observability Orchestrator

This script orchestrates the Multi-Source Profiler, Detection Engine, and Contract Guard
to provide comprehensive data reliability monitoring.

Usage:
    python src/orchestrator.py

Flow:
1. Run profiler to establish baselines
2. Run detection engine to check for anomalies
3. Run contract guard to prevent schema drift
4. Generate comprehensive health report
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent))

from profiler import MetricsProfiler
from detector import DetectionEngine
from contract_guard import ContractGuard


def main():
    """Main orchestration function"""
    print("🔍 Data Observability Orchestrator Starting...")
    print("=" * 70)
    
    # Step 1: Run profiler to establish/update baselines
    print("\n📊 Step 1: Running Multi-Source Profiler...")
    profiler = MetricsProfiler()
    profiler_success = profiler.run_profiling()
    
    if not profiler_success:
        print("❌ Profiler failed - cannot continue with detection")
        return 1
    
    print("✅ Profiler completed successfully")
    
    # Step 2: Run detection engine
    print("\n🚨 Step 2: Running Detection Engine...")
    detector = DetectionEngine()
    detection_results = detector.run_detection()
    
    # Step 3: Run contract guard for schema drift prevention
    print("\n📋 Step 3: Running Contract Guard...")
    guard = ContractGuard()
    contract_results = guard.run_contract_validation(
        use_database=True,
        use_json_logs=True
    )
    
    # Step 4: Generate comprehensive report
    print("\n📋 Step 4: Generating Comprehensive Report...")
    
    total_issues = (detection_results['total_anomalies'] + 
                   contract_results.get('total_violations', 0))
    
    print(f"\n{'='*70}")
    print("🎯 DATA OBSERVABILITY SUMMARY")
    print(f"{'='*70}")
    print(f"Profiler Status: ✅ Success")
    print(f"Volume Anomalies: {'🚨 DETECTED' if detection_results['volume_anomaly'] else '✅ Normal'}")
    print(f"Freshness Anomalies: {'🚨 DETECTED' if detection_results['freshness_anomaly'] else '✅ Normal'}")
    print(f"Contract Violations: {'🚨 DETECTED' if contract_results.get('total_violations', 0) > 0 else '✅ Normal'}")
    print(f"Total Issues: {total_issues}")
    
    if total_issues > 0:
        print(f"\n⚠️  ACTION REQUIRED: {total_issues} data quality issues detected!")
        print("📄 Check detection_summary.md for anomaly details")
        print("📄 Check contract_validation_report.md for contract violations")
        print("📄 Check data_health_scorecard.md for profiler results")
        return 1
    else:
        print(f"\n✅ ALL SYSTEMS NORMAL: No data quality issues detected")
        print("📄 Check data_health_scorecard.md for profiler results")
        return 0


if __name__ == "__main__":
    sys.exit(main())
