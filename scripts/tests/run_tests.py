#!/usr/bin/env python3
"""
OLIDS Test Runner

Discovers and executes all test_*.sql files in the same directory.
Produces a summary report with PASS/FAIL counts and details.

Usage:
    python run_tests.py
    python run_tests.py --test test_data_freshness.sql
    python run_tests.py --verbose
"""

import os
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import snowflake connector
try:
    from snowflake.snowpark import Session
    USE_SNOWPARK = True
except ImportError:
    try:
        import snowflake.connector
        USE_SNOWPARK = False
    except ImportError:
        print("ERROR: Neither snowflake-snowpark-python nor snowflake-connector-python installed")
        sys.exit(1)


# Configuration from environment
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
ROLE = os.getenv('SNOWFLAKE_ROLE')


def get_connection():
    """Create Snowflake connection using SSO."""
    connection_params = {
        "account": ACCOUNT,
        "user": USER,
        "authenticator": "externalbrowser",
        "warehouse": WAREHOUSE,
        "role": ROLE
    }
    
    if USE_SNOWPARK:
        return Session.builder.configs(connection_params).create()
    else:
        return snowflake.connector.connect(**connection_params)


def discover_tests(test_dir: Path, specific_test: str = None) -> list:
    """Find all test_*.sql files in the directory."""
    if specific_test:
        test_file = test_dir / specific_test
        if test_file.exists():
            return [test_file]
        else:
            print(f"ERROR: Test file not found: {specific_test}")
            sys.exit(1)
    
    tests = sorted(test_dir.glob("test_*.sql"))
    return tests


def execute_test(conn, sql_file: Path) -> list:
    """Execute a test SQL file and return results."""
    sql = sql_file.read_text(encoding='utf-8')
    
    if USE_SNOWPARK:
        # Snowpark returns a DataFrame
        df = conn.sql(sql).to_pandas()
        return df.to_dict('records')
    else:
        # Connector returns cursor
        cursor = conn.cursor()
        try:
            # Execute multi-statement SQL
            for statement in sql.split(';'):
                statement = statement.strip()
                if statement and not statement.startswith('--'):
                    cursor.execute(statement)
            
            # Fetch results from last query
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()


def print_results(all_results: dict, verbose: bool = False):
    """Print formatted test results to console."""
    total_tests = 0
    total_pass = 0
    total_fail = 0
    total_warn = 0
    
    print("\n" + "=" * 80)
    print("OLIDS DATA QUALITY TEST RESULTS")
    print("=" * 80)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    for test_file, results in all_results.items():
        if not results:
            print(f"\n⚠️  {test_file}: No results returned")
            continue
        
        # Count by status
        pass_count = sum(1 for r in results if r.get('STATUS') == 'PASS')
        fail_count = sum(1 for r in results if r.get('STATUS') == 'FAIL')
        warn_count = sum(1 for r in results if r.get('STATUS') == 'WARN')
        
        total_tests += len(results)
        total_pass += pass_count
        total_fail += fail_count
        total_warn += warn_count
        
        # Test header
        status_emoji = "✅" if fail_count == 0 else "❌"
        print(f"\n{status_emoji} {test_file}")
        print(f"   PASS: {pass_count} | FAIL: {fail_count}" + (f" | WARN: {warn_count}" if warn_count else ""))
        
        # Show failures
        failures = [r for r in results if r.get('STATUS') == 'FAIL']
        if failures:
            print("\n   Failures:")
            for f in failures:
                table = f.get('TABLE_NAME', 'N/A')
                subject = f.get('TEST_SUBJECT', 'N/A')
                metric = f.get('METRIC_VALUE', 'N/A')
                threshold = f.get('THRESHOLD', 'N/A')
                print(f"   - {table}.{subject}: {metric}% (threshold: {threshold}%)")
                
                if verbose:
                    details = f.get('DETAILS', '{}')
                    if isinstance(details, str):
                        try:
                            details = json.loads(details)
                        except:
                            pass
                    if isinstance(details, dict):
                        for k, v in details.items():
                            print(f"       {k}: {v}")
        
        # Show warnings
        warnings = [r for r in results if r.get('STATUS') == 'WARN']
        if warnings and verbose:
            print("\n   Warnings:")
            for w in warnings:
                table = w.get('TABLE_NAME', 'N/A')
                subject = w.get('TEST_SUBJECT', 'N/A')
                print(f"   - {table}.{subject}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total checks: {total_tests}")
    print(f"Passed: {total_pass} ({100*total_pass/total_tests:.1f}%)" if total_tests else "Passed: 0")
    print(f"Failed: {total_fail} ({100*total_fail/total_tests:.1f}%)" if total_tests else "Failed: 0")
    if total_warn:
        print(f"Warnings: {total_warn}")
    
    overall = "✅ ALL TESTS PASSED" if total_fail == 0 else f"❌ {total_fail} TESTS FAILED"
    print(f"\n{overall}")
    print("=" * 80)
    
    return total_fail == 0


def main():
    parser = argparse.ArgumentParser(description='Run OLIDS data quality tests')
    parser.add_argument('--test', '-t', help='Run specific test file only')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    args = parser.parse_args()
    
    # Find tests
    test_dir = Path(__file__).parent
    tests = discover_tests(test_dir, args.test)
    
    if not tests:
        print("No test files found (test_*.sql)")
        sys.exit(1)
    
    print(f"Found {len(tests)} test file(s)")
    for t in tests:
        print(f"  - {t.name}")
    
    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    try:
        conn = get_connection()
        print("✓ Connected successfully")
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        sys.exit(1)
    
    # Execute tests
    all_results = {}
    try:
        for test_file in tests:
            print(f"\nExecuting: {test_file.name}...")
            try:
                results = execute_test(conn, test_file)
                all_results[test_file.name] = results
                print(f"  → {len(results)} checks completed")
            except Exception as e:
                print(f"  → ERROR: {e}")
                all_results[test_file.name] = []
    finally:
        if USE_SNOWPARK:
            conn.close()
        else:
            conn.close()
        print("\nConnection closed")
    
    # Print results
    all_passed = print_results(all_results, verbose=args.verbose)
    
    # Exit code
    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
