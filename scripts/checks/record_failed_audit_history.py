#!/usr/bin/env python3
"""Record audit metrics from a failed dbt pipeline attempt."""

import argparse
import sys

from audit_history import build_run_context, record_attempt
from check_source_watermarks import (
    LANDING_SCHEMA,
    SNAPSHOT_TABLES,
    fetch_existing_tables,
    fetch_watermarks,
)
from compare_sources_to_information_schema import parse_sources
from snowflake_env import get_connection, load_env


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbt-build-outcome', required=True)
    parser.add_argument('--publish-outcome', required=True)
    parser.add_argument('--verify-outcome', required=True)
    args = parser.parse_args()

    env = load_env()
    target_database = env.get('SNOWFLAKE_TARGET_DATABASE')
    if not target_database:
        raise SystemExit('Missing environment variable: SNOWFLAKE_TARGET_DATABASE')

    source_database, source_schema, _ = parse_sources()
    conn = get_connection(env)
    try:
        source_watermarks = fetch_watermarks(
            conn, source_database, source_schema, SNAPSHOT_TABLES
        )
        existing = fetch_existing_tables(
            conn, target_database, LANDING_SCHEMA
        )
        landing_watermarks = fetch_watermarks(
            conn,
            target_database,
            LANDING_SCHEMA,
            SNAPSHOT_TABLES,
            existing=existing,
        )
        context = build_run_context(
            env,
            dbt_build_outcome=args.dbt_build_outcome,
            publish_outcome=args.publish_outcome,
            verify_outcome=args.verify_outcome,
            pipeline_status='failed',
        )
        audit_table_count = record_attempt(
            conn,
            target_database,
            context,
            source_watermarks,
            landing_watermarks,
        )
    finally:
        conn.close()

    print(
        f'Recorded failed attempt {context.run_id}/{context.run_attempt} '
        f'from {audit_table_count} audit tables.'
    )
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, ValueError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
