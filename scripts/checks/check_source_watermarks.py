#!/usr/bin/env python3
"""Check OLIDS snapshot freshness and decide whether dbt needs to run."""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from compare_sources_to_information_schema import parse_sources
from snowflake_env import get_connection, load_env

LANDING_SCHEMA = 'LANDING'
STATE_SCHEMA = 'AUDIT'
STATE_TABLE = 'PIPELINE_STATE'
LONDON = ZoneInfo('Europe/London')

# These source tables publish one lds_transform_datetime for the whole snapshot.
# Reading one row avoids a full scan solely to recover that batch watermark.
SNAPSHOT_TABLES = (
    'ALLERGY_INTOLERANCE',
    'APPOINTMENT',
    'APPOINTMENT_PRACTITIONER',
    'DIAGNOSTIC_ORDER',
    'ENCOUNTER',
    'EPISODE_OF_CARE',
    'EPISODE_OF_CARE_V2',
    'LOCATION',
    'MEDICATION_ORDER',
    'MEDICATION_STATEMENT',
    'OBSERVATION',
    'ORGANISATION',
    'PATIENT',
    'PATIENT_ADDRESS',
    'PATIENT_CONTACT',
    'PATIENT_PERSON',
    'PERSON',
    'PRACTITIONER',
    'PRACTITIONER_IN_ROLE',
    'PROCEDURE_REQUEST',
    'REFERRAL_REQUEST',
    'SCHEDULE',
    'SCHEDULE_PRACTITIONER',
)


def quote_identifier(value):
    """Quote a Snowflake identifier."""
    return '"' + value.replace('"', '""') + '"'


def fetch_existing_tables(conn, database, schema):
    """Return table names visible in a target schema."""
    query = (
        'SELECT table_name '
        f'FROM {quote_identifier(database)}.information_schema.tables '
        'WHERE table_schema = %s'
    )
    cur = conn.cursor()
    try:
        cur.execute(query, (schema,))
        return {row[0].upper() for row in cur.fetchall()}
    finally:
        cur.close()


def fetch_watermarks(conn, database, schema, tables, existing=None):
    """Read the snapshot batch watermark from each table."""
    watermarks = {}
    cur = conn.cursor()
    try:
        for table in tables:
            if existing is not None and table not in existing:
                watermarks[table] = None
                continue
            relation = '.'.join(
                quote_identifier(part) for part in (database, schema, table)
            )
            cur.execute(
                f'SELECT lds_transform_datetime FROM {relation} LIMIT 1'
            )
            row = cur.fetchone()
            watermarks[table] = row[0] if row else None
    finally:
        cur.close()
    return watermarks


def normalise_timestamp(value):
    """Return a timezone-aware timestamp."""
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=LONDON)
    return value


def format_watermark(value):
    """Format a watermark for logs."""
    return value.isoformat() if isinstance(value, datetime) else 'missing'


def write_outputs(path, should_run, watermark):
    """Write step outputs when running in GitHub Actions."""
    if not path:
        return
    with Path(path).open('a', encoding='utf-8') as handle:
        handle.write(f'should_run={str(should_run).lower()}\n')
        handle.write(f'source_watermark={watermark}\n')


def wait_for_current_source(
    conn, database, schema, timeout, interval, max_age_hours
):
    """Poll until every source snapshot is present and within the age SLA."""
    deadline = time.monotonic() + timeout
    while True:
        watermarks = fetch_watermarks(
            conn, database, schema, SNAPSHOT_TABLES
        )
        now = datetime.now(timezone.utc)
        oldest_allowed = (
            now - timedelta(hours=max_age_hours)
            if max_age_hours is not None
            else None
        )
        newest_allowed = now + timedelta(minutes=15)
        stale = {
            table: value
            for table, value in watermarks.items()
            if value is None
            or (
                oldest_allowed is not None
                and not (
                    oldest_allowed
                    <= normalise_timestamp(value).astimezone(timezone.utc)
                    <= newest_allowed
                )
            )
        }
        if not stale:
            latest = max(watermarks.values())
            message = f'All {len(watermarks)} source snapshots are available'
            if max_age_hours is not None:
                message += f' and no more than {max_age_hours:g} hours old'
            print(f'{message}; latest watermark {format_watermark(latest)}.')
            return watermarks

        print(f'{len(stale)} source snapshot(s) are missing or stale:')
        for table, value in stale.items():
            print(f'  {table}: {format_watermark(value)}')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError('Source watermark wait timed out')
        wait = min(interval, remaining)
        print(f'Retrying in {wait:.0f} seconds.')
        time.sleep(wait)


def compare_target(conn, database, source_watermarks):
    """Return target tables whose snapshot watermark differs from source."""
    existing = fetch_existing_tables(conn, database, LANDING_SCHEMA)
    target_watermarks = fetch_watermarks(
        conn,
        database,
        LANDING_SCHEMA,
        SNAPSHOT_TABLES,
        existing=existing,
    )
    changed = {
        table: (source_watermarks[table], target_watermarks[table])
        for table in SNAPSHOT_TABLES
        if source_watermarks[table] != target_watermarks[table]
    }
    return changed


def fetch_processed_watermarks(conn, database):
    """Return watermarks recorded by the last successful full build."""
    existing = fetch_existing_tables(conn, database, STATE_SCHEMA)
    if STATE_TABLE not in existing:
        return {table: None for table in SNAPSHOT_TABLES}

    relation = '.'.join(
        quote_identifier(part) for part in (database, STATE_SCHEMA, STATE_TABLE)
    )
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT table_name, source_watermark FROM {relation} '
            "WHERE pipeline_name = 'dbt-olids'"
        )
        recorded = {row[0].upper(): row[1] for row in cur.fetchall()}
    finally:
        cur.close()
    return {table: recorded.get(table) for table in SNAPSHOT_TABLES}


def compare_processed(conn, database, source_watermarks):
    """Return source tables not recorded by the last successful full build."""
    processed = fetch_processed_watermarks(conn, database)
    return {
        table: (source_watermarks[table], processed[table])
        for table in SNAPSHOT_TABLES
        if source_watermarks[table] != processed[table]
    }


def record_success(conn, database, source_watermarks):
    """Record the source snapshot set consumed by a successful full build."""
    schema = '.'.join(
        quote_identifier(part) for part in (database, STATE_SCHEMA)
    )
    relation = f'{schema}.{quote_identifier(STATE_TABLE)}'
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        cur.execute(
            f'CREATE TABLE IF NOT EXISTS {relation} ('
            'pipeline_name VARCHAR NOT NULL, '
            'table_name VARCHAR NOT NULL, '
            'source_watermark TIMESTAMP_TZ NOT NULL, '
            'completed_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), '
            'PRIMARY KEY (pipeline_name, table_name) NOT ENFORCED)'
        )
        cur.execute('BEGIN')
        cur.execute(
            f"DELETE FROM {relation} WHERE pipeline_name = 'dbt-olids'"
        )
        cur.executemany(
            f'INSERT INTO {relation} '
            '(pipeline_name, table_name, source_watermark) VALUES (%s, %s, %s)',
            [
                ('dbt-olids', table, source_watermarks[table])
                for table in SNAPSHOT_TABLES
            ],
        )
        cur.execute('COMMIT')
    except Exception:
        cur.execute('ROLLBACK')
        raise
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(
        description='Wait for OLIDS source snapshots and compare landing watermarks'
    )
    parser.add_argument('--timeout-seconds', type=int, default=0)
    parser.add_argument('--poll-seconds', type=int, default=300)
    parser.add_argument(
        '--max-age-hours',
        type=float,
        help='Fail when a source watermark is older than this age',
    )
    parser.add_argument('--force', action='store_true', help='Build even when current')
    parser.add_argument(
        '--verify', action='store_true', help='Fail unless landing matches source'
    )
    parser.add_argument(
        '--record-success',
        action='store_true',
        help='Record watermarks after successful verification',
    )
    parser.add_argument('--github-output', help='Path from the GITHUB_OUTPUT variable')
    args = parser.parse_args()

    source_database, source_schema, source_contract = parse_sources()
    missing = sorted(set(SNAPSHOT_TABLES) - set(source_contract))
    if missing:
        raise SystemExit(
            'Watermark tables missing from sources.yml: ' + ', '.join(missing)
        )

    env = load_env()
    target_database = env.get('SNOWFLAKE_TARGET_DATABASE')
    if not target_database:
        raise SystemExit('Missing environment variable: SNOWFLAKE_TARGET_DATABASE')

    conn = get_connection(env)
    try:
        source_watermarks = wait_for_current_source(
            conn,
            source_database,
            source_schema,
            max(args.timeout_seconds, 0),
            max(args.poll_seconds, 1),
            args.max_age_hours,
        )
        if args.verify:
            changed = compare_target(conn, target_database, source_watermarks)
            if not changed and args.record_success:
                record_success(conn, target_database, source_watermarks)
        else:
            changed = compare_processed(
                conn, target_database, source_watermarks
            )
    finally:
        conn.close()

    if changed:
        subject = 'landing snapshot' if args.verify else 'processed watermark'
        print(f'{len(changed)} {subject}(s) differ from source:')
        for table, (source, target) in changed.items():
            print(
                f'  {table}: source={format_watermark(source)}, '
                f'target={format_watermark(target)}'
            )
    else:
        subject = 'Landing snapshot' if args.verify else 'Processed watermarks'
        print(f'{subject} match source.')

    latest = format_watermark(max(source_watermarks.values()))
    if args.verify:
        if changed:
            print('Landing did not catch up to source.', file=sys.stderr)
            return 1
        if args.record_success:
            print('Recorded the successful full-build watermarks.')
        write_outputs(args.github_output, False, latest)
        return 0

    should_run = args.force or bool(changed)
    if args.force and not changed:
        print('A build was forced even though landing is current.')
    elif not should_run:
        print('No new source snapshot; skipping dbt build.')
    write_outputs(args.github_output, should_run, latest)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except RuntimeError as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
