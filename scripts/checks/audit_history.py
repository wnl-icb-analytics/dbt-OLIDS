"""Persist OLIDS audit metrics for each pipeline attempt."""

from dataclasses import dataclass
from uuid import uuid4


PIPELINE_NAME = 'dbt-olids'
STATE_SCHEMA = 'AUDIT'
RUN_HISTORY_TABLE = 'PIPELINE_RUN_HISTORY'
WATERMARK_HISTORY_TABLE = 'WATERMARK_HISTORY'
METRIC_HISTORY_TABLE = 'METRIC_HISTORY'


def quote_identifier(value):
    """Quote a Snowflake identifier."""
    return '"' + value.replace('"', '""') + '"'


def relation(database, table):
    """Return a quoted relation in the audit schema."""
    return '.'.join(
        quote_identifier(part) for part in (database, STATE_SCHEMA, table)
    )


@dataclass(frozen=True)
class RunContext:
    """Identifiers and outcomes for one pipeline attempt."""

    run_id: str
    run_attempt: int
    git_sha: str | None
    event_name: str | None
    dbt_build_outcome: str
    publish_outcome: str
    verify_outcome: str
    pipeline_status: str


def build_run_context(
    env,
    *,
    dbt_build_outcome,
    publish_outcome,
    verify_outcome,
    pipeline_status,
):
    """Build run metadata from GitHub Actions or a local fallback."""
    run_id = env.get('GITHUB_RUN_ID') or f'local-{uuid4()}'
    try:
        run_attempt = int(env.get('GITHUB_RUN_ATTEMPT', '1'))
    except ValueError as exc:
        raise ValueError('GITHUB_RUN_ATTEMPT must be an integer') from exc
    return RunContext(
        run_id=run_id,
        run_attempt=run_attempt,
        git_sha=env.get('GITHUB_SHA'),
        event_name=env.get('GITHUB_EVENT_NAME'),
        dbt_build_outcome=dbt_build_outcome,
        publish_outcome=publish_outcome,
        verify_outcome=verify_outcome,
        pipeline_status=pipeline_status,
    )


def ensure_history_tables(cur, database):
    """Create append-only audit history tables when absent."""
    schema = '.'.join(
        quote_identifier(part) for part in (database, STATE_SCHEMA)
    )
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS {relation(database, RUN_HISTORY_TABLE)} ('
        'pipeline_name VARCHAR NOT NULL, '
        'run_id VARCHAR NOT NULL, '
        'run_attempt NUMBER NOT NULL, '
        'git_sha VARCHAR, '
        'event_name VARCHAR, '
        'dbt_build_outcome VARCHAR NOT NULL, '
        'publish_outcome VARCHAR NOT NULL, '
        'verify_outcome VARCHAR NOT NULL, '
        'pipeline_status VARCHAR NOT NULL, '
        'recorded_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), '
        'PRIMARY KEY (pipeline_name, run_id, run_attempt) NOT ENFORCED)'
    )
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS '
        f'{relation(database, WATERMARK_HISTORY_TABLE)} ('
        'pipeline_name VARCHAR NOT NULL, '
        'run_id VARCHAR NOT NULL, '
        'run_attempt NUMBER NOT NULL, '
        'table_name VARCHAR NOT NULL, '
        'source_watermark TIMESTAMP_TZ, '
        'landing_watermark TIMESTAMP_TZ, '
        'recorded_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), '
        'PRIMARY KEY '
        '(pipeline_name, run_id, run_attempt, table_name) NOT ENFORCED)'
    )
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS {relation(database, METRIC_HISTORY_TABLE)} ('
        'pipeline_name VARCHAR NOT NULL, '
        'run_id VARCHAR NOT NULL, '
        'run_attempt NUMBER NOT NULL, '
        'audit_model VARCHAR NOT NULL, '
        'table_name VARCHAR NOT NULL, '
        'metric_name VARCHAR NOT NULL, '
        'practice_code VARCHAR, '
        'value_number NUMBER(38, 4), '
        'value_text VARCHAR, '
        'value_date DATE, '
        'recorded_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP())'
    )


def fetch_audit_metric_tables(cur, database):
    """Return current-state audit metric tables."""
    cur.execute(
        f'SELECT table_name FROM '
        f'{quote_identifier(database)}.information_schema.tables '
        "WHERE table_schema = 'AUDIT' "
        "AND table_type = 'BASE TABLE' "
        "AND STARTSWITH(table_name, 'AUDIT_') "
        'ORDER BY table_name'
    )
    return [row[0] for row in cur.fetchall()]


def insert_attempt_history(
    cur,
    database,
    context,
    source_watermarks,
    landing_watermarks,
    audit_tables,
):
    """Replace one attempt key and insert its watermarks and metrics."""
    key = (PIPELINE_NAME, context.run_id, context.run_attempt)
    for table in (
        METRIC_HISTORY_TABLE,
        WATERMARK_HISTORY_TABLE,
        RUN_HISTORY_TABLE,
    ):
        cur.execute(
            f'DELETE FROM {relation(database, table)} '
            'WHERE pipeline_name = %s AND run_id = %s AND run_attempt = %s',
            key,
        )

    cur.execute(
        f'INSERT INTO {relation(database, RUN_HISTORY_TABLE)} ('
        'pipeline_name, run_id, run_attempt, git_sha, event_name, '
        'dbt_build_outcome, publish_outcome, verify_outcome, pipeline_status'
        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
        (
            *key,
            context.git_sha,
            context.event_name,
            context.dbt_build_outcome,
            context.publish_outcome,
            context.verify_outcome,
            context.pipeline_status,
        ),
    )
    cur.executemany(
        f'INSERT INTO {relation(database, WATERMARK_HISTORY_TABLE)} ('
        'pipeline_name, run_id, run_attempt, table_name, '
        'source_watermark, landing_watermark'
        ') VALUES (%s, %s, %s, %s, %s, %s)',
        [
            (
                *key,
                table,
                source_watermarks.get(table),
                landing_watermarks.get(table),
            )
            for table in sorted(
                set(source_watermarks) | set(landing_watermarks)
            )
        ],
    )

    metric_relation = relation(database, METRIC_HISTORY_TABLE)
    for audit_table in audit_tables:
        source_relation = '.'.join(
            quote_identifier(part)
            for part in (database, STATE_SCHEMA, audit_table)
        )
        cur.execute(
            f'INSERT INTO {metric_relation} ('
            'pipeline_name, run_id, run_attempt, audit_model, '
            'table_name, metric_name, practice_code, value_number, '
            'value_text, value_date'
            ') SELECT %s, %s, %s, %s, table_name, metric_name, '
            f'practice_code, value_number, value_text, value_date FROM '
            f'{source_relation}',
            (*key, audit_table),
        )


def record_attempt(
    conn,
    database,
    context,
    source_watermarks,
    landing_watermarks,
):
    """Record one pipeline attempt in a transaction."""
    cur = conn.cursor()
    try:
        ensure_history_tables(cur, database)
        audit_tables = fetch_audit_metric_tables(cur, database)
        cur.execute('BEGIN')
        insert_attempt_history(
            cur,
            database,
            context,
            source_watermarks,
            landing_watermarks,
            audit_tables,
        )
        cur.execute('COMMIT')
        return len(audit_tables)
    except Exception:
        cur.execute('ROLLBACK')
        raise
    finally:
        cur.close()
