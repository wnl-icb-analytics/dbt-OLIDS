#!/usr/bin/env python3
"""Publish OLIDS stable models as DATA_LAKE views."""

import json
import sys

from snowflake_env import get_connection, load_env


EXPECTED_ROLE = 'DATA_PLATFORM_MANAGER'
DEPLOY_SQL = """
CALL DATA_LAKE.CONTROL.DEPLOY_DATA_LAKE_VIEWS_FOR_SINGLE_SOURCE_MAPPING(
    SOURCE_DATABASE => 'OLIDS_ENGINEERING',
    SOURCE_SCHEMA => 'STABLE',
    DESTINATION_SCHEMA => 'OLIDS'
)
"""
COMMENT_SYNC_SQL = """
CALL DATA_LAKE.CONTROL.SYNC_DATA_LAKE_VIEW_COMMENTS_FOR_SINGLE_SOURCE_MAPPING(
    SOURCE_DATABASE => 'OLIDS_ENGINEERING',
    SOURCE_SCHEMA => 'STABLE',
    DESTINATION_SCHEMA => 'OLIDS'
)
"""


def parse_response(value):
    """Return the procedure response as a dictionary."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise RuntimeError('Data lake deployment returned an invalid response')
    return {str(key).lower(): item for key, item in value.items()}


def response_failed(response):
    """Return the error flag, rejecting missing or invalid values."""
    if 'error_flag' not in response:
        raise RuntimeError('Data lake deployment response has no error flag')
    value = response['error_flag']
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {'true', 'false'}:
        return value.lower() == 'true'
    raise RuntimeError('Data lake deployment returned an invalid error flag')


def call_procedure(cur, sql, operation):
    """Call a deployment procedure and validate its response."""
    cur.execute(sql)
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f'{operation} returned no response')

    response = parse_response(row[0])
    if response_failed(response):
        raise RuntimeError(response.get('message', f'{operation} failed'))
    return response


def verify_mapping(cur):
    """Verify every stable object has one DPM-owned data lake view."""
    cur.execute(
        "SELECT table_name FROM OLIDS_ENGINEERING.information_schema.tables "
        "WHERE table_schema = 'STABLE'"
    )
    source_objects = {row[0] for row in cur.fetchall()}

    cur.execute(
        "SELECT table_name, table_owner FROM DATA_LAKE.information_schema.views "
        "WHERE table_schema = 'OLIDS'"
    )
    destination_views = {row[0]: row[1] for row in cur.fetchall()}

    missing = sorted(source_objects - destination_views.keys())
    extra = sorted(destination_views.keys() - source_objects)
    wrong_owner = sorted(
        name
        for name, owner in destination_views.items()
        if owner.upper() != EXPECTED_ROLE
    )
    errors = []
    if missing:
        errors.append('missing views: ' + ', '.join(missing))
    if extra:
        errors.append('unexpected views: ' + ', '.join(extra))
    if wrong_owner:
        errors.append(
            'views not owned by DATA_PLATFORM_MANAGER: '
            + ', '.join(wrong_owner)
        )
    if errors:
        raise RuntimeError('; '.join(errors))


def deploy(conn):
    """Deploy the OLIDS source mapping and fail on an error response."""
    cur = conn.cursor()
    try:
        cur.execute('SELECT CURRENT_ROLE()')
        role = cur.fetchone()[0]
        if role.upper() != EXPECTED_ROLE:
            raise RuntimeError(
                f'Expected role {EXPECTED_ROLE}, connected with {role}'
            )

        call_procedure(cur, DEPLOY_SQL, 'Data lake deployment')
        comment_response = call_procedure(
            cur, COMMENT_SYNC_SQL, 'View comment sync'
        )
        verify_mapping(cur)
        return comment_response.get('updated_view_count', 0)
    finally:
        cur.close()


def main():
    env = load_env()
    conn = get_connection(env)
    try:
        updated_comments = deploy(conn)
    finally:
        conn.close()
    print(
        'Published OLIDS_ENGINEERING.STABLE to DATA_LAKE.OLIDS; '
        f'synchronised {updated_comments} view comment(s).'
    )
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, ValueError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
