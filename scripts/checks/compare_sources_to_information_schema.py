#!/usr/bin/env python3
"""Compare the olids_pseudo dbt source contract with live Snowflake.

Parses models/sources.yml, queries INFORMATION_SCHEMA for the same database/schema,
and reports tables and columns that drift. Exit code 1 on any difference, so it can
gate CI. Run after upstream releases; regenerate sources.yml when it reports drift.

Usage:
    python scripts/checks/compare_sources_to_information_schema.py
    python scripts/checks/compare_sources_to_information_schema.py --verbose
"""

import argparse
import sys

import yaml

from snowflake_env import REPO_ROOT, load_env, get_connection

SOURCE_NAME = 'olids_pseudo'


def unquote(value):
    """Strip dbt quoted-identifier wrapping: '"Data_Store_OLIDS_WNL"' -> Data_Store_OLIDS_WNL."""
    return str(value).strip().strip('"').strip("'")


def parse_sources():
    """Return (database, schema, {table: {column: data_type}}) for olids_pseudo."""
    path = REPO_ROOT / 'models' / 'sources.yml'
    data = yaml.safe_load(path.read_text(encoding='utf-8'))
    for source in data.get('sources', []):
        if source.get('name') != SOURCE_NAME:
            continue
        database = unquote(source['database'])
        schema = unquote(source['schema'])
        tables = {}
        for table in source.get('tables', []):
            name = unquote(table.get('identifier', table['name'])).upper()
            cols = {}
            for col in table.get('columns', []) or []:
                cols[unquote(col['name']).upper()] = (col.get('data_type') or '').upper()
            tables[name] = cols
        return database, schema, tables
    raise SystemExit(f"Source '{SOURCE_NAME}' not found in {path}")


def fetch_live(conn, database, schema):
    """Return {table: {column: data_type}} from INFORMATION_SCHEMA."""
    query = (
        'SELECT table_name, column_name, data_type, ordinal_position '
        f'FROM "{database}".information_schema.columns '
        f"WHERE table_schema = '{schema}' "
        'ORDER BY table_name, ordinal_position'
    )
    cur = conn.cursor()
    try:
        cur.execute(query)
        tables = {}
        for table_name, column_name, data_type, _pos in cur.fetchall():
            tables.setdefault(table_name.upper(), {})[column_name.upper()] = (data_type or '').upper()
        return tables
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description='Compare sources.yml to live Snowflake schema')
    parser.add_argument('--verbose', action='store_true', help='Also list matching tables')
    args = parser.parse_args()

    database, schema, spec = parse_sources()
    env = load_env()
    print(f'Comparing {SOURCE_NAME}: "{database}"."{schema}"')
    print('Connecting to Snowflake...')
    conn = get_connection(env)
    try:
        live = fetch_live(conn, database, schema)
    finally:
        conn.close()

    only_spec_tables = sorted(set(spec) - set(live))
    only_live_tables = sorted(set(live) - set(spec))
    shared = sorted(set(spec) & set(live))
    diffs = 0

    if only_spec_tables:
        diffs += len(only_spec_tables)
        print('\nTables in sources.yml but not live:')
        for t in only_spec_tables:
            print(f'  - {t}')
    if only_live_tables:
        diffs += len(only_live_tables)
        print('\nTables live but not in sources.yml:')
        for t in only_live_tables:
            print(f'  - {t}')

    for t in shared:
        s_cols, l_cols = spec[t], live[t]
        only_spec = sorted(set(s_cols) - set(l_cols))
        only_live = sorted(set(l_cols) - set(s_cols))
        # Case-insensitive compare; skip columns without a declared data_type.
        mismatches = sorted(
            (c, s_cols[c], l_cols[c])
            for c in set(s_cols) & set(l_cols)
            if s_cols[c] and l_cols[c] and s_cols[c] != l_cols[c]
        )
        if not (only_spec or only_live or mismatches):
            if args.verbose:
                print(f'\n{t}: OK ({len(s_cols)} columns)')
            continue
        diffs += len(only_spec) + len(only_live) + len(mismatches)
        print(f'\n{t}:')
        for c in only_spec:
            print(f'  - column only in sources.yml: {c}')
        for c in only_live:
            print(f'  - column only live: {c}')
        for c, sd, ld in mismatches:
            print(f'  - data_type mismatch: {c} sources.yml={sd} live={ld}')

    print(f'\nSummary: {len(only_spec_tables)} spec-only table(s), '
          f'{len(only_live_tables)} live-only table(s), '
          f'{len(shared)} shared table(s), {diffs} difference(s).')
    return 1 if diffs else 0


if __name__ == '__main__':
    sys.exit(main())
