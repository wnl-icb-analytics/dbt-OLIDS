#!/usr/bin/env python3
"""Run the OLIDS checks suite against Snowflake.

Executes each checks/*.sql, substitutes {TARGET_DATABASE}, and prints an aligned
table per check. PASS/INFO rows are hidden unless --verbose; WARN/FAIL always show.
Exit code 1 if any FAIL row is returned, else 0.

Usage:
    python scripts/checks/run_checks.py
    python scripts/checks/run_checks.py --check source_freshness
    python scripts/checks/run_checks.py --verbose
    python scripts/checks/run_checks.py --csv output/
"""

import argparse
import csv
import sys
from pathlib import Path

from snowflake_env import load_env, get_connection

CHECKS_DIR = Path(__file__).parent / 'checks'


def discover(name=None):
    """Return the check files to run."""
    if name:
        f = CHECKS_DIR / (name if name.endswith('.sql') else name + '.sql')
        if not f.exists():
            raise SystemExit(f"Check not found: {f.name}")
        return [f]
    files = sorted(CHECKS_DIR.glob('*.sql'))
    if not files:
        raise SystemExit(f"No checks found in {CHECKS_DIR}")
    return files


def run_check(conn, sql_file, target_db):
    """Execute one check and return (columns, rows-as-dicts)."""
    sql = sql_file.read_text(encoding='utf-8').replace('{TARGET_DATABASE}', target_db)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return cols, rows
    finally:
        cur.close()


def status_of(row):
    return str(row.get('STATUS', '')).upper()


def print_table(cols, rows):
    """Print rows as an aligned text table."""
    widths = {c: len(c) for c in cols}
    for r in rows:
        for c in cols:
            widths[c] = max(widths[c], len(str(r.get(c, ''))))
    print('  ' + ' | '.join(c.ljust(widths[c]) for c in cols))
    print('  ' + '-+-'.join('-' * widths[c] for c in cols))
    for r in rows:
        print('  ' + ' | '.join(str(r.get(c, '')).ljust(widths[c]) for c in cols))


def main():
    parser = argparse.ArgumentParser(description='Run the OLIDS checks suite')
    parser.add_argument('--check', help='Run one check (name without .sql)')
    parser.add_argument('--verbose', action='store_true',
                        help='Show all rows, including PASS/INFO')
    parser.add_argument('--csv', metavar='DIR', help='Also write one CSV per check to DIR')
    args = parser.parse_args()

    env = load_env()
    target_db = env.get('SNOWFLAKE_TARGET_DATABASE')
    if not target_db:
        raise SystemExit('SNOWFLAKE_TARGET_DATABASE is not set')

    checks = discover(args.check)
    csv_dir = Path(args.csv) if args.csv else None
    if csv_dir:
        csv_dir.mkdir(parents=True, exist_ok=True)

    print('Connecting to Snowflake...')
    conn = get_connection(env)
    any_fail = False
    totals = {}
    try:
        for f in checks:
            print(f"\n=== {f.stem} ===")
            try:
                cols, rows = run_check(conn, f, target_db)
            except Exception as exc:  # a broken check must not stop the others
                print(f"  ERROR: {exc}")
                any_fail = True
                totals['ERROR'] = totals.get('ERROR', 0) + 1
                continue

            counts = {}
            for r in rows:
                s = status_of(r)
                counts[s] = counts.get(s, 0) + 1
                totals[s] = totals.get(s, 0) + 1
            if counts.get('FAIL'):
                any_fail = True

            if csv_dir and rows:
                out = csv_dir / f"{f.stem}.csv"
                with out.open('w', newline='', encoding='utf-8') as fh:
                    writer = csv.DictWriter(fh, fieldnames=cols)
                    writer.writeheader()
                    writer.writerows(rows)

            shown = rows if args.verbose else [r for r in rows if status_of(r) in ('WARN', 'FAIL')]
            if shown:
                print_table(cols, shown)
            elif rows:
                print('  (no WARN/FAIL rows; use --verbose to see all)')
            else:
                print('  (no rows)')

            summary = ', '.join(f"{k}={counts[k]}" for k in sorted(counts))
            print(f"  Summary: {summary or 'no rows'}")
    finally:
        conn.close()

    print('\n=== Totals ===')
    print('  ' + (', '.join(f"{k}={totals[k]}" for k in sorted(totals)) or 'no rows'))
    return 1 if any_fail else 0


if __name__ == '__main__':
    sys.exit(main())
