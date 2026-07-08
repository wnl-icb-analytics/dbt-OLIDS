#!/usr/bin/env python3
"""Report recent dbt query performance from Snowflake query history.

Shows per-query bytes scanned, local/remote disk spill, elapsed time and the
dbt node responsible, for the project's query tag on a given warehouse.
Remote spill on a model is the signal to move it to a larger warehouse.

Usage:
    python scripts/checks/report_run_queries.py                 # last 6 hours
    python scripts/checks/report_run_queries.py --hours 24
    python scripts/checks/report_run_queries.py --warehouse WH_WNL_OLIDS_XL
    python scripts/checks/report_run_queries.py --min-gb 1 --limit 40
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from snowflake_env import load_env, get_connection

NODE_RE = re.compile(r'"node_id"\s*:\s*"([^"]+)"')

# ACCOUNT_USAGE carries the spill columns but lags ~45 minutes; the
# information_schema fallback is current but has no spill visibility.
ACCOUNT_USAGE_SQL = """
select
    start_time,
    query_type,
    total_elapsed_time / 1000 as elapsed_s,
    bytes_scanned / power(1024, 3) as gb_scanned,
    bytes_spilled_to_local_storage / power(1024, 3) as gb_spill_local,
    bytes_spilled_to_remote_storage / power(1024, 3) as gb_spill_remote,
    rows_produced,
    warehouse_size,
    query_text
from snowflake.account_usage.query_history
where warehouse_name = %(warehouse)s
  and start_time >= dateadd('hour', -%(hours)s, current_timestamp())
  and query_tag like 'dbt_olids%%'
  and execution_status = 'SUCCESS'
  and query_type in ('SELECT', 'CREATE_TABLE_AS_SELECT', 'MERGE', 'INSERT')
order by total_elapsed_time desc
"""

INFO_SCHEMA_SQL = """
select
    start_time,
    query_type,
    total_elapsed_time / 1000 as elapsed_s,
    bytes_scanned / power(1024, 3) as gb_scanned,
    null as gb_spill_local,
    null as gb_spill_remote,
    rows_produced,
    warehouse_size,
    query_text
from table(information_schema.query_history_by_warehouse(
    warehouse_name => %(warehouse)s,
    end_time_range_start => dateadd('hour', -%(hours)s, current_timestamp()),
    result_limit => 10000
))
where query_tag like 'dbt_olids%%'
  and execution_status = 'SUCCESS'
  and query_type in ('SELECT', 'CREATE_TABLE_AS_SELECT', 'MERGE', 'INSERT')
order by total_elapsed_time desc
"""


def node_of(query_text):
    m = NODE_RE.search(query_text or '')
    if m:
        return m.group(1).split('.')[-1]
    return '(untagged)'


def fmt(v, dp=1):
    return '-' if v in (None, 0) else f"{v:,.{dp}f}"


def main():
    parser = argparse.ArgumentParser(description='dbt query performance report')
    parser.add_argument('--hours', type=int, default=6)
    parser.add_argument('--warehouse', help='defaults to SNOWFLAKE_WAREHOUSE from .env')
    parser.add_argument('--limit', type=int, default=25, help='rows to display')
    parser.add_argument('--min-gb', type=float, default=0.0, help='hide queries scanning less')
    args = parser.parse_args()

    env = load_env()
    warehouse = args.warehouse or env.get('SNOWFLAKE_WAREHOUSE')
    if not warehouse:
        raise SystemExit('No warehouse given and SNOWFLAKE_WAREHOUSE not set')

    conn = get_connection(env)
    lagged = True
    try:
        cur = conn.cursor()
        params = {'warehouse': warehouse, 'hours': args.hours}
        try:
            cur.execute(ACCOUNT_USAGE_SQL, params)
        except Exception:
            # no ACCOUNT_USAGE access: fall back, spill columns unavailable
            lagged = False
            cur.execute(INFO_SCHEMA_SQL, params)
        rows = cur.fetchall()
    finally:
        conn.close()
    if lagged:
        print('(source: ACCOUNT_USAGE — lags ~45 minutes; very recent queries may be missing)')

    records = []
    for start, qtype, elapsed, gb, spill_l, spill_r, produced, size, text in rows:
        if (gb or 0) < args.min_gb:
            continue
        records.append({
            'node': node_of(text)[:52],
            'type': qtype[:6],
            'size': size or '',
            'elapsed_s': elapsed or 0,
            'gb_scanned': gb or 0,
            'gb_spill_local': spill_l or 0,
            'gb_spill_remote': spill_r or 0,
            'rows': produced or 0,
        })

    shown = records[:args.limit]
    header = f"{'node':<52} {'type':<6} {'size':<8} {'elapsed_s':>9} {'gb_scan':>8} {'spill_l':>8} {'spill_r':>8} {'rows':>13}"
    print(f"\nWarehouse {warehouse}, last {args.hours}h, {len(records)} dbt queries (showing {len(shown)})\n")
    print(header)
    print('-' * len(header))
    for r in shown:
        print(f"{r['node']:<52} {r['type']:<6} {r['size']:<8} {r['elapsed_s']:>9,.0f} "
              f"{fmt(r['gb_scanned']):>8} {fmt(r['gb_spill_local']):>8} {fmt(r['gb_spill_remote']):>8} {r['rows']:>13,}")

    total_gb = sum(r['gb_scanned'] for r in records)
    spillers_r = [r for r in records if r['gb_spill_remote'] > 0]
    spillers_l = [r for r in records if r['gb_spill_local'] > 0]
    print(f"\nTotals: {total_gb:,.1f} GB scanned; {len(spillers_l)} queries spilled to local disk; "
          f"{len(spillers_r)} spilled to REMOTE disk")
    if spillers_r:
        print("Remote spillers (candidates for a larger warehouse):")
        for r in spillers_r[:10]:
            print(f"  {r['node']} — {r['gb_spill_remote']:,.1f} GB remote spill, {r['elapsed_s']:,.0f}s")
    return 0


if __name__ == '__main__':
    sys.exit(main())
