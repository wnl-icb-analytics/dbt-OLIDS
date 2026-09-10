"""Emit aggregate completeness SQL for a compiled OLIDS output and its model YAML."""

import argparse
import re
from pathlib import Path

import yaml


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    parser.add_argument('model_yaml', type=Path)
    parser.add_argument('--group-by', required=True)
    parser.add_argument('--distinct-key', help='Optional full distinct count; expensive for the clinical snapshot.')
    args = parser.parse_args()
    columns = [c['name'] for c in yaml.safe_load(args.model_yaml.read_text())['models'][0]['columns']]
    if any(not re.fullmatch(r'[a-z][a-z0-9_]*', c) for c in columns):
        raise ValueError('Expected unquoted snake_case column names')
    if args.group_by not in columns or (args.distinct_key and args.distinct_key not in columns):
        raise ValueError('Grouping and distinct key must be documented model columns')
    metrics = ['COUNT(*) AS row_count']
    if args.distinct_key:
        metrics.append(f'COUNT(DISTINCT {args.distinct_key}) AS distinct_ids')
    metrics.extend(f'COUNT({c}) AS {c}_populated' for c in columns if c != args.group_by)
    model = args.compiled_sql.read_text().strip().rstrip(';')
    print('ALTER SESSION SET USE_CACHED_RESULT=FALSE;')
    print(f'WITH candidate AS ({model})\nSELECT {args.group_by},')
    print(',\n'.join(metrics))
    print(f'FROM candidate GROUP BY {args.group_by};')
