#!/usr/bin/env python3
"""Open one issue per undeclared source table without repeat notifications."""

import json
import os
import subprocess
import sys
from pathlib import Path

LABEL = 'source-schema-drift'


def gh(*args):
    """Run GitHub CLI against the workflow repository."""
    result = subprocess.run(
        ['gh', *args, '--repo', os.environ['GITHUB_REPOSITORY']],
        check=True, capture_output=True, text=True,
    )
    return result.stdout.strip()


def report_tables(report, run_url):
    """Create an issue unless that table already has an open issue."""
    tables = report['undeclared_tables']
    if not tables:
        return

    gh(
        'label', 'create', LABEL, '--color', 'FBCA04',
        '--description', 'Upstream source tables absent from the dbt contract',
        '--force',
    )
    raw = gh(
        'issue', 'list', '--state', 'open', '--label', LABEL,
        '--json', 'title', '--limit', '1000',
    )
    open_titles = {issue['title'] for issue in json.loads(raw or '[]')}
    for table in tables:
        title = f"[dbt-olids] Undeclared source table: {report['source']}.{table}"
        if title in open_titles:
            print(f'Existing open issue: {title}')
            continue
        relation = f"{report['database']}.{report['schema']}.{table}"
        body = (
            f'`{relation}` exists upstream but is not declared under '
            f"`{report['source']}` in `models/sources.yml`.\n\n"
            f'Detected in [this workflow run]({run_url}). '
            'The undeclared table does not block the build.\n\n'
            'Check whether to add the table to the source contract or leave it '
            'undeclared, for example during an upstream retirement. '
            'Keep this issue open while the table remains undeclared '
            'to avoid repeat issues.'
        )
        gh('issue', 'create', '--title', title, '--body', body, '--label', LABEL)
        open_titles.add(title)


def main():
    report = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
    report_tables(report, os.environ['RUN_URL'])


if __name__ == '__main__':
    main()
