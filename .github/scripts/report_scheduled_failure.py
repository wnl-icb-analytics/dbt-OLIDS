#!/usr/bin/env python3
"""Create one GitHub issue for scheduled failures and close it on recovery."""

import json
import os
import subprocess
from pathlib import Path

TITLE = '[dbt-olids] Scheduled build failing'


def gh(*args, check=True):
    """Run GitHub CLI and return stdout."""
    result = subprocess.run(
        ['gh', *args], check=check, capture_output=True, text=True
    )
    return result.stdout.strip()


def failed_nodes():
    """Return failed dbt node names from run_results.json."""
    path = Path('target/run_results.json')
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding='utf-8'))
    return [
        result.get('unique_id', 'unknown')
        for result in data.get('results', [])
        if result.get('status') in {'error', 'fail'}
    ]


def find_issue(label):
    """Return the open scheduled-failure issue number, if any."""
    raw = gh(
        'issue', 'list', '--state', 'open', '--label', label,
        '--json', 'number,title', '--limit', '100'
    )
    issues = json.loads(raw or '[]')
    match = next((issue for issue in issues if issue['title'] == TITLE), None)
    return str(match['number']) if match else None


def main():
    outcome = os.environ['JOB_OUTCOME']
    run_url = os.environ['RUN_URL']
    label = os.environ.get('ISSUE_LABEL', 'dbt-scheduled-run-failure')

    gh(
        'label', 'create', label, '--color', 'B60205',
        '--description', 'Automated dbt schedule failure', '--force'
    )
    issue = find_issue(label)

    if outcome == 'success':
        if issue:
            gh(
                'issue', 'close', issue, '--comment',
                f'Recovered in [this workflow run]({run_url}).'
            )
        return

    nodes = failed_nodes()
    node_text = '\n'.join(f'- `{node}`' for node in nodes[:20])
    body = f'Scheduled dbt-olids failed: [workflow run]({run_url}).'
    if node_text:
        body += f'\n\nFailed nodes:\n{node_text}'
    if issue:
        gh('issue', 'comment', issue, '--body', body)
    else:
        gh(
            'issue', 'create', '--title', TITLE, '--body', body,
            '--label', label
        )


if __name__ == '__main__':
    main()
