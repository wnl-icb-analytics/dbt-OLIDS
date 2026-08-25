"""Tests for audit history recording."""

import sys
import unittest
from pathlib import Path


CHECKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CHECKS_DIR))

from audit_history import (  # noqa: E402
    RunContext,
    build_run_context,
    insert_attempt_history,
)


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.executed_many = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def executemany(self, query, params):
        self.executed_many.append((query, params))


class AuditHistoryTests(unittest.TestCase):
    def test_build_run_context_uses_github_identity(self):
        context = build_run_context(
            {
                'GITHUB_RUN_ID': '123',
                'GITHUB_RUN_ATTEMPT': '2',
                'GITHUB_SHA': 'abc',
                'GITHUB_EVENT_NAME': 'workflow_dispatch',
            },
            dbt_build_outcome='failure',
            publish_outcome='skipped',
            verify_outcome='skipped',
            pipeline_status='failed',
        )

        self.assertEqual(context.run_id, '123')
        self.assertEqual(context.run_attempt, 2)
        self.assertEqual(context.pipeline_status, 'failed')

    def test_build_run_context_rejects_invalid_attempt(self):
        with self.assertRaisesRegex(ValueError, 'must be an integer'):
            build_run_context(
                {'GITHUB_RUN_ATTEMPT': 'x'},
                dbt_build_outcome='success',
                publish_outcome='success',
                verify_outcome='success',
                pipeline_status='success',
            )

    def test_insert_attempt_history_is_idempotent_per_attempt(self):
        cur = FakeCursor()
        context = RunContext(
            run_id='123',
            run_attempt=1,
            git_sha='abc',
            event_name='schedule',
            dbt_build_outcome='success',
            publish_outcome='success',
            verify_outcome='success',
            pipeline_status='success',
        )

        insert_attempt_history(
            cur,
            'OLIDS_ENGINEERING',
            context,
            {'PERSON': 'source', 'PATIENT': 'source'},
            {'PERSON': 'landing', 'PATIENT': 'landing'},
            ['AUDIT_PATIENT_LINKAGE'],
        )

        delete_queries = [query for query, _ in cur.executed[:3]]
        self.assertTrue(
            all(query.startswith('DELETE FROM') for query in delete_queries)
        )
        self.assertEqual(len(cur.executed_many[0][1]), 2)
        self.assertIn('AUDIT_PATIENT_LINKAGE', cur.executed[-1][1])


if __name__ == '__main__':
    unittest.main()
