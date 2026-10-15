"""Tests for schema failures and non-blocking undeclared table issues."""

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


CHECKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CHECKS_DIR))

import compare_sources_to_information_schema as check  # noqa: E402

reporter_spec = importlib.util.spec_from_file_location(
    'report_undeclared_source_tables',
    CHECKS_DIR.parents[1] / '.github/scripts/report_undeclared_source_tables.py',
)
reporter = importlib.util.module_from_spec(reporter_spec)
reporter_spec.loader.exec_module(reporter)


class SourceSchemaTests(unittest.TestCase):
    def run_check(self, live):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / 'target/report.json'
            conn = Mock()
            with (
                patch.object(check, 'parse_sources', return_value=(
                    'SOURCE_DB', 'SOURCE_SCHEMA', {'PATIENT': {'ID': 'TEXT'}},
                )),
                patch.object(check, 'load_env', return_value={}),
                patch.object(check, 'get_connection', return_value=conn),
                patch.object(check, 'fetch_live', return_value=live),
                patch.object(sys, 'argv', ['check', '--report-json', str(report_path)]),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                code = check.main()
            conn.close.assert_called_once()
            return code, json.loads(report_path.read_text(encoding='utf-8'))

    def test_matching_contract_passes_without_issues(self):
        code, report = self.run_check({'PATIENT': {'ID': 'TEXT'}})
        self.assertEqual(code, 0)
        self.assertEqual(report['undeclared_tables'], [])

    def test_undeclared_tables_pass_and_are_reported(self):
        code, report = self.run_check({
            'PATIENT': {'ID': 'TEXT'}, 'RETIRED_V2': {}, 'NEW_TABLE': {},
        })
        self.assertEqual(code, 0)
        self.assertEqual(report, {
            'source': 'olids_pseudo', 'database': 'SOURCE_DB',
            'schema': 'SOURCE_SCHEMA',
            'undeclared_tables': ['NEW_TABLE', 'RETIRED_V2'],
        })

    def test_missing_declared_table_still_fails_and_reports_extras(self):
        code, report = self.run_check({'NEW_TABLE': {}})
        self.assertEqual(code, 1)
        self.assertEqual(report['undeclared_tables'], ['NEW_TABLE'])

    def test_column_drift_still_fails(self):
        for columns in ({}, {'ID': 'NUMBER'}, {'ID': 'TEXT', 'NEW_COLUMN': 'TEXT'}):
            with self.subTest(columns=columns):
                code, _ = self.run_check({'PATIENT': columns})
                self.assertEqual(code, 1)


class UndeclaredTableIssueTests(unittest.TestCase):
    def report(self, tables):
        return {
            'source': 'olids_pseudo', 'database': 'SOURCE_DB',
            'schema': 'SOURCE_SCHEMA', 'undeclared_tables': tables,
        }

    def test_no_undeclared_tables_does_not_call_github(self):
        with patch.object(reporter, 'gh') as gh:
            reporter.report_tables(self.report([]), 'https://example.test/run')
        gh.assert_not_called()

    def test_only_new_tables_create_issues(self):
        existing = '[dbt-olids] Undeclared source table: olids_pseudo.EXISTING'
        with patch.object(reporter, 'gh', side_effect=[
            '', json.dumps([{'title': existing}]), 'https://example.test/issue',
        ]) as gh:
            reporter.report_tables(
                self.report(['EXISTING', 'NEW']), 'https://example.test/run',
            )
        create = gh.call_args_list[-1].args
        self.assertEqual(create[:2], ('issue', 'create'))
        self.assertIn('olids_pseudo.NEW', create[3])
        self.assertIn('SOURCE_DB.SOURCE_SCHEMA.NEW', create[5])
        self.assertIn('https://example.test/run', create[5])
        self.assertEqual(gh.call_count, 3)

    def test_second_detection_reuses_the_open_issue(self):
        issues = []

        def fake_gh(*args):
            if args[:2] == ('issue', 'list'):
                self.assertEqual(args[args.index('--state') + 1], 'open')
                return json.dumps(issues)
            if args[:2] == ('issue', 'create'):
                issues.append({'title': args[args.index('--title') + 1]})
            return ''

        with patch.object(reporter, 'gh', side_effect=fake_gh):
            reporter.report_tables(self.report(['NEW']), 'https://example.test/run/1')
            reporter.report_tables(self.report(['NEW']), 'https://example.test/run/2')
        self.assertEqual(len(issues), 1)

    def test_github_calls_target_the_workflow_repository(self):
        with (
            patch.dict(os.environ, {'GITHUB_REPOSITORY': 'wnl-icb-analytics/dbt-OLIDS'}),
            patch.object(reporter.subprocess, 'run', return_value=Mock(stdout='[]')) as run,
        ):
            reporter.gh('issue', 'list')
        self.assertEqual(run.call_args.args[0], [
            'gh', 'issue', 'list', '--repo', 'wnl-icb-analytics/dbt-OLIDS',
        ])
        self.assertTrue(run.call_args.kwargs['check'])

    def test_github_errors_are_visible_to_the_workflow(self):
        with patch.object(reporter, 'gh', side_effect=subprocess.CalledProcessError(1, 'gh')):
            with self.assertRaises(subprocess.CalledProcessError):
                reporter.report_tables(self.report(['NEW']), 'https://example.test/run')


if __name__ == '__main__':
    unittest.main()
