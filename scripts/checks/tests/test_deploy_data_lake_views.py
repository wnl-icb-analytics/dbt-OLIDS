"""Tests for data lake publication checks."""

import sys
import unittest
from pathlib import Path


CHECKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CHECKS_DIR))

from deploy_data_lake_views import call_procedure  # noqa: E402


class FakeCursor:
    def __init__(self, response):
        self.response = response
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return (self.response,)


class CallProcedureTests(unittest.TestCase):
    def test_returns_valid_response(self):
        cur = FakeCursor(
            '{"error_flag": false, "updated_view_count": 2}'
        )

        response = call_procedure(cur, 'CALL example()', 'Example')

        self.assertEqual(response['updated_view_count'], 2)

    def test_raises_on_error_response(self):
        cur = FakeCursor('{"error_flag": true, "message": "failed"}')

        with self.assertRaisesRegex(RuntimeError, 'failed'):
            call_procedure(cur, 'CALL example()', 'Example')


if __name__ == '__main__':
    unittest.main()
