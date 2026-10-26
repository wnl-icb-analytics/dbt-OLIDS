"""Generate an aggregate check of compiled result-header SQL using synthetic UUIDs."""

import argparse
import re
from pathlib import Path


def check_sql(model):
    defaults = {
        'patient_id': "'synthetic-patient'",
        'clinical_effective_date': "'2026-01-01'::TIMESTAMP_NTZ",
        'date_recorded': "'2026-01-02'::TIMESTAMP_NTZ",
        'mapped_concept_code': "'synthetic-mapped'",
        'source_code': "'synthetic-source'",
        'source_system': "'synthetic-system'",
        'source_entity': "'observation'",
        'lds_is_deleted': 'FALSE',
        'is_problem': 'FALSE',
        'result_value': 'NULL::FLOAT',
        'result_text': 'NULL::VARCHAR',
    }
    pairs = [
        ({}, {}),
        ({'result_text': "'real text'"}, {}),
        ({'result_value': '12'}, {}),
        ({}, {'lds_is_deleted': 'TRUE'}),
        ({}, {'mapped_concept_code': "'other'", 'source_code': "'other'"}),
        ({}, {'patient_id': "'other-patient'"}),
        ({}, {'clinical_effective_date': "'2026-01-02'::TIMESTAMP_NTZ"}),
        ({}, {'date_recorded': "'2026-02-01'::TIMESTAMP_NTZ"}),
        ({'mapped_concept_code': 'NULL::VARCHAR'}, {'mapped_concept_code': 'NULL::VARCHAR'}),
        ({}, {'source_code': "'other'"}),
        ({'clinical_effective_date': 'NULL::TIMESTAMP_NTZ'}, {}),
        ({'mapped_concept_code': 'NULL::VARCHAR', 'source_code': 'NULL::VARCHAR'},
         {'mapped_concept_code': 'NULL::VARCHAR', 'source_code': 'NULL::VARCHAR'}),
        ({}, {}),
        ({'lds_is_deleted': 'TRUE'}, {}),
        ({'source_entity': "'referral_request'"}, {}),
        ({}, {'result_value': 'NULL::FLOAT', 'result_text': "'text only'"}),
        ({'lds_is_deleted': 'NULL::BOOLEAN'}, {'lds_is_deleted': 'NULL::BOOLEAN'}),
        ({'mapped_concept_code': 'NULL::VARCHAR'},
         {'mapped_concept_code': 'NULL::VARCHAR', 'source_system': "'other-system'"}),
        ({'patient_id': 'NULL::VARCHAR'}, {'patient_id': 'NULL::VARCHAR'}),
        ({}, {'clinical_effective_date': "'2026-01-01 12:00'::TIMESTAMP_NTZ"}),
    ]

    def uuid(number):
        return f"'00000000-0000-0000-0000-{number:012d}'::UUID"

    rows = []
    for i, (parent, child) in enumerate(pairs):
        parent_id = 2 * i + 1
        rows.extend([
            defaults | {'id': uuid(parent_id), 'parent_observation_id': 'NULL::UUID'} | parent,
            defaults | {'id': uuid(parent_id + 1), 'parent_observation_id': uuid(parent_id),
                        'result_value': '42::FLOAT'} | child,
        ])
    rows.append(defaults | {'id': uuid(41), 'parent_observation_id': uuid(25), 'result_value': '43'})
    rows.append(defaults | {'id': uuid(42), 'parent_observation_id': 'NULL::UUID'})
    rows.extend([
        defaults | {'id': uuid(43), 'parent_observation_id': 'NULL::UUID', 'is_problem': 'TRUE'},
        defaults | {'id': uuid(44), 'parent_observation_id': uuid(43), 'result_value': '42::FLOAT'},
    ])
    fixture = '\nUNION ALL\n'.join(
        'SELECT ' + ', '.join(f'{value} AS {key}' for key, value in row.items())
        for row in rows
    )
    model, replacements = re.subn(
        r'(?i)(?:"?OLIDS_ENGINEERING"?\.)?"?CONFORMED"?\."?OBSERVATION"?',
        'fixture', model.strip().rstrip(';'),
    )
    if replacements != 2:
        raise ValueError(f'Expected two conformed observation references, found {replacements}')
    expected_ids = [1, 15, 17, 19, 25, 33, 39, 43]
    expected = ' UNION ALL '.join(f'SELECT {uuid(i)} AS id' for i in expected_ids)
    return f'''WITH fixture AS ({fixture}), actual AS ({model}), expected AS ({expected})
SELECT (SELECT COUNT(*) FROM fixture) AS fixture_rows,
    (SELECT COUNT(*) FROM actual) AS actual_headers,
    (SELECT COUNT(*) FROM actual) - (SELECT COUNT(DISTINCT id) FROM actual) AS duplicate_ids,
    (SELECT COUNT(*) FROM (SELECT id FROM expected MINUS SELECT id FROM actual)) AS missing_headers,
    (SELECT COUNT(*) FROM (SELECT id FROM actual MINUS SELECT id FROM expected)) AS unexpected_headers,
    (SELECT COUNT_IF(is_result_header IS DISTINCT FROM TRUE) FROM actual) AS incorrect_flags;
'''


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    parser.add_argument('output_sql', type=Path)
    args = parser.parse_args()
    args.output_sql.write_text(check_sql(args.compiled_sql.read_text()), encoding='utf8')
