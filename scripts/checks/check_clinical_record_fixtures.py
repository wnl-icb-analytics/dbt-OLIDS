"""Check compiled clinical SQL with synthetic orders and statement links."""

import argparse
import re
from pathlib import Path


def fixture(name, defaults, rows):
    selects = []
    for row in rows:
        values = defaults | row
        selects.append('SELECT ' + ', '.join(f'{value} AS {key}' for key, value in values.items()))
    return name + ' AS (' + ' UNION ALL '.join(selects) + ')'


def check_sql(model):
    common = dict.fromkeys([
        'id', 'patient_id', 'encounter_id', 'date_precision_source_code',
        'date_precision_source_display', 'source_code', 'source_display',
        'source_system', 'mapped_concept_code', 'mapped_concept_display',
        'target_system', 'provider_organisation_id', 'publisher_organisation_id',
        'publisher_organisation_code', 'practitioner_id',
    ], "'synthetic'")
    common |= {
        'person_id': '1', 'clinical_effective_date': "'2000-01-01'::DATE",
        'date_recorded': "'2000-01-02'::TIMESTAMP_NTZ",
        'source_extraction_date': "'2026-01-01'::TIMESTAMP_NTZ", 'lds_is_deleted': 'FALSE',
    }
    orders = common | {
        'medication_statement_id': "'statement'", 'medication_name': "'Order medicine'",
        'dose': "'Order dose'", 'quantity_value': '28::FLOAT', 'quantity_unit': "'tablet'",
        'quantity_value_description': "'28 tablets'", 'duration_days': '14',
    }
    observations = common | {
        'result_value': '42::FLOAT', 'result_date': "'2000-01-01'::DATE",
        'result_unit_source_code': "'unit'", 'result_unit_source_display': "'Unit'",
        'allergy_medication_name': 'NULL::VARCHAR',
    }
    statements = common | {
        'authorisation_type_source_code': "'R'", 'authorisation_type_source_display': "'Repeat'",
        'medication_name': "'Different statement medicine'", 'dose': "'Current statement dose'",
        'quantity_value': '99::FLOAT', 'quantity_unit': "'different unit'", 'duration_days': '99',
        'clinical_effective_date': "'2020-01-01'::DATE", 'source_code': "'different_code'",
        'mapped_concept_code': "'different_mapped_code'",
    }
    fixtures = [
        fixture('fixture_observation', observations, [
            {'id': "'observation'"},
            {'id': "'context_missing'", 'encounter_id': "'absent'", 'clinical_effective_date': 'NULL::DATE'},
            {'id': "'context_wrong_person'", 'encounter_id': "'other_person'", 'clinical_effective_date': 'NULL::DATE'},
            {'id': "'context_deleted'", 'encounter_id': "'deleted'", 'clinical_effective_date': 'NULL::DATE'},
        ]),
        fixture('fixture_medication_order', orders, [
            {'id': "'matched'"}, {'id': "'repeat_issue'"},
            {'id': "'missing'", 'medication_statement_id': "'absent'"},
            {'id': "'wrong_person'", 'medication_statement_id': "'other_person'"},
            {'id': "'deleted_statement'", 'medication_statement_id': "'deleted'"},
            {'id': "'no_reference'", 'medication_statement_id': 'NULL::VARCHAR'},
            {'id': "'deleted_order'", 'lds_is_deleted': 'TRUE'},
        ]),
        fixture('fixture_medication_statement', statements, [
            {'id': "'statement'"}, {'id': "'unlinked'"},
            {'id': "'other_person'", 'person_id': '2'},
            {'id': "'deleted'", 'lds_is_deleted': 'TRUE'},
        ]),
        fixture('fixture_patient', {'id': "'synthetic'", 'person_id': '1', 'sk_patient_id': '123'}, [{}]),
        fixture('fixture_encounter', {
            'id': "'synthetic'", 'person_id': '1', 'clinical_effective_date': "'1999-12-30'::DATE",
            'date_precision_source_code': "'YM'", 'date_precision_source_display': "'Month'",
            'lds_is_deleted': 'FALSE',
        }, [{}, {'id': "'other_person'", 'person_id': '2'}, {'id': "'deleted'", 'lds_is_deleted': 'TRUE'}]),
        fixture('fixture_organisation', {
            'id': "'synthetic'", 'organisation_code': "'ORG'", 'assigning_authority_code': "'ODS'",
            'name': "'Synthetic organisation'", 'lds_is_deleted': 'FALSE',
        }, [{}]),
    ]
    for entity, expected_count in [('observation', 1), ('medication_order', 1),
                                   ('medication_statement', 1), ('patient', 1), ('organisation', 2),
                                   ('encounter', 1)]:
        model, count = re.subn(rf'\bOLIDS_ENGINEERING\.CONFORMED\.{entity}\b',
                               f'fixture_{entity}', model, flags=re.IGNORECASE)
        if count != expected_count:
            raise ValueError(f'Expected {expected_count} compiled references for {entity}, found {count}')
    return 'WITH ' + ',\n'.join(fixtures) + ', actual AS (' + model + ''')
SELECT COUNT(*) AS actual_rows, COUNT(DISTINCT clinical_record_id) AS distinct_ids,
    COUNT_IF(source_record_type='medication_statement') AS standalone_statements,
    COUNT_IF(source_record_type='medication_order' AND (
        NOT EQUAL_NULL(source_code,'synthetic') OR NOT EQUAL_NULL(mapped_code,'synthetic')
        OR NOT EQUAL_NULL(clinical_record_date,'2000-01-01'::DATE) OR NOT EQUAL_NULL(medication_name,'Order medicine')
        OR NOT EQUAL_NULL(medication_dose,'Order dose') OR NOT EQUAL_NULL(medication_quantity_value,28)
        OR NOT EQUAL_NULL(medication_quantity_unit,'tablet')
        OR NOT EQUAL_NULL(medication_duration_days,14))) AS changed_order_details,
    COUNT_IF(NOT EQUAL_NULL(medication_authorisation_type_code,
        IFF(source_record_id IN ('matched','repeat_issue'),'R',NULL))
        OR NOT EQUAL_NULL(medication_authorisation_type_name,
        IFF(source_record_id IN ('matched','repeat_issue'),'Repeat',NULL))) AS incorrect_enrichment,
    COUNT_IF(encounter_id='synthetic' AND (
        is_encounter_person_consistent IS DISTINCT FROM TRUE
        OR encounter_date IS DISTINCT FROM '1999-12-30'::DATE
        OR encounter_date_precision_code IS DISTINCT FROM 'YM')) AS missing_valid_context,
    COUNT_IF(encounter_id<>'synthetic' AND (
        encounter_date IS NOT NULL OR encounter_date_precision_code IS NOT NULL
        OR encounter_date_precision_name IS NOT NULL)) AS invalid_context_promoted,
    COUNT_IF(source_record_id LIKE 'context_%' AND clinical_record_date IS NOT NULL) AS fabricated_clinical_dates,
    COUNT_IF(source_record_id='context_wrong_person' AND is_encounter_person_consistent IS DISTINCT FROM FALSE)
        AS missed_person_conflicts,
    COUNT_IF(clinical_record_id != UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8',
        'olids:clinical_record:' || source_record_type || ':' || source_record_id)::UUID) AS changed_ids
FROM actual;
'''


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    parser.add_argument('output_sql', type=Path)
    args = parser.parse_args()
    args.output_sql.write_text(check_sql(args.compiled_sql.read_text().strip().rstrip(';')))
