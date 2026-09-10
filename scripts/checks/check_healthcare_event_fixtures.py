"""Emit an aggregate-only check of compiled event SQL against synthetic records."""

import argparse
import re
from pathlib import Path


def check_sql(model, referral_model):
    fixtures = """
fixture_appointment AS (
    SELECT column1 AS id, 'practice' AS patient_id, 1 AS person_id,
        column2::TIMESTAMP_NTZ AS datetime_booked, '2026-09-10 09:00'::TIMESTAMP_NTZ AS start_date,
        column3 AS lds_is_deleted, 'Booked' AS appointment_status_source_display,
        'B' AS appointment_status_source_code, '1' AS appointment_status_code,
        'Booked' AS appointment_status_display, 'org' AS provider_organisation_id,
        'org' AS publisher_organisation_id, 'PRACTICE' AS publisher_organisation_code,
        '2026-09-09'::TIMESTAMP_NTZ AS source_extraction_date
    FROM VALUES ('00000000-0000-0000-0000-000000000011','2026-09-08',FALSE), ('00000000-0000-0000-0000-000000000012',NULL,FALSE), ('00000000-0000-0000-0000-000000000013','2026-09-08',TRUE)
),
fixture_booking AS (
    SELECT *, id AS appointment_id FROM fixture_appointment
    WHERE datetime_booked IS NOT NULL AND NOT lds_is_deleted
),
fixture_observation AS (
    SELECT column1 AS id, column2 AS source_code, column3 AS source_system,
        column4 AS mapped_concept_code, column5 AS target_system,
        column6::DATE AS clinical_effective_date, column7 AS date_precision_source_code,
        column8 AS lds_is_deleted, 'practice' AS patient_id, 1 AS person_id,
        'org' AS provider_organisation_id, 'org' AS publisher_organisation_id,
        'PRACTICE' AS publisher_organisation_code, '2026-09-09'::TIMESTAMP_NTZ AS source_extraction_date
    FROM VALUES
        ('direct','100','snomed_info_sct',NULL,NULL,'2026-09-01','YMD',FALSE),
        ('mapped','local','EMIS_CodeID_cs','100','snomed_info_sct','2026-09-01','YM',FALSE),
        ('both','100','snomed_info_sct','100','snomed_info_sct','2026-01-01','Y',FALSE),
        ('historical','99','http:__snomed.info_sct',NULL,NULL,NULL,'Unknown',FALSE),
        ('wrong_system','100','EMIS_CodeID_cs','100','other_system','2026-09-01','YMD',FALSE),
        ('not_referral','200','snomed_info_sct',NULL,NULL,'2026-09-01','YMD',FALSE),
        ('deleted','100','snomed_info_sct',NULL,NULL,'2026-09-01','YMD',TRUE)
),
fixture_codes AS (
    SELECT column1 AS code, column2 AS code_name
    FROM VALUES ('100','Synthetic current referral'), ('99','Synthetic historical referral')
),
fixture_patient AS (SELECT 'practice' AS id, 1 AS person_id, 123 AS sk_patient_id),
fixture_organisation AS (
    SELECT 'org' AS id, 'PRACTICE' AS organisation_code, 'ODS' AS assigning_authority_code,
        'Synthetic practice' AS name, FALSE AS lds_is_deleted
)
"""
    # Exercise the actual canonical referral SQL, including original-ID retention.
    observation_fields = set(re.findall(r'\bo\.([a-z_]+)', referral_model))
    supplied = {'id', 'source_code', 'source_system', 'mapped_concept_code', 'target_system',
                'clinical_effective_date', 'date_precision_source_code', 'lds_is_deleted',
                'patient_id', 'person_id', 'provider_organisation_id', 'publisher_organisation_id',
                'publisher_organisation_code', 'source_extraction_date'}
    expressions = {
        'source_entity': "IFF(column1 = 'direct', 'referral_request', 'observation')",
        # This collision must not attach original referral detail to the native observation.
        'source_record_id': "IFF(column1 IN ('direct', 'mapped'), '00000000-0000-0000-0000-000000000001', column1)",
        'date_recorded': "'2026-09-09'::TIMESTAMP_NTZ",
        'date_precision_source_display': "'Synthetic precision'",
    }
    additions = [expressions.get(c, 'NULL::VARCHAR') + ' AS ' + c
                 for c in sorted(observation_fields - supplied)]
    start = fixtures.index('fixture_observation AS (')
    end = fixtures.index('    FROM VALUES', start)
    fixtures = fixtures[:end].rstrip() + ',\n        ' + ',\n        '.join(additions) + '\n' + fixtures[end:]
    original_fields = sorted(set(re.findall(r'\br\.([a-z_]+)', referral_model)))
    original_values = {'id': "'00000000-0000-0000-0000-000000000001'", 'person_id': '1',
                       'unique_booking_reference_number': "'synthetic-ubrn'"}
    fixtures += ', fixture_original_referral AS (SELECT ' + ', '.join(
        original_values.get(c, 'NULL::VARCHAR') + ' AS ' + c for c in original_fields) + ')'
    for relation, name in {
        'OLIDS_ENGINEERING.CONFORMED.observation': 'fixture_observation',
        'OLIDS_ENGINEERING.CONFORMED.referral_request_source': 'fixture_original_referral',
        'REFERENCE.TERMINOLOGY.patient_referral_snomed_codes': 'fixture_codes',
    }.items():
        referral_model, count = re.subn(re.escape(relation) + r'\b', name, referral_model, flags=re.IGNORECASE)
        if not count:
            raise ValueError(f'Missing referral reference: {relation}')
    fixtures += ', fixture_referral AS (' + referral_model + ')'
    replacements = {
        'OLIDS_ENGINEERING.CONFORMED.appointment_booking': 'fixture_booking',
        'OLIDS_ENGINEERING.CONFORMED.appointment': 'fixture_appointment',
        'OLIDS_ENGINEERING.CONFORMED.referral_request': 'fixture_referral',
        'OLIDS_ENGINEERING.CONFORMED.patient': 'fixture_patient',
        'OLIDS_ENGINEERING.CONFORMED.organisation': 'fixture_organisation',
    }
    for relation, fixture in replacements.items():
        model, count = re.subn(re.escape(relation) + r'\b', fixture, model, flags=re.IGNORECASE)
        if count == 0:
            raise ValueError(f'Missing compiled reference: {relation}')
    return f"""WITH {fixtures}, candidate AS ({model})
SELECT COUNT(*) AS actual_rows,
    ABS(COUNT(*) - 7)
    + ABS(COUNT(DISTINCT healthcare_event_id) - 7)
    + ABS(COUNT_IF(event_type = 'appointment_booking') - 1)
    + ABS(COUNT_IF(event_type = 'appointment_slot') - 2)
    + ABS(COUNT_IF(event_type = 'patient_referral') - 4)
    + ABS(COUNT_IF(event_type = 'patient_referral' AND event_time_precision = 'year') - 1)
    + ABS(COUNT_IF(event_type = 'patient_referral' AND event_time_precision = 'month') - 1)
    + ABS(COUNT_IF(event_code = '99' AND event_code_name = 'Synthetic historical referral'
        AND event_date IS NULL AND event_time_precision = 'unknown') - 1)
    + COUNT_IF(event_type = 'patient_referral' AND event_at IS NOT NULL)
    + ABS((SELECT COUNT(*) FROM fixture_referral WHERE id = '00000000-0000-0000-0000-000000000001'
        AND observation_id = 'direct' AND unique_booking_reference_number = 'synthetic-ubrn') - 1)
    + (SELECT COUNT(*) FROM fixture_referral WHERE observation_id <> 'direct'
        AND unique_booking_reference_number IS NOT NULL)
    + (SELECT COUNT(*) FROM fixture_referral WHERE observation_id IN ('wrong_system', 'not_referral', 'deleted'))
    AS failures
FROM candidate;
"""


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('compiled_sql', type=Path)
    parser.add_argument('compiled_referral_sql', type=Path)
    args = parser.parse_args()
    print(check_sql(args.compiled_sql.read_text().strip().rstrip(';'),
                    args.compiled_referral_sql.read_text().strip().rstrip(';')))
