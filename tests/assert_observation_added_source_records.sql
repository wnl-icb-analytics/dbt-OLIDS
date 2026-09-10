-- Added records retain their source identity, clinical code, date and supplied flags.
WITH expected AS (
    SELECT
        'allergy_intolerance' AS source_entity,
        id AS source_record_id,
        lds_source_record_id,
        patient_id,
        person_id,
        clinical_effective_date,
        clinical_effective_date_precision_source_concept_id,
        allergy_intolerance_source_concept_id AS observation_source_concept_id,
        mapped_concept_code,
        date_recorded,
        is_review,
        is_confidential,
        lds_is_deleted,
        medication_name AS allergy_medication_name
    FROM {{ ref('conformed_allergy_intolerance') }}

    UNION ALL

    SELECT
        'referral_request',
        id,
        lds_source_record_id,
        patient_id,
        person_id,
        clinical_effective_date,
        clinical_effective_date_precision_source_concept_id,
        referral_request_source_concept_id,
        mapped_concept_code,
        recorded_datetime,
        is_review,
        NULL::BOOLEAN,
        lds_is_deleted,
        NULL::VARCHAR
    -- Observations retain all original content, including non-referral codes.
    FROM {{ ref('conformed_referral_request_source') }}
),

actual AS (
    SELECT *
    FROM {{ ref('conformed_observation') }}
    WHERE source_entity IN ('allergy_intolerance', 'referral_request')
)

SELECT
    COALESCE(expected.source_entity, actual.source_entity) AS source_entity,
    COALESCE(expected.source_record_id, actual.source_record_id) AS source_record_id
FROM expected
FULL OUTER JOIN actual
    ON expected.source_entity = actual.source_entity
    AND expected.source_record_id = actual.source_record_id
WHERE expected.source_record_id IS NULL
    OR actual.source_record_id IS NULL
    OR actual.id != UUID_STRING(
        '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
        'olids:' || expected.source_entity || ':' || expected.source_record_id::VARCHAR
    )::UUID
    OR NOT EQUAL_NULL(expected.lds_source_record_id, actual.lds_source_record_id)
    OR NOT EQUAL_NULL(expected.patient_id, actual.patient_id)
    OR NOT EQUAL_NULL(expected.person_id, actual.person_id)
    OR NOT EQUAL_NULL(expected.clinical_effective_date, actual.clinical_effective_date)
    OR NOT EQUAL_NULL(
        expected.clinical_effective_date_precision_source_concept_id,
        actual.clinical_effective_date_precision_source_concept_id
    )
    OR NOT EQUAL_NULL(expected.observation_source_concept_id, actual.observation_source_concept_id)
    OR NOT EQUAL_NULL(expected.mapped_concept_code, actual.mapped_concept_code)
    OR NOT EQUAL_NULL(expected.date_recorded, actual.date_recorded)
    OR NOT EQUAL_NULL(expected.is_review, actual.is_review)
    OR NOT EQUAL_NULL(expected.is_confidential, actual.is_confidential)
    OR NOT EQUAL_NULL(expected.lds_is_deleted, actual.lds_is_deleted)
    OR NOT EQUAL_NULL(expected.allergy_medication_name, actual.allergy_medication_name)
    OR actual.result_value IS NOT NULL
    OR actual.result_date IS NOT NULL
    OR actual.result_text IS NOT NULL
    OR actual.result_value_units_source_concept_id IS NOT NULL
    OR actual.is_problem IS NOT NULL
    OR actual.is_problem_deleted IS NOT NULL

UNION ALL

SELECT source_entity, source_record_id
FROM {{ ref('conformed_observation') }}
WHERE source_entity = 'observation' AND NOT EQUAL_NULL(id, source_record_id)
