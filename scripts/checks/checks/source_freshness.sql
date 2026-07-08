-- source_freshness
-- Recency of each landing table that carries source_extraction_date and
-- lds_transform_datetime. Runtime check, not a dbt model, so CURRENT_DATE is used.
-- status keys off source_extraction_date: PASS <= 2 days, WARN <= 7 days, else FAIL.

WITH freshness AS (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ALLERGY_INTOLERANCE"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'APPOINTMENT' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."APPOINTMENT"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."APPOINTMENT_PRACTITIONER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'DIAGNOSTIC_ORDER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."DIAGNOSTIC_ORDER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'ENCOUNTER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ENCOUNTER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'EPISODE_OF_CARE' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."EPISODE_OF_CARE"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'MEDICATION_ORDER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."MEDICATION_ORDER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'MEDICATION_STATEMENT' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."MEDICATION_STATEMENT"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'OBSERVATION' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."OBSERVATION"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'ORGANISATION' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ORGANISATION"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PATIENT' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PATIENT_ADDRESS' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_ADDRESS"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PATIENT_CONTACT' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_CONTACT"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PRACTITIONER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PRACTITIONER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PRACTITIONER_IN_ROLE' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PRACTITIONER_IN_ROLE"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PROCEDURE_REQUEST' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PROCEDURE_REQUEST"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'REFERRAL_REQUEST' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."REFERRAL_REQUEST"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'SCHEDULE' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."SCHEDULE"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'SCHEDULE_PRACTITIONER' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."SCHEDULE_PRACTITIONER"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'LOCATION' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."LOCATION"
    WHERE lds_is_deleted = FALSE

    UNION ALL

    SELECT 'PERSON' AS table_name,
        MAX(lds_transform_datetime) AS max_transform,
        MAX(source_extraction_date) AS max_extraction
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PERSON"
    WHERE lds_is_deleted = FALSE
)

SELECT
    'source_freshness' AS check_name,
    table_name,
    'source extraction recency' AS test_subject,
    CASE
        WHEN DATEDIFF('day', max_extraction, CURRENT_DATE) <= 2 THEN 'PASS'
        WHEN DATEDIFF('day', max_extraction, CURRENT_DATE) <= 7 THEN 'WARN'
        ELSE 'FAIL'
    END AS status,
    DATEDIFF('day', max_extraction, CURRENT_DATE) AS metric_value,
    '<= 2 days' AS threshold,
    max_transform,
    max_extraction,
    DATEDIFF('day', max_transform, CURRENT_DATE) AS days_since_transform,
    DATEDIFF('day', max_extraction, CURRENT_DATE) AS days_since_extraction
FROM freshness
ORDER BY days_since_extraction DESC, table_name;
