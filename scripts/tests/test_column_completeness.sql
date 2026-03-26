/*
    Test: Column Completeness
    
    Checks NULL rates for critical fields across OLIDS tables.
    
    Configuration:
    - NULL_THRESHOLD_PCT: Maximum allowed NULL percentage (default: 1.0)
    
    Returns standardised test results with PASS/FAIL status.
*/

-- ============================================
-- CONFIGURATION - Edit threshold here
-- ============================================
SET null_threshold_pct = 1.0;  -- Default 1% - columns with > 1% NULLs will FAIL

-- ============================================
-- TEST EXECUTION
-- ============================================
WITH patient_checks AS (
    SELECT
        'PATIENT' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
        UNION ALL
        SELECT 'sk_patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN sk_patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
        UNION ALL
        SELECT 'birth_year', $null_threshold_pct, COUNT(*), SUM(CASE WHEN birth_year IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
        UNION ALL
        SELECT 'birth_month', $null_threshold_pct, COUNT(*), SUM(CASE WHEN birth_month IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
        UNION ALL
        SELECT 'gender_concept_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN gender_concept_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
    )
),

observation_checks AS (
    SELECT
        'OBSERVATION' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
        UNION ALL
        SELECT 'person_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
        UNION ALL
        SELECT 'clinical_effective_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN clinical_effective_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
        UNION ALL
        SELECT 'observation_source_concept_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN observation_source_concept_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
    )
),

encounter_checks AS (
    SELECT
        'ENCOUNTER' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER
        UNION ALL
        SELECT 'person_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER
        UNION ALL
        SELECT 'clinical_effective_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN clinical_effective_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER
    )
),

medication_order_checks AS (
    SELECT
        'MEDICATION_ORDER' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER
        UNION ALL
        SELECT 'person_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER
        UNION ALL
        SELECT 'clinical_effective_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN clinical_effective_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER
    )
),

medication_statement_checks AS (
    SELECT
        'MEDICATION_STATEMENT' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT
        UNION ALL
        SELECT 'person_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT
        UNION ALL
        SELECT 'clinical_effective_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN clinical_effective_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT
    )
),

episode_of_care_checks AS (
    SELECT
        'EPISODE_OF_CARE' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE
        UNION ALL
        SELECT 'person_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE
        UNION ALL
        SELECT 'episode_of_care_start_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN episode_of_care_start_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE
    )
),

allergy_intolerance_checks AS (
    SELECT
        'ALLERGY_INTOLERANCE' AS table_name,
        column_name,
        threshold,
        total_rows,
        null_count,
        ROUND(100.0 * null_count / NULLIF(total_rows, 0), 4) AS null_pct
    FROM (
        SELECT 'id' AS column_name, 0.0 AS threshold, COUNT(*) AS total_rows, SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_count FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE
        UNION ALL
        SELECT 'patient_id', $null_threshold_pct, COUNT(*), SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE
        UNION ALL
        SELECT 'clinical_effective_date', $null_threshold_pct, COUNT(*), SUM(CASE WHEN clinical_effective_date IS NULL THEN 1 ELSE 0 END) FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE
    )
),

all_checks AS (
    SELECT * FROM patient_checks
    UNION ALL SELECT * FROM observation_checks
    UNION ALL SELECT * FROM encounter_checks
    UNION ALL SELECT * FROM medication_order_checks
    UNION ALL SELECT * FROM medication_statement_checks
    UNION ALL SELECT * FROM episode_of_care_checks
    UNION ALL SELECT * FROM allergy_intolerance_checks
)

SELECT
    'column_completeness' AS test_name,
    table_name,
    column_name AS test_subject,
    CASE 
        WHEN null_pct <= threshold THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    ROUND(100.0 - null_pct, 2) AS metric_value,  -- Completeness percentage
    ROUND(100.0 - threshold, 2) AS threshold,     -- Required completeness
    OBJECT_CONSTRUCT(
        'total_rows', total_rows,
        'null_count', null_count,
        'null_percentage', null_pct,
        'completeness_percentage', ROUND(100.0 - null_pct, 2),
        'threshold_null_pct', threshold
    )::VARCHAR AS details
FROM all_checks
ORDER BY status DESC, null_pct DESC, table_name, column_name;
