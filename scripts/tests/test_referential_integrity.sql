/*
    Test: Referential Integrity
    
    Validates that foreign key columns reference existing records in parent tables.
    Uses naming convention: <table_name>_id → <TABLE_NAME>.id
    
    Returns standardised test results with PASS/FAIL status.
    Threshold: 100% referential integrity required
*/

WITH fk_checks AS (
    -- OBSERVATION foreign keys
    SELECT
        'OBSERVATION' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT o.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN o.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN o.patient_id IS NOT NULL AND p.id IS NULL THEN o.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN o.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION o
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON o.patient_id = p.id
    
    UNION ALL
    
    SELECT
        'OBSERVATION' AS child_table,
        'encounter_id' AS fk_column,
        'ENCOUNTER' AS parent_table,
        COUNT(DISTINCT o.encounter_id) AS total_distinct_fk,
        SUM(CASE WHEN o.encounter_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN o.encounter_id IS NOT NULL AND e.id IS NULL THEN o.encounter_id END) AS orphaned_fk,
        SUM(CASE WHEN o.encounter_id IS NOT NULL AND e.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION o
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER e ON o.encounter_id = e.id
    
    UNION ALL
    
    SELECT
        'OBSERVATION' AS child_table,
        'practitioner_id' AS fk_column,
        'PRACTITIONER' AS parent_table,
        COUNT(DISTINCT o.practitioner_id) AS total_distinct_fk,
        SUM(CASE WHEN o.practitioner_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN o.practitioner_id IS NOT NULL AND pr.id IS NULL THEN o.practitioner_id END) AS orphaned_fk,
        SUM(CASE WHEN o.practitioner_id IS NOT NULL AND pr.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION o
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.PRACTITIONER pr ON o.practitioner_id = pr.id
    
    UNION ALL
    
    -- ENCOUNTER foreign keys
    SELECT
        'ENCOUNTER' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT e.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN e.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN e.patient_id IS NOT NULL AND p.id IS NULL THEN e.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN e.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER e
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON e.patient_id = p.id
    
    UNION ALL
    
    SELECT
        'ENCOUNTER' AS child_table,
        'practitioner_id' AS fk_column,
        'PRACTITIONER' AS parent_table,
        COUNT(DISTINCT e.practitioner_id) AS total_distinct_fk,
        SUM(CASE WHEN e.practitioner_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN e.practitioner_id IS NOT NULL AND pr.id IS NULL THEN e.practitioner_id END) AS orphaned_fk,
        SUM(CASE WHEN e.practitioner_id IS NOT NULL AND pr.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER e
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.PRACTITIONER pr ON e.practitioner_id = pr.id
    
    UNION ALL
    
    SELECT
        'ENCOUNTER' AS child_table,
        'episode_of_care_id' AS fk_column,
        'EPISODE_OF_CARE' AS parent_table,
        COUNT(DISTINCT e.episode_of_care_id) AS total_distinct_fk,
        SUM(CASE WHEN e.episode_of_care_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN e.episode_of_care_id IS NOT NULL AND eoc.id IS NULL THEN e.episode_of_care_id END) AS orphaned_fk,
        SUM(CASE WHEN e.episode_of_care_id IS NOT NULL AND eoc.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER e
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE eoc ON e.episode_of_care_id = eoc.id
    
    UNION ALL
    
    -- MEDICATION_ORDER foreign keys
    SELECT
        'MEDICATION_ORDER' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT mo.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN mo.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN mo.patient_id IS NOT NULL AND p.id IS NULL THEN mo.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN mo.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER mo
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON mo.patient_id = p.id
    
    UNION ALL
    
    SELECT
        'MEDICATION_ORDER' AS child_table,
        'medication_statement_id' AS fk_column,
        'MEDICATION_STATEMENT' AS parent_table,
        COUNT(DISTINCT mo.medication_statement_id) AS total_distinct_fk,
        SUM(CASE WHEN mo.medication_statement_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN mo.medication_statement_id IS NOT NULL AND ms.id IS NULL THEN mo.medication_statement_id END) AS orphaned_fk,
        SUM(CASE WHEN mo.medication_statement_id IS NOT NULL AND ms.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER mo
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT ms ON mo.medication_statement_id = ms.id
    
    UNION ALL
    
    -- MEDICATION_STATEMENT foreign keys
    SELECT
        'MEDICATION_STATEMENT' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT ms.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN ms.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN ms.patient_id IS NOT NULL AND p.id IS NULL THEN ms.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN ms.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT ms
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON ms.patient_id = p.id
    
    UNION ALL
    
    -- EPISODE_OF_CARE foreign keys
    SELECT
        'EPISODE_OF_CARE' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT eoc.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN eoc.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN eoc.patient_id IS NOT NULL AND p.id IS NULL THEN eoc.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN eoc.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE eoc
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON eoc.patient_id = p.id
    
    UNION ALL
    
    SELECT
        'EPISODE_OF_CARE' AS child_table,
        'organisation_id' AS fk_column,
        'ORGANISATION' AS parent_table,
        COUNT(DISTINCT eoc.organisation_id) AS total_distinct_fk,
        SUM(CASE WHEN eoc.organisation_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN eoc.organisation_id IS NOT NULL AND org.id IS NULL THEN eoc.organisation_id END) AS orphaned_fk,
        SUM(CASE WHEN eoc.organisation_id IS NOT NULL AND org.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE eoc
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION org ON eoc.organisation_id = org.id
    
    UNION ALL
    
    -- ALLERGY_INTOLERANCE foreign keys
    SELECT
        'ALLERGY_INTOLERANCE' AS child_table,
        'patient_id' AS fk_column,
        'PATIENT' AS parent_table,
        COUNT(DISTINCT ai.patient_id) AS total_distinct_fk,
        SUM(CASE WHEN ai.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN ai.patient_id IS NOT NULL AND p.id IS NULL THEN ai.patient_id END) AS orphaned_fk,
        SUM(CASE WHEN ai.patient_id IS NOT NULL AND p.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE ai
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT p ON ai.patient_id = p.id
    
    UNION ALL
    
    SELECT
        'ALLERGY_INTOLERANCE' AS child_table,
        'encounter_id' AS fk_column,
        'ENCOUNTER' AS parent_table,
        COUNT(DISTINCT ai.encounter_id) AS total_distinct_fk,
        SUM(CASE WHEN ai.encounter_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN ai.encounter_id IS NOT NULL AND e.id IS NULL THEN ai.encounter_id END) AS orphaned_fk,
        SUM(CASE WHEN ai.encounter_id IS NOT NULL AND e.id IS NULL THEN 1 ELSE 0 END) AS orphaned_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE ai
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER e ON ai.encounter_id = e.id
)

SELECT
    'referential_integrity' AS test_name,
    child_table AS table_name,
    fk_column || ' → ' || parent_table AS test_subject,
    CASE 
        WHEN orphaned_fk = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    ROUND(100.0 * (total_distinct_fk - orphaned_fk) / NULLIF(total_distinct_fk, 0), 2) AS metric_value,
    100.0 AS threshold,
    OBJECT_CONSTRUCT(
        'total_distinct_fk', total_distinct_fk,
        'total_rows_with_fk', total_rows_with_fk,
        'orphaned_fk_values', orphaned_fk,
        'orphaned_rows', orphaned_rows,
        'integrity_percentage', ROUND(100.0 * (total_distinct_fk - orphaned_fk) / NULLIF(total_distinct_fk, 0), 2)
    )::VARCHAR AS details
FROM fk_checks
WHERE total_rows_with_fk > 0
ORDER BY status DESC, metric_value ASC, child_table, fk_column;
