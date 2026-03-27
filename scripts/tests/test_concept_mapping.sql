/*
    Test: Concept Mapping Integrity
    
    Validates that concept_id fields correctly map through:
    concept_id → CONCEPT_MAP.source_code_id → CONCEPT_MAP.target_code_id → CONCEPT.id
    
    Returns standardised test results with PASS/FAIL status.
    Threshold: 100% mapping required (any unmapped = FAIL)
*/

WITH observation_obs_src AS (
    SELECT
        'OBSERVATION' AS table_name,
        'observation_source_concept_id' AS concept_field,
        COUNT(DISTINCT base.observation_source_concept_id) AS total_distinct,
        SUM(CASE WHEN base.observation_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.observation_source_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.observation_source_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.observation_source_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.observation_source_concept_id IS NOT NULL
),

observation_units AS (
    SELECT
        'OBSERVATION' AS table_name,
        'result_value_units_concept_id' AS concept_field,
        COUNT(DISTINCT base.result_value_units_concept_id) AS total_distinct,
        SUM(CASE WHEN base.result_value_units_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.result_value_units_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.result_value_units_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.result_value_units_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.result_value_units_concept_id IS NOT NULL
),

observation_precision AS (
    SELECT
        'OBSERVATION' AS table_name,
        'date_precision_concept_id' AS concept_field,
        COUNT(DISTINCT base.date_precision_concept_id) AS total_distinct,
        SUM(CASE WHEN base.date_precision_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.date_precision_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.date_precision_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.date_precision_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.date_precision_concept_id IS NOT NULL
),

observation_episodicity AS (
    SELECT
        'OBSERVATION' AS table_name,
        'episodicity_concept_id' AS concept_field,
        COUNT(DISTINCT base.episodicity_concept_id) AS total_distinct,
        SUM(CASE WHEN base.episodicity_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.episodicity_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.episodicity_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.episodicity_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.episodicity_concept_id IS NOT NULL
),

medication_statement_src AS (
    SELECT
        'MEDICATION_STATEMENT' AS table_name,
        'medication_statement_source_concept_id' AS concept_field,
        COUNT(DISTINCT base.medication_statement_source_concept_id) AS total_distinct,
        SUM(CASE WHEN base.medication_statement_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.medication_statement_source_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.medication_statement_source_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.medication_statement_source_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.medication_statement_source_concept_id IS NOT NULL
),

medication_order_src AS (
    SELECT
        'MEDICATION_ORDER' AS table_name,
        'medication_order_source_concept_id' AS concept_field,
        COUNT(DISTINCT base.medication_order_source_concept_id) AS total_distinct,
        SUM(CASE WHEN base.medication_order_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.medication_order_source_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.medication_order_source_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.medication_order_source_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.medication_order_source_concept_id IS NOT NULL
),

encounter_src AS (
    SELECT
        'ENCOUNTER' AS table_name,
        'encounter_source_concept_id' AS concept_field,
        COUNT(DISTINCT base.encounter_source_concept_id) AS total_distinct,
        SUM(CASE WHEN base.encounter_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.encounter_source_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.encounter_source_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.encounter_source_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.encounter_source_concept_id IS NOT NULL
),

allergy_src AS (
    SELECT
        'ALLERGY_INTOLERANCE' AS table_name,
        'allergy_intolerance_source_concept_id' AS concept_field,
        COUNT(DISTINCT base.allergy_intolerance_source_concept_id) AS total_distinct,
        SUM(CASE WHEN base.allergy_intolerance_source_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.allergy_intolerance_source_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.allergy_intolerance_source_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.allergy_intolerance_source_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.allergy_intolerance_source_concept_id IS NOT NULL
),

patient_gender AS (
    SELECT
        'PATIENT' AS table_name,
        'gender_concept_id' AS concept_field,
        COUNT(DISTINCT base.gender_concept_id) AS total_distinct,
        SUM(CASE WHEN base.gender_concept_id IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.gender_concept_id END) AS unmapped_concepts,
        SUM(CASE WHEN base.gender_concept_id IS NOT NULL AND cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS unmapped_rows
    FROM "NCL_Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT base
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT_MAP cm ON base.gender_concept_id = cm.source_code_id
    LEFT JOIN "NCL_Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT c ON cm.target_code_id = c.id
    WHERE base.gender_concept_id IS NOT NULL
),

all_results AS (
    SELECT * FROM observation_obs_src
    UNION ALL SELECT * FROM observation_units
    UNION ALL SELECT * FROM observation_precision
    UNION ALL SELECT * FROM observation_episodicity
    UNION ALL SELECT * FROM medication_statement_src
    UNION ALL SELECT * FROM medication_order_src
    UNION ALL SELECT * FROM encounter_src
    UNION ALL SELECT * FROM allergy_src
    UNION ALL SELECT * FROM patient_gender
)

SELECT
    'concept_mapping' AS test_name,
    table_name,
    concept_field AS test_subject,
    CASE 
        WHEN unmapped_concepts = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    ROUND(100.0 * (total_distinct - unmapped_concepts) / NULLIF(total_distinct, 0), 2) AS metric_value,
    100.0 AS threshold,
    OBJECT_CONSTRUCT(
        'total_distinct_concepts', total_distinct,
        'total_rows', total_rows,
        'unmapped_concepts', unmapped_concepts,
        'unmapped_rows', unmapped_rows,
        'mapped_percentage', ROUND(100.0 * (total_distinct - unmapped_concepts) / NULLIF(total_distinct, 0), 2)
    )::VARCHAR AS details
FROM all_results
ORDER BY status DESC, metric_value ASC, table_name, concept_field;
