{{ config(materialized='table', transient=true) }}

-- One row per empty native observation with a valued child on the same clinical day.
WITH empty_observations AS (
    SELECT id, patient_id, clinical_effective_date, mapped_concept_code, source_code, source_system
    FROM {{ ref('conformed_observation') }}
    WHERE source_entity = 'observation'
        AND NOT COALESCE(lds_is_deleted, FALSE)
        AND result_value IS NULL
        AND result_text IS NULL
),
valued_children AS (
    SELECT parent_observation_id, patient_id, clinical_effective_date,
        mapped_concept_code, source_code, source_system
    FROM {{ ref('conformed_observation') }}
    WHERE source_entity = 'observation'
        AND NOT COALESCE(lds_is_deleted, FALSE)
        AND parent_observation_id IS NOT NULL
        AND result_value IS NOT NULL
)

SELECT DISTINCT parent.id, TRUE AS is_result_header
FROM empty_observations AS parent
INNER JOIN valued_children AS child
    ON child.parent_observation_id = parent.id
    AND child.patient_id = parent.patient_id
    AND child.clinical_effective_date::DATE = parent.clinical_effective_date::DATE
    AND (
        child.mapped_concept_code = parent.mapped_concept_code
        OR (child.source_code = parent.source_code AND child.source_system = parent.source_system)
    )
