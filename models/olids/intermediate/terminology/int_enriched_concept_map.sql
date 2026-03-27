{{
    config(
        materialized='table',
        schema='olids',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_code_id', 'target_code_id'],
        alias='enriched_concept_map')
}}

/*
Enriched Concept Map
Enhances the OLIDS concept map by:
1. Replacing retired SNOMED target codes with their active successor (via SCT_History)
2. Replacing root concept 138875005 targets with real SNOMED codes (via EMIS reference)
3. Adding missing EMIS->SNOMED mappings not present in the concept map (via EMIS reference)
*/

WITH sct_history AS (
    SELECT
        h."OldConceptId" AS old_concept_id,
        h."NewConceptId" AS new_concept_id,
        h."NewConceptFullySpecifiedName" AS new_concept_display
    FROM {{ source('nhsd_snomed', 'SCT_History') }} h
    INNER JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} sct
        ON h."NewConceptId" = sct."Id"
    WHERE h."IsAmbiguous" = FALSE
        AND sct."Active" = TRUE
),

emis_clinical AS (
    SELECT
        olids_emis_code_concept_id,
        emis_code_id,
        term,
        snomed_ct_concept_id,
        olids_snomed_concept_id,
        lds_start_date_time
    FROM {{ ref('base_emis_clinical_code') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY olids_emis_code_concept_id
        ORDER BY lds_start_date_time DESC
    ) = 1
),

enriched_existing AS (
    SELECT
        cm.id,
        cm.lds_id,
        cm.lds_business_key,
        cm.lds_dataset_id,
        cm.concept_map_id,
        cm.concept_map_resource_id,
        cm.concept_map_url,
        cm.concept_map_version,
        cm.source_code_id,
        cm.source_system,
        cm.source_code,
        cm.source_display,
        CASE
            WHEN cm.target_code = '138875005'
                AND emis_ref.olids_snomed_concept_id IS NOT NULL
                THEN emis_ref.olids_snomed_concept_id
            ELSE cm.target_code_id
        END AS target_code_id,
        cm.target_system,
        COALESCE(
            CASE
                WHEN cm.target_code = '138875005'
                    AND emis_ref.snomed_ct_concept_id IS NOT NULL
                    THEN emis_ref.snomed_ct_concept_id::VARCHAR
            END,
            sct_history.new_concept_id::VARCHAR,
            cm.target_code
        ) AS target_code,
        COALESCE(
            CASE
                WHEN cm.target_code = '138875005'
                    AND emis_ref.term IS NOT NULL
                    THEN emis_ref.term
            END,
            sct_history.new_concept_display,
            cm.target_display
        ) AS target_display,
        cm.is_primary,
        cm.is_active,
        cm.equivalence,
        cm.lds_start_date_time
    FROM {{ ref('base_olids_concept_map') }} cm
    LEFT JOIN emis_clinical emis_ref
        ON cm.source_code_id = emis_ref.olids_emis_code_concept_id
        AND cm.target_code = '138875005'
    LEFT JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} sct
        ON TRY_CAST(cm.target_code AS NUMBER(38,0)) = sct."Id"
        AND sct."Active" = FALSE
    LEFT JOIN sct_history
        ON TRY_CAST(cm.target_code AS NUMBER(38,0)) = sct_history.old_concept_id
        AND sct."Id" IS NOT NULL
),

missing_emis_mappings AS (
    SELECT
        'EMIS_BACKFILL_' || emis_ref.olids_emis_code_concept_id AS id,
        NULL::VARCHAR AS lds_id,
        NULL::VARCHAR AS lds_business_key,
        NULL::VARCHAR AS lds_dataset_id,
        NULL::VARCHAR AS concept_map_id,
        NULL::VARCHAR AS concept_map_resource_id,
        'http://LDS.nhs/EMIStoSNOMED/CodeID/cm' AS concept_map_url,
        NULL::VARCHAR AS concept_map_version,
        emis_ref.olids_emis_code_concept_id AS source_code_id,
        'http://LDS.nhs/EMIS/CodeID/cs' AS source_system,
        emis_ref.emis_code_id::VARCHAR AS source_code,
        emis_ref.term AS source_display,
        emis_ref.olids_snomed_concept_id AS target_code_id,
        'http://snomed.info/sct' AS target_system,
        emis_ref.snomed_ct_concept_id::VARCHAR AS target_code,
        emis_ref.term AS target_display,
        TRUE AS is_primary,
        TRUE AS is_active,
        'emis-reference-backfill' AS equivalence,
        emis_ref.lds_start_date_time
    FROM emis_clinical emis_ref
    LEFT JOIN {{ ref('base_olids_concept_map') }} cm
        ON emis_ref.olids_emis_code_concept_id = cm.source_code_id
    WHERE cm.source_code_id IS NULL
)

SELECT
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_code_id,
    source_system,
    source_code,
    source_display,
    target_code_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_date_time
FROM enriched_existing

UNION ALL

SELECT
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_code_id,
    source_system,
    source_code,
    source_display,
    target_code_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_date_time
FROM missing_emis_mappings
