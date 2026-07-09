{{
    config(
        materialized='table',
        schema='synapse_stable',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_concept_id', 'target_concept_id'],
        alias='enriched_concept_map')
}}

/*
Enriched concept map: local source codes (EMIS/TPP and enumeration families)
mapped to their canonical targets, one row per source concept - join-safe
without any downstream dedupe. Snomed-as-source rows are excluded.
Replaces retired SNOMED targets, repairs root targets and backfills missing EMIS mappings.
*/

WITH sct_history AS (
    SELECT
        h."OldConceptId" AS old_concept_id,
        h."NewConceptId" AS new_concept_id,
        h."NewConceptFullySpecifiedName" AS new_concept_display
    FROM {{ source('nhsd_snomed', 'SCT_History') }} AS h
    INNER JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS sct
        ON h."NewConceptId" = sct."Id"
    WHERE
        h."IsAmbiguous" = FALSE
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
    FROM {{ ref('synapse_base_emis_clinical_code') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY olids_emis_code_concept_id
        ORDER BY lds_start_date_time DESC
    ) = 1
),

enriched_existing AS (
    SELECT
        cm.mapped_item_id::VARCHAR AS mapped_item_id,
        cm.concept_map_id,
        cm.concept_map_resource_id,
        cm.concept_map_url,
        cm.concept_map_version,
        cm.source_concept_id,
        cm.source_system,
        cm.source_code,
        cm.source_display,
        cm.target_system,
        cm.is_primary,
        cm.is_active,
        cm.equivalence,
        cm.lds_start_datetime,
        CASE
            WHEN
                cm.target_code = '138875005'
                AND emis_ref.olids_snomed_concept_id IS NOT NULL
                THEN emis_ref.olids_snomed_concept_id
            ELSE cm.target_concept_id
        END AS target_concept_id,
        COALESCE(
            CASE
                WHEN
                    cm.target_code = '138875005'
                    AND emis_ref.snomed_ct_concept_id IS NOT NULL
                    THEN emis_ref.snomed_ct_concept_id::VARCHAR
            END,
            sct_history.new_concept_id::VARCHAR,
            cm.target_code
        ) AS target_code,
        COALESCE(
            CASE
                WHEN
                    cm.target_code = '138875005'
                    AND emis_ref.term IS NOT NULL
                    THEN emis_ref.term
            END,
            sct_history.new_concept_display,
            cm.target_display
        ) AS target_display
    FROM {{ ref('synapse_base_olids_concept_map') }} AS cm
    LEFT JOIN emis_clinical AS emis_ref
        ON
            cm.source_concept_id = emis_ref.olids_emis_code_concept_id
            AND cm.target_code = '138875005'
    LEFT JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS sct
        ON
            TRY_CAST(cm.target_code AS NUMBER(38, 0)) = sct."Id"
            AND sct."Active" = FALSE
    LEFT JOIN sct_history
        ON
            TRY_CAST(cm.target_code AS NUMBER(38, 0))
            = sct_history.old_concept_id
            AND sct."Id" IS NOT NULL
    -- local-code and enumeration families only; snomed-as-source rows
    -- (OPCS, cluster maps) are a different artefact, not this lookup
    WHERE cm.source_system NOT IN ('http://snomed.info/sct')
),

missing_emis_mappings AS (
    SELECT
        ('EMIS_BACKFILL_' || emis_ref.olids_emis_code_concept_id)::VARCHAR
            AS mapped_item_id,
        NULL::VARCHAR AS concept_map_id,
        NULL::VARCHAR AS concept_map_resource_id,
        'http://LDS.nhs/EMIStoSNOMED/CodeID/cm' AS concept_map_url,
        NULL::VARCHAR AS concept_map_version,
        emis_ref.olids_emis_code_concept_id AS source_concept_id,
        'http://LDS.nhs/EMIS/CodeID/cs' AS source_system,
        emis_ref.emis_code_id::VARCHAR AS source_code,
        emis_ref.term AS source_display,
        emis_ref.olids_snomed_concept_id AS target_concept_id,
        'http://snomed.info/sct' AS target_system,
        emis_ref.snomed_ct_concept_id::VARCHAR AS target_code,
        emis_ref.term AS target_display,
        TRUE AS is_primary,
        TRUE AS is_active,
        'emis-reference-backfill' AS equivalence,
        emis_ref.lds_start_date_time AS lds_start_datetime
    FROM emis_clinical AS emis_ref
    LEFT JOIN {{ ref('synapse_base_olids_concept_map') }} AS cm
        ON emis_ref.olids_emis_code_concept_id = cm.source_concept_id
    WHERE cm.source_concept_id IS NULL
),

/*
local_backfills: explicit row-by-row backfills for concepts known to be
missing from the upstream CONCEPT_MAP. Append a new SELECT for each.
*/
local_backfills AS (
    -- Episode-of-care registration status "Deceased" â€” upstream concept map
    -- has no row for this UUID; downstream consumers were getting NULL
    -- status for ~46k episodes. Maps to GP22 deregistration - death.
    SELECT
        'LOCAL_BACKFILL_DECEASED_EPISODE_STATUS'::VARCHAR AS mapped_item_id,
        NULL::VARCHAR AS concept_map_id,
        NULL::VARCHAR AS concept_map_resource_id,
        'http://LDS.nhs/local-backfill/cm' AS concept_map_url,
        NULL::VARCHAR AS concept_map_version,
        '5a8a5445-b192-671c-fba0-24048a06fcf4'::VARCHAR AS source_concept_id,
        'http://LDS.nhs/EMIS/RegistrationStatus/cs' AS source_system,
        'Deceased' AS source_code,
        'Deceased' AS source_display,
        NULL::VARCHAR AS target_concept_id,
        'http://snomed.info/sct' AS target_system,
        '725951000000101' AS target_code,
        'GP22 deregistration - death' AS target_display,
        TRUE AS is_primary,
        TRUE AS is_active,
        'local-backfill' AS equivalence,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS lds_start_datetime
),

unioned AS (

    SELECT
        mapped_item_id,
        concept_map_id,
        concept_map_resource_id,
        concept_map_url,
        concept_map_version,
        source_concept_id,
        source_system,
        source_code,
        source_display,
        target_concept_id,
        target_system,
        target_code,
        target_display,
        is_primary,
        is_active,
        equivalence,
        lds_start_datetime
    FROM enriched_existing

    UNION ALL

    SELECT
        mapped_item_id,
        concept_map_id,
        concept_map_resource_id,
        concept_map_url,
        concept_map_version,
        source_concept_id,
        source_system,
        source_code,
        source_display,
        target_concept_id,
        target_system,
        target_code,
        target_display,
        is_primary,
        is_active,
        equivalence,
        lds_start_datetime
    FROM missing_emis_mappings

    UNION ALL

    SELECT
        mapped_item_id,
        concept_map_id,
        concept_map_resource_id,
        concept_map_url,
        concept_map_version,
        source_concept_id,
        source_system,
        source_code,
        source_display,
        target_concept_id,
        target_system,
        target_code,
        target_display,
        is_primary,
        is_active,
        equivalence,
        lds_start_datetime
    FROM local_backfills
)

SELECT *
FROM unioned
-- one row per source concept: a local code has exactly one canonical mapping
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_concept_id
    ORDER BY target_display NULLS LAST, target_concept_id NULLS LAST
) = 1
