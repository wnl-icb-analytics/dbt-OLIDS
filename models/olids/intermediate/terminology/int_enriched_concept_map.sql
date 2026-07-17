{{
    config(
        materialized='table',
        schema='olids_stable',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_concept_id', 'target_concept_id'],
        alias='enriched_concept_map')
}}

/*
Enriched concept map: local source codes (EMIS/TPP and enumeration families)
mapped to their canonical targets, one row per source concept - join-safe
without any downstream dedupe. Snomed-as-source rows (OPCS, cluster maps) are excluded.
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
        concept_id,
        code_id,
        term,
        snomed_ct_concept_id
    FROM {{ ref('conformed_emis_clinical_code') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY concept_id
        ORDER BY code_id, snomed_ct_concept_id
    ) = 1
),

snomed_concepts AS (
    SELECT
        concept_id,
        code
    FROM {{ ref('conformed_concept') }}
    -- new feed uses identifier-style system names, not URIs
    WHERE system = 'snomed_info_sct'
),

enriched_existing AS (
    SELECT
        cm.source_concept_id,
        cm.source_code,
        cm.source_display,
        cm.source_system,
        cm.target_system,
        cm.equivalence,
        cm.equivalence_rank,
        CASE
            WHEN
                cm.target_code = '138875005'
                AND root_target.concept_id IS NOT NULL
                THEN root_target.concept_id
            -- successor rewrite: repoint the concept id alongside the code
            WHEN
                sct_history.new_concept_id IS NOT NULL
                AND successor_target.concept_id IS NOT NULL
                THEN successor_target.concept_id
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
        ) AS target_display,
        cm.is_primary = 1 AS is_primary
    FROM {{ ref('conformed_concept_map') }} AS cm
    LEFT JOIN emis_clinical AS emis_ref
        ON
            cm.source_concept_id = emis_ref.concept_id
            AND cm.target_code = '138875005'
    -- REVIEW: target_concept_id is resolved by SNOMED code because OLIDS no longer supplies it in EMIS reference.
    LEFT JOIN snomed_concepts AS root_target
        ON emis_ref.snomed_ct_concept_id = root_target.code
    LEFT JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS sct
        ON
            TRY_CAST(cm.target_code AS NUMBER(38, 0)) = sct."Id"
            AND sct."Active" = FALSE
    LEFT JOIN sct_history
        ON
            TRY_CAST(cm.target_code AS NUMBER(38, 0))
            = sct_history.old_concept_id
            AND sct."Id" IS NOT NULL
    -- falls back to cm.target_concept_id when the successor code has no OLIDS concept row
    LEFT JOIN snomed_concepts AS successor_target
        ON sct_history.new_concept_id::VARCHAR = successor_target.code
    -- local-code and enumeration families only; snomed-as-source rows
    -- (OPCS, cluster maps) are a different artefact, not this lookup
    WHERE cm.source_system NOT IN ('http:__snomed.info_sct', 'snomed_info_sct')
),

missing_emis_mappings AS (
    SELECT
        emis_ref.concept_id AS source_concept_id,
        emis_ref.code_id::VARCHAR AS source_code,
        emis_ref.term AS source_display,
        'EMIS_CodeID_cs' AS source_system,
        target.concept_id AS target_concept_id,
        emis_ref.snomed_ct_concept_id::VARCHAR AS target_code,
        emis_ref.term AS target_display,
        'snomed_info_sct' AS target_system,
        TRUE AS is_primary,
        'emis-reference-backfill' AS equivalence,
        1 AS equivalence_rank
    FROM emis_clinical AS emis_ref
    LEFT JOIN {{ ref('conformed_concept_map') }} AS cm
        ON emis_ref.concept_id = cm.source_concept_id
    -- REVIEW: target_concept_id is resolved by SNOMED code because OLIDS no longer supplies it in EMIS reference.
    LEFT JOIN snomed_concepts AS target
        ON emis_ref.snomed_ct_concept_id = target.code
    WHERE cm.source_concept_id IS NULL
),

{#-
    Fallback mappings for vocabularies the feed ships without CONCEPT_MAP rows.
    Keyed on (system, code), never concept UUID: each environment mints its own
    pseudonymised ids. Targets mirror the legacy feed's authoritative mappings.
-#}
{% set local_mappings = [
    {'system': 'EMIS_RegistrationStatus_cs', 'code': 'Deceased',
     'target_code': '725951000000101', 'target_display': 'GP22 deregistration - death'},
    {'system': 'EMIS_and_TPP_MedicationStatement_cs', 'code': 'Acute',
     'target_code': '1217105006', 'target_display': 'Prescription given'},
    {'system': 'EMIS_and_TPP_MedicationStatement_cs', 'code': 'Automatic',
     'target_code': '1217105006', 'target_display': 'Prescription given'},
    {'system': 'EMIS_and_TPP_MedicationStatement_cs', 'code': 'Repeat',
     'target_code': '182918009', 'target_display': 'Repeated prescription'},
    {'system': 'EMIS_and_TPP_MedicationStatement_cs', 'code': 'Repeat Dispensing',
     'target_code': '182918009', 'target_display': 'Repeated prescription'},
] %}

local_backfills AS (
    -- fallback only: applies when the feed supplies no CONCEPT_MAP row
    SELECT
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        NULL::VARCHAR AS target_concept_id,
        m.target_code,
        m.target_display,
        'snomed_info_sct' AS target_system,
        TRUE AS is_primary,
        'local-backfill' AS equivalence,
        1 AS equivalence_rank
    FROM {{ ref('landing_concept') }} AS src
    INNER JOIN (
        {% for m in local_mappings %}
        SELECT
            '{{ m.system }}' AS system,
            '{{ m.code }}' AS code,
            '{{ m.target_code }}' AS target_code,
            '{{ m.target_display }}' AS target_display
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ) AS m
        ON src.system = m.system AND src.code = m.code
    LEFT JOIN {{ ref('conformed_concept_map') }} AS cm
        ON src.concept_id = cm.source_concept_id
    WHERE cm.source_concept_id IS NULL
),

unmapped_passthrough AS (
    -- concepts with no CONCEPT_MAP row keep their source code and display so
    -- enumerations (date precision, address and contact types, statuses) stay
    -- usable downstream; target side stays null, so mapping gates still count
    -- these as unmapped. The final dedupe prefers any row with a target.
    SELECT
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        NULL::VARCHAR AS target_concept_id,
        NULL::VARCHAR AS target_code,
        NULL::VARCHAR AS target_display,
        NULL::VARCHAR AS target_system,
        TRUE AS is_primary,
        'unmapped-passthrough' AS equivalence,
        99 AS equivalence_rank
    FROM {{ ref('landing_concept') }} AS src
    LEFT JOIN {{ ref('conformed_concept_map') }} AS cm
        ON src.concept_id = cm.source_concept_id
    WHERE
        cm.source_concept_id IS NULL
        -- honour the snomed-as-source exclusion applied to mapped rows
        AND src.system NOT IN ('http:__snomed.info_sct', 'snomed_info_sct')
),

unioned AS (

    SELECT
        source_concept_id,
        source_code,
        source_display,
        source_system,
        target_concept_id,
        target_code,
        target_display,
        target_system,
        is_primary,
        equivalence,
        equivalence_rank
    FROM enriched_existing

    UNION ALL

    SELECT
        source_concept_id,
        source_code,
        source_display,
        source_system,
        target_concept_id,
        target_code,
        target_display,
        target_system,
        is_primary,
        equivalence,
        equivalence_rank
    FROM missing_emis_mappings

    UNION ALL

    SELECT
        source_concept_id,
        source_code,
        source_display,
        source_system,
        target_concept_id,
        target_code,
        target_display,
        target_system,
        is_primary,
        equivalence,
        equivalence_rank
    FROM local_backfills

    UNION ALL

    SELECT
        source_concept_id,
        source_code,
        source_display,
        source_system,
        target_concept_id,
        target_code,
        target_display,
        target_system,
        is_primary,
        equivalence,
        equivalence_rank
    FROM unmapped_passthrough
)

SELECT *
FROM unioned
-- one row per source concept: a local code has exactly one canonical mapping
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_concept_id
    ORDER BY target_display NULLS LAST, target_concept_id NULLS LAST
) = 1
