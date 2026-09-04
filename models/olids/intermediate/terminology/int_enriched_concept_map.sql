{{
    config(
        materialized='table',
        schema='olids_conformed',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_concept_id', 'target_concept_id'],
        alias='enriched_concept_map')
}}

{% set emis_drug_map = 'EMIS_to_SNOMED_DrugCodeID_cm' %}
{% set emis_clinical_map = 'EMIS_to_SNOMED_CodeID_cm' %}
{% set tpp_referral_specific_map =
    'TPP_to_DataDictionary_ReferralService_' ~
    'usingMainSpecAndTreatmentFunction_cm'
%}
{% set tpp_referral_general_map = 'TPP_to_DataDictionary_ReferralService_cm' %}

/*
Join-safe analytical concept map at one row per source concept.

CONCEPT_MAP_V2 contains parallel map types and historical versions. This model
chooses one non-READ analytical target, preferring active mappings and then the
most specific map where V2 contains overlapping map families. If no mapping is
active, it retains the last known target and marks it as such.
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
    WHERE system = 'snomed_info_sct'
),

source_concepts AS (
    SELECT
        concept_id,
        code,
        display,
        system
    FROM {{ ref('conformed_concept') }}
    WHERE system NOT IN ('http:__snomed.info_sct', 'snomed_info_sct')
),

history_presence AS (
    SELECT
        source_concept_id,
        COUNT_IF(
            concept_map_name = 'EMIS_to_READ_or_Local_cm'
            OR target_system = 'EMIS_READ_Local_cs'
        ) > 0 AS has_legacy_mapping
    FROM {{ ref('conformed_concept_map') }}
    WHERE source_concept_id IS NOT NULL
    GROUP BY 1
),

analytical_history AS (
    SELECT
        cm.*,
        CASE
            WHEN cm.concept_map_name = '{{ emis_drug_map }}' THEN 1
            WHEN cm.concept_map_name = '{{ emis_clinical_map }}' THEN 2
            WHEN cm.concept_map_name = '{{ tpp_referral_specific_map }}' THEN 1
            WHEN cm.concept_map_name = '{{ tpp_referral_general_map }}' THEN 2
            ELSE 1
        END AS map_preference
    FROM {{ ref('conformed_concept_map') }} AS cm
    WHERE
        cm.source_concept_id IS NOT NULL
        AND cm.source_system NOT IN (
            'http:__snomed.info_sct', 'snomed_info_sct'
        )
        AND cm.concept_map_name <> 'EMIS_to_READ_or_Local_cm'
        AND cm.target_system <> 'EMIS_READ_Local_cs'
        AND NOT (
            cm.target_system = 'snomed_info_sct'
            AND cm.target_code = '138875005'
        )
),

selected_history AS (
    SELECT *
    FROM analytical_history
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_concept_id
        ORDER BY
            IFF(is_active = 1, 0, 1) ASC,
            map_preference ASC,
            equivalence_rank ASC NULLS LAST,
            IFF(is_primary = 1, 0, 1) ASC,
            last_updated_date DESC NULLS LAST,
            concept_map_name ASC,
            target_code ASC,
            target_concept_id ASC
    ) = 1
),

enriched_existing AS (
    SELECT  -- noqa: ST06
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        CASE
            WHEN
                cm.target_system = 'snomed_info_sct'
                AND sct_history.new_concept_id IS NOT NULL
                AND successor_target.concept_id IS NOT NULL
                THEN successor_target.concept_id
            ELSE cm.target_concept_id
        END AS target_concept_id,
        COALESCE(
            CASE
                WHEN cm.target_system = 'snomed_info_sct'
                    THEN sct_history.new_concept_id::VARCHAR
            END,
            cm.target_code
        ) AS target_code,
        COALESCE(
            CASE
                WHEN cm.target_system = 'snomed_info_sct'
                    THEN sct_history.new_concept_display
            END,
            cm.target_display
        ) AS target_display,
        cm.target_system,
        cm.is_primary = 1 AS is_primary,
        cm.equivalence,
        cm.equivalence_rank,
        cm.concept_map_name AS mapping_name,
        IFF(cm.is_active = 1, 'active', 'last_known') AS mapping_status,
        cm.is_active = 1 AS mapping_is_active,
        cm.last_updated_date AS mapping_updated_date,
        1 AS selection_priority
    FROM selected_history AS cm
    INNER JOIN source_concepts AS src
        ON cm.source_concept_id = src.concept_id
    LEFT JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS sct
        ON
            cm.target_system = 'snomed_info_sct'
            AND TRY_CAST(cm.target_code AS NUMBER(38, 0)) = sct."Id"
            AND sct."Active" = FALSE
    LEFT JOIN sct_history
        ON
            TRY_CAST(cm.target_code AS NUMBER(38, 0))
            = sct_history.old_concept_id
            AND sct."Id" IS NOT NULL
    LEFT JOIN snomed_concepts AS successor_target
        ON sct_history.new_concept_id::VARCHAR = successor_target.code
),

missing_emis_mappings AS (
    SELECT
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        target.concept_id AS target_concept_id,
        emis_ref.snomed_ct_concept_id::VARCHAR AS target_code,
        emis_ref.term AS target_display,
        'snomed_info_sct' AS target_system,
        TRUE AS is_primary,
        'emis-reference-backfill' AS equivalence,
        1 AS equivalence_rank,
        'emis-reference-backfill' AS mapping_name,
        'backfilled' AS mapping_status,
        TRUE AS mapping_is_active,
        NULL::DATE AS mapping_updated_date,
        2 AS selection_priority
    FROM emis_clinical AS emis_ref
    INNER JOIN source_concepts AS src
        ON emis_ref.concept_id = src.concept_id
    LEFT JOIN snomed_concepts AS target
        ON emis_ref.snomed_ct_concept_id = target.code
    WHERE
        emis_ref.snomed_ct_concept_id IS NOT NULL
        AND emis_ref.snomed_ct_concept_id::VARCHAR <> '138875005'
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

local_mapping_values AS (
    {% for m in local_mappings %}
        SELECT
            '{{ m.system }}' AS source_system,
            '{{ m.code }}' AS source_code,
            '{{ m.target_code }}' AS target_code,
            '{{ m.target_display }}' AS target_display
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
),

local_backfills AS (
    SELECT
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        target.concept_id AS target_concept_id,
        m.target_code,
        m.target_display,
        'snomed_info_sct' AS target_system,
        TRUE AS is_primary,
        'local-backfill' AS equivalence,
        1 AS equivalence_rank,
        'local-backfill' AS mapping_name,
        'backfilled' AS mapping_status,
        TRUE AS mapping_is_active,
        NULL::DATE AS mapping_updated_date,
        3 AS selection_priority
    FROM source_concepts AS src
    INNER JOIN local_mapping_values AS m
        ON
            src.system = m.source_system
            AND src.code = m.source_code
    LEFT JOIN snomed_concepts AS target
        ON m.target_code = target.code
),

unmapped_passthrough AS (
    SELECT
        src.concept_id AS source_concept_id,
        src.code AS source_code,
        src.display AS source_display,
        src.system AS source_system,
        NULL::VARCHAR AS target_concept_id,
        NULL::VARCHAR AS target_code,
        NULL::VARCHAR AS target_display,
        NULL::VARCHAR AS target_system,
        FALSE AS is_primary,
        NULL::VARCHAR AS equivalence,
        NULL::NUMBER AS equivalence_rank,
        NULL::VARCHAR AS mapping_name,
        IFF(
            presence.has_legacy_mapping, 'legacy_only', 'unmapped'
        ) AS mapping_status,
        FALSE AS mapping_is_active,
        NULL::DATE AS mapping_updated_date,
        9 AS selection_priority
    FROM source_concepts AS src
    LEFT JOIN history_presence AS presence
        ON src.concept_id = presence.source_concept_id
),

unioned AS (
    SELECT * FROM enriched_existing
    UNION ALL
    SELECT * FROM missing_emis_mappings
    UNION ALL
    SELECT * FROM local_backfills
    UNION ALL
    SELECT * FROM unmapped_passthrough
)

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
    equivalence_rank,
    mapping_name,
    mapping_status,
    mapping_is_active,
    mapping_updated_date
FROM unioned
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_concept_id
    ORDER BY
        selection_priority,
        target_code NULLS LAST,
        target_concept_id NULLS LAST
) = 1
