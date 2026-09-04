{{
    config(
        materialized='table',
        tags=['conformed', 'terminology'],
        cluster_by=['source_concept_id', 'mapped_concept_id'],
        alias='concept_map')
}}

{% set emis_drug_map = 'EMIS_to_SNOMED_DrugCodeID_cm' %}
{% set emis_clinical_map = 'EMIS_to_SNOMED_CodeID_cm' %}
{% set tpp_referral_specific_map =
    'TPP_to_DataDictionary_ReferralService_' ~
    'usingMainSpecAndTreatmentFunction_cm'
%}
{% set tpp_referral_general_map = 'TPP_to_DataDictionary_ReferralService_cm' %}

/*
One row per non-SNOMED source concept, with its best mapping from V2.

READ/local targets and the non-specific SNOMED root remain in
CONCEPT_MAP_HISTORY. If no active mapping exists, the latest inactive mapping
from the preferred map is retained. EMIS_CLINICAL_CODE supplies a final SNOMED
fallback when V2 has no usable analytical mapping.
*/

-- Use CONCEPT as the spine so concepts remain joinable when no map exists.
WITH source_concepts AS (
    SELECT
        concept_id,
        code,
        display,
        system
    FROM {{ ref('conformed_concept') }}
    WHERE system NOT IN ('http:__snomed.info_sct', 'snomed_info_sct')
),

/*
Remove targets that are unsuitable for a general analytical join, then choose
one V2 row per source concept. Drug and detailed referral mappings take
precedence over their general equivalents. For inactive rows from the same
preferred map, the most recent version is the last known mapping.
*/
ranked_mappings AS (
    SELECT
        source_concept_id,
        target_concept_id,
        target_code,
        target_display,
        target_system,
        is_primary,
        equivalence,
        equivalence_rank,
        concept_map_name,
        is_active,
        last_updated_date
    FROM {{ ref('conformed_concept_map_history') }}
    WHERE
        source_concept_id IS NOT NULL
        AND source_system NOT IN (
            'http:__snomed.info_sct', 'snomed_info_sct'
        )
        AND concept_map_name <> 'EMIS_to_READ_or_Local_cm'
        AND target_system <> 'EMIS_READ_Local_cs'
        AND target_code IS NOT NULL
        AND NOT (
            target_system = 'snomed_info_sct'
            AND target_code = '138875005'
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_concept_id
        ORDER BY
            IFF(is_active = 1, 0, 1),
            CASE
                WHEN concept_map_name = '{{ emis_drug_map }}' THEN 1
                WHEN concept_map_name = '{{ emis_clinical_map }}' THEN 2
                WHEN concept_map_name = '{{ tpp_referral_specific_map }}' THEN 1
                WHEN concept_map_name = '{{ tpp_referral_general_map }}' THEN 2
                ELSE 1
            END,
            IFF(is_active = 1, NULL, last_updated_date) DESC NULLS LAST,
            equivalence_rank ASC NULLS LAST,
            IFF(is_primary = 1, 0, 1),
            last_updated_date DESC NULLS LAST,
            concept_map_name,
            target_code,
            target_concept_id
    ) = 1
),

/*
Some EMIS concepts have no analytical V2 row but do have a supplied SNOMED
mapping in EMIS_CLINICAL_CODE. Use that only after V2 has been exhausted.
*/
emis_fallbacks AS (
    SELECT
        concept_id AS source_concept_id,
        snomed_ct_concept_id::VARCHAR AS target_code,
        term AS target_display
    FROM {{ ref('conformed_emis_clinical_code') }}
    WHERE snomed_ct_concept_id::VARCHAR <> '138875005'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY concept_id
        ORDER BY code_id, snomed_ct_concept_id
    ) = 1
),

-- Preserve concepts that have only a READ/local mapping, without selecting it.
legacy_sources AS (
    SELECT DISTINCT source_concept_id
    FROM {{ ref('conformed_concept_map_history') }}
    WHERE
        concept_map_name = 'EMIS_to_READ_or_Local_cm'
        OR target_system = 'EMIS_READ_Local_cs'
)

/*
V2 wins whenever it has a usable mapping. The EMIS reference is the only
fallback. Every other source concept remains as legacy-only or unmapped.
*/
SELECT  -- noqa: ST06
    source.concept_id AS source_concept_id,
    source.code AS source_code,
    source.display AS source_display,
    source.system AS source_system,
    COALESCE(mapping.target_concept_id, fallback_target.concept_id)
        AS mapped_concept_id,
    COALESCE(mapping.target_code, fallback.target_code)
        AS mapped_concept_code,
    COALESCE(mapping.target_display, fallback.target_display)
        AS mapped_concept_display,
    COALESCE(
        mapping.target_system,
        IFF(fallback.source_concept_id IS NOT NULL, 'snomed_info_sct', NULL)
    ) AS mapped_concept_system,
    COALESCE(
        mapping.is_primary = 1,
        fallback.source_concept_id IS NOT NULL,
        FALSE
    ) AS is_primary,
    COALESCE(
        mapping.equivalence,
        IFF(
            fallback.source_concept_id IS NOT NULL,
            'emis-reference-backfill',
            NULL
        )
    ) AS equivalence,
    COALESCE(
        mapping.equivalence_rank,
        IFF(fallback.source_concept_id IS NOT NULL, 1, NULL)
    ) AS equivalence_rank,
    COALESCE(
        mapping.concept_map_name,
        IFF(
            fallback.source_concept_id IS NOT NULL,
            'emis-reference-backfill',
            NULL
        )
    ) AS mapping_name,
    CASE
        WHEN mapping.source_concept_id IS NOT NULL
            THEN IFF(mapping.is_active = 1, 'active', 'last_known')
        WHEN fallback.source_concept_id IS NOT NULL THEN 'backfilled'
        WHEN legacy.source_concept_id IS NOT NULL THEN 'legacy_only'
        ELSE 'unmapped'
    END AS mapping_status,
    COALESCE(
        mapping.is_active = 1,
        fallback.source_concept_id IS NOT NULL,
        FALSE
    ) AS mapping_is_active,
    mapping.last_updated_date AS mapping_updated_date
FROM source_concepts AS source
LEFT JOIN ranked_mappings AS mapping
    ON source.concept_id = mapping.source_concept_id
LEFT JOIN emis_fallbacks AS fallback
    ON
        source.concept_id = fallback.source_concept_id
        AND mapping.source_concept_id IS NULL
LEFT JOIN {{ ref('conformed_concept') }} AS fallback_target
    ON
        fallback.target_code = fallback_target.code
        AND fallback_target.system = 'snomed_info_sct'
LEFT JOIN legacy_sources AS legacy
    ON source.concept_id = legacy.source_concept_id
