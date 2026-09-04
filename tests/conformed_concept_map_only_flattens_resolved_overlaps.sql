/*
Fail if a new map family overlaps at different targets without an explicit
preference, or if the semantic ranking still leaves different targets tied.
The final code and ID ordering must only make output deterministic, not choose
between clinically different mappings.
*/

{% set tpp_referral_specific_map =
    'TPP_to_DataDictionary_ReferralService_' ~
    'usingMainSpecAndTreatmentFunction_cm'
%}
{% set tpp_referral_general_map = 'TPP_to_DataDictionary_ReferralService_cm' %}

WITH eligible AS (
    SELECT
        source_concept_id,
        target_code,
        concept_map_name,
        is_active,
        last_updated_date,
        equivalence_rank,
        is_primary,
        CASE
            WHEN concept_map_name = 'EMIS_to_SNOMED_DrugCodeID_cm' THEN 1
            WHEN concept_map_name = 'EMIS_to_SNOMED_CodeID_cm' THEN 2
            WHEN concept_map_name = '{{ tpp_referral_specific_map }}' THEN 1
            WHEN concept_map_name = '{{ tpp_referral_general_map }}' THEN 2
            ELSE 1
        END AS map_preference
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
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY source_concept_id
            ORDER BY
                IFF(is_active = 1, 0, 1) ASC,
                map_preference ASC,
                IFF(
                    is_active = 1, NULL, last_updated_date
                ) DESC NULLS LAST,
                equivalence_rank ASC NULLS LAST,
                IFF(is_primary = 1, 0, 1) ASC,
                last_updated_date DESC NULLS LAST
        ) AS semantic_rank
    FROM eligible
),

source_profile AS (
    SELECT
        source_concept_id,
        COUNT(DISTINCT target_code) AS target_count,
        COUNT(DISTINCT concept_map_name) AS map_count,
        COUNT(DISTINCT IFF(
            semantic_rank = 1, target_code, NULL
        )) AS top_target_count,
        COUNT_IF(
            concept_map_name = 'EMIS_to_SNOMED_CodeID_cm'
        ) > 0 AS has_emis_clinical,
        COUNT_IF(
            concept_map_name = 'EMIS_to_SNOMED_DrugCodeID_cm'
        ) > 0 AS has_emis_drug,
        COUNT_IF(
            concept_map_name = 'TPP_to_DataDictionary_ReferralService_cm'
        ) > 0 AS has_tpp_general_referral,
        COUNT_IF(
            concept_map_name = '{{ tpp_referral_specific_map }}'
        ) > 0 AS has_tpp_specific_referral
    FROM ranked
    GROUP BY 1
)

SELECT source_concept_id
FROM source_profile
WHERE
    top_target_count > 1
    OR (
        target_count > 1
        AND map_count > 1
        AND NOT (
            (
                map_count = 2
                AND has_emis_clinical
                AND has_emis_drug
            )
            OR (
                map_count = 2
                AND has_tpp_general_referral
                AND has_tpp_specific_referral
            )
        )
    )
