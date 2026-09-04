{% set referral_specific_map =
    'TPP_to_DataDictionary_ReferralService_' ~
    'usingMainSpecAndTreatmentFunction_cm'
%}

SELECT selected.source_concept_id
FROM {{ ref('int_enriched_concept_map') }} AS selected
WHERE
    (
        selected.mapping_name = 'EMIS_to_SNOMED_CodeID_cm'
        AND EXISTS (
            SELECT 1
            FROM {{ ref('conformed_concept_map') }} AS candidate
            WHERE
                candidate.source_concept_id = selected.source_concept_id
                AND candidate.concept_map_name = 'EMIS_to_SNOMED_DrugCodeID_cm'
                AND candidate.is_active = 1
                AND candidate.target_code <> '138875005'
        )
    )
    OR (
        selected.mapping_name = 'TPP_to_DataDictionary_ReferralService_cm'
        AND EXISTS (
            SELECT 1
            FROM {{ ref('conformed_concept_map') }} AS candidate
            WHERE
                candidate.source_concept_id = selected.source_concept_id
                AND candidate.concept_map_name = '{{ referral_specific_map }}'
                AND candidate.is_active = 1
        )
    )
