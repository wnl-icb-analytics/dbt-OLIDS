SELECT selected.source_concept_id
FROM {{ ref('conformed_concept_map') }} AS selected
WHERE
    selected.mapping_status = 'last_known'
    AND EXISTS (
        SELECT 1
        FROM {{ ref('conformed_concept_map_history') }} AS candidate
        WHERE
            candidate.source_concept_id = selected.source_concept_id
            AND candidate.is_active = 1
            AND candidate.concept_map_name <> 'EMIS_to_READ_or_Local_cm'
            AND candidate.target_system <> 'EMIS_READ_Local_cs'
            AND NOT (
                candidate.target_system = 'snomed_info_sct'
                AND candidate.target_code = '138875005'
            )
    )
