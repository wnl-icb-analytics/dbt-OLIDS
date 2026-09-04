SELECT selected.source_concept_id
FROM {{ ref('conformed_concept_map') }} AS selected
WHERE
    selected.mapping_status = 'last_known'
    AND EXISTS (
        SELECT 1
        FROM {{ ref('conformed_concept_map_history') }} AS candidate
        WHERE
            candidate.source_concept_id = selected.source_concept_id
            AND candidate.concept_map_name = selected.mapping_name
            AND candidate.is_active <> 1
            AND candidate.target_code IS NOT NULL
            AND candidate.last_updated_date > selected.mapping_updated_date
    )
