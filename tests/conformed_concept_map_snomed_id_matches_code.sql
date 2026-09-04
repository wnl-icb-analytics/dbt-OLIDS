SELECT mapping.source_concept_id
FROM {{ ref('conformed_concept_map') }} AS mapping
INNER JOIN {{ ref('conformed_concept') }} AS target
    ON mapping.mapped_concept_id = target.concept_id
WHERE
    mapping.mapped_concept_system = 'snomed_info_sct'
    AND target.code <> mapping.mapped_concept_code
