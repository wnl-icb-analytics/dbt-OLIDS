SELECT source_concept_id
FROM {{ ref('int_enriched_concept_map') }}
WHERE
    target_system = 'snomed_info_sct'
    AND target_code = '138875005'
