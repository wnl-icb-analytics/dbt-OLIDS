SELECT source_concept_id
FROM {{ ref('conformed_concept_map') }}
WHERE
    mapped_concept_system = 'snomed_info_sct'
    AND mapped_concept_code = '138875005'
