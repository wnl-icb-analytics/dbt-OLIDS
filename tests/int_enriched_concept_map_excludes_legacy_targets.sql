SELECT source_concept_id
FROM {{ ref('int_enriched_concept_map') }}
WHERE
    target_system = 'EMIS_READ_Local_cs'
    OR mapping_name = 'EMIS_to_READ_or_Local_cm'
