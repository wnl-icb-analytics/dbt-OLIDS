{{
    config(
        secure=true,
        alias='concept_map')
}}

/*
Complete CONCEPT_MAP_V2 history. Selection belongs in the enriched model.
*/

SELECT
    src.concept_map_name,
    src.source_concept_id,
    src.source_system,
    src.source_code,
    src.source_display,
    src.target_concept_id,
    src.target_system,
    src.target_code,
    src.target_display,
    src.equivalence,
    src.equivalence_rank,
    src.is_primary,
    src.is_active,
    src.last_updated_date
FROM {{ ref('landing_concept_map') }} AS src
