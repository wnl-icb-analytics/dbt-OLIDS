{{
    config(
        secure=true,
        alias='concept_map')
}}

/*
CONCEPT_MAP base view.
Uses the landing cache for the WNL pseudonymised feed.
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
    src.is_primary
FROM {{ ref('landing_concept_map') }} AS src
