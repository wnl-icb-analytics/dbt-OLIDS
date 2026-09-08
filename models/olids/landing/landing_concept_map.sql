{{
    config(alias='CONCEPT_MAP')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    concept_map_name,
    source_concept_id,
    source_system,
    source_code,
    source_display,
    target_concept_id,
    target_system,
    target_code,
    target_display,
    equivalence,
    equivalence_rank,
    is_primary,
    last_updated_date
FROM {{ source('olids_pseudo', 'CONCEPT_MAP') }}
