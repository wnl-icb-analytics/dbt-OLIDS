{{
    config(
        cluster_by=['source_concept_id', 'last_updated_date'],
        transient=false,
        alias='concept_map_history'
    )
}}

/*
Lossless CONCEPT_MAP_V2 history. This table retains READ/local mappings and
inactive versions for audit and specialist terminology work.
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
    is_active,
    last_updated_date
FROM {{ ref('conformed_concept_map_history') }}
