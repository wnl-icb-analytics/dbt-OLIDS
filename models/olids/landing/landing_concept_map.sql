{{
    config(alias='CONCEPT_MAP_V2')
}}

/*
Single daily scan of the parallel V2 source. This preserves every mapping row.
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
FROM {{ source('olids_pseudo', 'CONCEPT_MAP_V2') }}
