{{
    config(
        cluster_by=['source_concept_id', 'target_concept_id'],
        transient=false,
        alias='concept_map'
    )
}}

SELECT
    source_concept_id,
    source_code,
    source_display,
    source_system,
    target_concept_id,
    target_code,
    target_display,
    target_system,
    is_primary,
    equivalence,
    equivalence_rank
FROM {{ ref('int_enriched_concept_map') }}
