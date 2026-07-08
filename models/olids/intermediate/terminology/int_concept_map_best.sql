{{
    config(
        materialized='table',
        schema='olids_stable',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_concept_id'],
        alias='concept_map_best')
}}

/*
Best mapping per source concept.
One row per source_concept_id from the enriched map, so clinical models can join
without a per-model QUALIFY to collapse map fanout.
Row picked by target_display then target_concept_id (NULLS LAST) — the ordering the
old per-model QUALIFYs used. Multi-alias models previously picked a single combined
row across all aliases, so per-alias picks can differ on genuinely ambiguous
mappings; a deterministic pick per concept is the intended improvement.
*/

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
QUALIFY
    ROW_NUMBER()
        OVER (
            PARTITION BY source_concept_id
            ORDER BY target_display NULLS LAST, target_concept_id NULLS LAST
        )
    = 1
