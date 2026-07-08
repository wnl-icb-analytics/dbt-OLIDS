{{
    config(
        materialized='table',
        schema='synapse_stable',
        tags=['intermediate', 'terminology'],
        cluster_by=['source_concept_id'],
        alias='concept_map_best')
}}

/*
Best mapping per source concept.
One row per source_concept_id from the enriched map, so base models can join
without a per-model QUALIFY to collapse map fanout.
Row picked by target_display then target_concept_id (NULLS LAST) — the ordering the
old per-model QUALIFYs used. Multi-alias models previously picked a single combined
row across all aliases, so per-alias picks can differ on genuinely ambiguous
mappings; a deterministic pick per concept is the intended improvement.
*/

SELECT
    mapped_item_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_concept_id,
    source_system,
    source_code,
    source_display,
    target_concept_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_datetime
FROM {{ ref('synapse_int_enriched_concept_map') }}
QUALIFY
    ROW_NUMBER()
        OVER (
            PARTITION BY source_concept_id
            ORDER BY target_display NULLS LAST, target_concept_id NULLS LAST
        )
    = 1
