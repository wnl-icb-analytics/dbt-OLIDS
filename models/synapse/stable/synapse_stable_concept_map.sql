{{
    config(
        cluster_by=['source_concept_id', 'target_concept_id'],
        alias='concept_map',
        transient=false,
        tags=['stable']
    )
}}

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
