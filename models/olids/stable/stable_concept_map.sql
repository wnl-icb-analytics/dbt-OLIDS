{{
    config(
        cluster_by=['source_concept_id', 'mapped_concept_id'],
        transient=false,
        alias='concept_map'
    )
}}

SELECT
    source_concept_id,
    source_code,
    source_display,
    source_system,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    mapped_concept_system,
    mapping_name,
    mapping_status,
    mapping_is_active,
    equivalence AS mapping_equivalence,
    equivalence_rank AS mapping_equivalence_rank,
    mapping_updated_date
FROM {{ ref('conformed_concept_map') }}
