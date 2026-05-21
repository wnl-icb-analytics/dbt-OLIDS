{{
    config(
        materialized='incremental',
        unique_key='mapped_item_id',
        on_schema_change='fail',
        cluster_by=['source_concept_id', 'target_concept_id'],
        alias='concept_map',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
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
from {{ ref('int_enriched_concept_map') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
