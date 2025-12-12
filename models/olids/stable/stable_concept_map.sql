{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['source_code_id', 'target_code_id'],
        alias='concept_map',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_code_id,
    source_system,
    source_code,
    source_display,
    target_code_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_date_time
from {{ ref('base_olids_concept_map') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}