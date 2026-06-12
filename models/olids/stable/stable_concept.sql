{{
    config(
        materialized='incremental',
        unique_key='concept_id',
        on_schema_change='fail',
        cluster_by=['concept_id'],
        alias='concept',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    concept_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_is_deleted,
    lds_start_datetime
from {{ ref('base_olids_concept') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
