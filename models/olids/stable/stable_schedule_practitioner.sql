{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['schedule_id', 'practitioner_id'],
        alias='schedule_practitioner',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    id,
    schedule_id,
    practitioner_id,
    publisher_organisation_id,
    publisher_organisation_code,
    lds_source_record_shard_id,
    lds_id,
    lds_business_key,
    lds_source_dataset_id,
    lds_cdm_event_id,
    lds_versioner_event_id,
    lds_datetime_first_acquired,
    lds_datetime_update_acquired,
    lds_is_deleted,
    lds_start_datetime,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_schedule_practitioner') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
