{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['appointment_id', 'practitioner_id'],
        alias='appointment_practitioner',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    lds_record_shard_id,
    id,
    appointment_id,
    practitioner_id,
    publisher_organisation_id,
    provider_organisation_id,
    publisher_organisation_code,
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
from {{ ref('base_olids_appointment_practitioner') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
