{{
    config(
        materialized='incremental',
        unique_key=['patient_id', 'person_uuid'],
        on_schema_change='fail',
        cluster_by=['patient_id', 'person_id'],
        alias='patient_person',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    lds_source_record_id_person,
    patient_id,
    person_id,
    person_uuid,
    publisher_organisation_code,
    patient_shard_id,
    person_shard_id,
    lds_source_record_shard_id,
    lds_id,
    lds_business_key,
    lds_source_dataset_id,
    lds_cdm_event_id,
    lds_registrar_event_id,
    lds_datetime_update_acquired,
    lds_datetime_update_acquired_person,
    lds_is_deleted,
    lds_start_datetime,
    lds_end_datetime,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_patient_person') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
