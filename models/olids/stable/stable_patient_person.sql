{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['patient_id', 'person_id'],
        alias='patient_person',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    lds_record_id_person,
    id,
    patient_id,
    person_id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_datetime_data_acquired,
    lds_is_deleted,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_patient_person') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}