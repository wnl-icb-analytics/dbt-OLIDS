{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['patient_id', 'start_date'],
        alias='patient_contact',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    id,
    person_id,
    patient_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    contact_type,
    contact_type_source_concept_id,
    start_date,
    end_date,
    publisher_organisation_code,
    patient_shard_id,
    person_shard_id,
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
from {{ ref('base_olids_patient_contact') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
