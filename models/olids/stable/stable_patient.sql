{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id'],
        alias='patient',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    id,
    person_id,
    nhs_number_hash,
    sk_patient_id,
    local_patient_id,
    title,
    gender_source_concept_id,
    gender_source_code,
    gender_source_display,
    gender_code,
    gender_display,
    publisher_organisation_id,
    registered_practice_organisation_id,
    birth_year,
    birth_month,
    death_year,
    death_month,
    is_confidential,
    is_test_patient,
    is_spine_sensitive,
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
from {{ ref('base_olids_patient') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
