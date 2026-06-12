-- depends_on: {{ ref('stable_patient') }}

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id'],
        alias='person',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    id,
    person_uuid,
    person_version_id,
    person_record_type,
    matched_nhs_no_hash,
    sk_patient_id,
    gender,
    birth_year,
    birth_month,
    death_year,
    death_month,
    death_notification_status,
    postcode_hash,
    preferred_contact_method,
    nominated_pharmacy,
    dispensing_doctor,
    medical_appliance_supplier,
    gp_practice_code,
    gp_registration_date,
    as_at_date,
    sensitivity_flag,
    error_success_code,
    person_shard_id,
    lds_source_record_id,
    lds_source_record_shard_id,
    lds_id,
    lds_source_dataset_id,
    lds_cdm_event_id,
    lds_datetime_first_acquired_person,
    lds_datetime_update_acquired_person,
    lds_is_deleted,
    lds_start_datetime,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_person') }}

{% if is_incremental() %}
where lds_start_datetime > (
    select coalesce(max(lds_start_datetime), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
