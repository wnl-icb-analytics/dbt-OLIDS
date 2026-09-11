{{ config(
    alias='healthcare_event', materialized='incremental', incremental_strategy='merge',
    unique_key='healthcare_event_id', on_schema_change='fail',
    cluster_by=['person_id', 'coalesce(event_at, event_date::timestamp_ntz)'],
    pre_hook="{{ longitudinal_build_warehouse() }}",
    post_hook=[
        "{{ longitudinal_remove_withdrawn_records('conformed_healthcare_event', 'healthcare_event_id') }}",
        "{{ longitudinal_build_warehouse(restore=true) }}"
    ]
) }}

SELECT
    healthcare_event_id,
    event_type,
    person_id,
    patient_id,
    sk_patient_id,
    event_at,
    event_date,
    event_time_precision,
    event_time_basis,
    source_record_type,
    source_record_id,
    appointment_id,
    clinical_record_id,
    event_code,
    event_code_name,
    event_coding_system,
    source_status_code,
    source_status_name,
    status_code,
    status_name,
    provider_organisation_id,
    publisher_organisation_id,
    publisher_organisation_code,
    source_extraction_date,
    provider_organisation_code,
    provider_code_authority,
    provider_organisation_name,
    publisher_organisation_name
FROM {{ ref('conformed_healthcare_event') }} as current_records
{{ longitudinal_delivery_filter() }}
