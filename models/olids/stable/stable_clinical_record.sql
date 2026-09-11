{{ config(
    alias='clinical_record', materialized='incremental', incremental_strategy='merge',
    unique_key='clinical_record_id', on_schema_change='fail',
    cluster_by=['person_id', 'clinical_record_date'],
    pre_hook="{{ longitudinal_build_warehouse() }}",
    post_hook=[
        "{{ longitudinal_remove_withdrawn_records('conformed_clinical_record', ['source_record_type', 'source_record_id']) }}",
        "{{ longitudinal_build_warehouse(restore=true) }}"
    ]
) }}

SELECT
    clinical_record_id,
    source_record_type,
    source_record_id,
    person_id,
    patient_id,
    sk_patient_id,
    encounter_id,
    clinical_record_date,
    clinical_date_precision_code,
    clinical_date_precision_name,
    date_recorded,
    source_code,
    source_code_name,
    source_coding_system,
    mapped_code,
    mapped_code_name,
    mapped_coding_system,
    result_value,
    result_date,
    result_unit_source_code,
    result_unit_source_name,
    medication_name,
    medication_dose,
    medication_quantity_value,
    medication_quantity_unit,
    medication_duration_days,
    medication_authorisation_type_code,
    medication_authorisation_type_name,
    provider_organisation_id,
    publisher_organisation_id,
    publisher_organisation_code,
    practitioner_id,
    source_extraction_date,
    provider_organisation_code,
    provider_code_authority,
    provider_organisation_name,
    publisher_organisation_name
FROM {{ ref('conformed_clinical_record') }} as current_records
{{ longitudinal_delivery_filter() }}
