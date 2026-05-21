{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['mapped_concept_code', 'clinical_effective_date'],
        alias='allergy_intolerance',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    id,
    patient_id,
    person_id,
    practitioner_id,
    encounter_id,
    clinical_status,
    verification_status,
    category,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    date_precision_source_code,
    date_precision_source_display,
    date_precision_code,
    date_precision_display,
    is_review,
    medication_name,
    multi_lex_action,
    allergy_intolerance_source_concept_id,
    source_code,
    source_display,
    source_system,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    target_system,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
    is_confidential,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
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
from {{ ref('base_olids_allergy_intolerance') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
