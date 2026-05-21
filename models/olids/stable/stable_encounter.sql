{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['encounter_source_concept_id', 'clinical_effective_date'],
        alias='encounter',
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
    practitioner_id,
    appointment_id,
    episode_of_care_id,
    provider_organisation_id,
    publisher_organisation_id,
    author_organisation_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    location,
    encounter_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    type,
    sub_type,
    admission_method,
    end_date,
    date_recorded,
    publisher_organisation_code,
    patient_shard_id,
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
from {{ ref('base_olids_encounter') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
