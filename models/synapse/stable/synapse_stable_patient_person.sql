{{
    config(
        unique_key=['patient_id', 'person_uuid'],
        cluster_by=['patient_id', 'person_id'],
        alias='patient_person',
        transient=false,
        tags=['stable']
    )
}}

SELECT
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
FROM {{ ref('synapse_base_olids_patient_person') }}
