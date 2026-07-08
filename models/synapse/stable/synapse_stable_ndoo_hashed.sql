{{
    config(
        cluster_by=['nhs_number_hash'],
        alias='ndoo_hashed',
        transient=false,
        tags=['stable']
    )
}}

SELECT
    id,
    lds_record_id,
    sk_patient_id,
    nhs_number_hash,
    preference_type,
    preference_status,
    lds_is_deleted,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_batch_id,
    lds_file_id,
    lds_dataset_id,
    effective_from,
    effective_to,
    is_latest,
    lakehouse_date_processed,
    high_watermark_date_time
FROM {{ ref('synapse_base_olids_ndoo_hashed') }}
