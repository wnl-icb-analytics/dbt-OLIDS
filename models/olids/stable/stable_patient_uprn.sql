{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['masked_uprn'],
        alias='patient_uprn',
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
    patient_address_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    masked_uprn,
    masked_usrn,
    masked_postcode,
    address_format_quality,
    postcode_quality,
    matched_with_assign,
    qualifier,
    classification,
    algorithm,
    match_pattern,
    publisher_organisation_code,
    patient_shard_id,
    person_shard_id,
    lds_source_record_shard_id,
    lds_id,
    lds_source_dataset_id,
    lds_cdm_event_id,
    lds_registrar_event_id,
    lds_datetime_update_acquired,
    lds_is_deleted,
    lds_start_datetime,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_patient_uprn') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
