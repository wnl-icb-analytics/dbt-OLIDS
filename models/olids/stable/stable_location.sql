{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id'],
        alias='location',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    lds_source_record_id,
    id,
    name,
    location_type_source_concept_id,
    type_description,
    is_primary_location,
    house_name,
    house_number,
    house_name_flat_number,
    street,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    postcode,
    managing_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    publisher_organisation_code,
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
from {{ ref('base_olids_location') }}

{% if is_incremental() %}
    where lds_start_datetime > (select max(lds_start_datetime) from {{ this }})
{% endif %}
