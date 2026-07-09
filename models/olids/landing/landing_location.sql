{{
    config(alias='LOCATION')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
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
    lds_is_deleted,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'LOCATION') }}
