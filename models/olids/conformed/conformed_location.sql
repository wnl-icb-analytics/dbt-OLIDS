{{
    config(
        secure=true,
        alias='location')
}}

/*
Conformed LOCATION view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.name,
    src.location_type_source_concept_id,
    src.type_description,
    src.is_primary_location,
    src.house_name,
    src.house_number,
    src.house_name_flat_number,
    src.street,
    src.address_line_1,
    src.address_line_2,
    src.address_line_3,
    src.address_line_4,
    src.postcode,
    src.managing_organisation_id,
    src.open_date,
    src.close_date,
    src.is_obsolete,
    src.lds_is_deleted,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_location') }} AS src
