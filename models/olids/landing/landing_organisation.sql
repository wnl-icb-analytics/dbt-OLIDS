{{
    config(alias='ORGANISATION')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    organisation_code,
    assigning_authority_code,
    name,
    type_description,
    primary_location_type_source_concept_id,
    postcode,
    parent_organisation_id,
    open_date,
    close_date,
    is_obsolete,
    lds_is_deleted,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'ORGANISATION') }}
