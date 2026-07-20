{{
    config(
        cluster_by=['organisation_code'],
        transient=false,
        alias='organisation'
    )
}}

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
FROM {{ ref('conformed_organisation') }}
