{{
    config(alias='PROCEDURE_REQUEST')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    practitioner_id,
    encounter_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    date_recorded,
    description,
    procedure_request_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    is_confidential,
    status_source_concept_id,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_source_dataset,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PROCEDURE_REQUEST') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
