{{
    config(alias='base_olids_concept')
}}

/*
CONCEPT Base View
OLIDS terminology concepts from CONCEPT source.
Passthrough view. The primary key column is now `concept_id` (UUID); the older
`id` / `lds_id` / `lds_business_key` / `lds_dataset_id` columns were dropped in
the latest OLIDS release.
*/

SELECT
    concept_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_is_deleted,
    lds_start_datetime
FROM {{ source('olids_terminology', 'CONCEPT') }}
