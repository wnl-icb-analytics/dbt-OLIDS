/*
CONCEPT_MAP Base View

WORKAROUND: the CONCEPT_MAP table that shipped with the latest
Data_Store_OLIDS release is in a broken state. Until upstream is fixed we
read from the previously-stable copy that this project produced into
DATA_LAB_OLIDS_NCL.OLIDS.CONCEPT_MAP (~1.99M rows), and map its columns to
the new contract (mapped_item_id / source_concept_id / target_concept_id /
lds_start_datetime).

When upstream is fixed, revert to:
    FROM {{ source('olids_terminology', 'CONCEPT_MAP') }}
and select the new column names directly.
*/

SELECT
    id AS mapped_item_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_code_id AS source_concept_id,
    source_system,
    source_code,
    source_display,
    target_code_id AS target_concept_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    NULL::DATE AS lakehouse_date_processed,
    NULL::TIMESTAMP_NTZ AS lakehouse_datetime_updated,
    FALSE AS lds_is_deleted,
    lds_start_date_time AS lds_start_datetime
FROM DATA_LAB_OLIDS_NCL.OLIDS.CONCEPT_MAP
