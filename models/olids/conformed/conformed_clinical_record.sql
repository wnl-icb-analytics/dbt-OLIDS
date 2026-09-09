{{ config(alias='clinical_record') }}

-- Grain: one retained clinical source record, with expanded observations read once.
WITH clinical_records AS (
    SELECT
        'observation'::VARCHAR(32) AS source_record_type,
        id AS source_record_id,
        person_id,
        patient_id,
        encounter_id,
        clinical_effective_date AS clinical_record_date,
        date_precision_source_code AS clinical_date_precision_code,
        date_precision_source_display AS clinical_date_precision_name,
        date_recorded,
        source_code,
        source_display AS source_code_name,
        source_system AS source_coding_system,
        mapped_concept_code AS mapped_code,
        mapped_concept_display AS mapped_code_name,
        target_system AS mapped_coding_system,
        result_value,
        result_date,
        result_unit_source_code,
        result_unit_source_display AS result_unit_source_name,
        allergy_medication_name AS medication_name,
        provider_organisation_id,
        publisher_organisation_id,
        publisher_organisation_code,
        practitioner_id,
        source_extraction_date
    FROM {{ ref('conformed_observation') }}
    WHERE NOT COALESCE(lds_is_deleted, FALSE)

    UNION ALL

    SELECT
        'medication_order'::VARCHAR(32) AS source_record_type,
        id AS source_record_id,
        person_id,
        patient_id,
        encounter_id,
        clinical_effective_date AS clinical_record_date,
        date_precision_source_code AS clinical_date_precision_code,
        date_precision_source_display AS clinical_date_precision_name,
        date_recorded,
        source_code,
        source_display AS source_code_name,
        source_system AS source_coding_system,
        mapped_concept_code AS mapped_code,
        mapped_concept_display AS mapped_code_name,
        target_system AS mapped_coding_system,
        NULL::FLOAT AS result_value,
        NULL::DATE AS result_date,
        NULL::VARCHAR AS result_unit_source_code,
        NULL::VARCHAR AS result_unit_source_name,
        medication_name,
        provider_organisation_id,
        publisher_organisation_id,
        publisher_organisation_code,
        practitioner_id,
        source_extraction_date
    FROM {{ ref('conformed_medication_order') }}
    WHERE NOT COALESCE(lds_is_deleted, FALSE)

    UNION ALL

    SELECT
        'medication_statement'::VARCHAR(32) AS source_record_type,
        id AS source_record_id,
        person_id,
        patient_id,
        encounter_id,
        clinical_effective_date AS clinical_record_date,
        date_precision_source_code AS clinical_date_precision_code,
        date_precision_source_display AS clinical_date_precision_name,
        date_recorded,
        source_code,
        source_display AS source_code_name,
        source_system AS source_coding_system,
        mapped_concept_code AS mapped_code,
        mapped_concept_display AS mapped_code_name,
        target_system AS mapped_coding_system,
        NULL::FLOAT AS result_value,
        NULL::DATE AS result_date,
        NULL::VARCHAR AS result_unit_source_code,
        NULL::VARCHAR AS result_unit_source_name,
        medication_name,
        provider_organisation_id,
        publisher_organisation_id,
        publisher_organisation_code,
        practitioner_id,
        source_extraction_date
    FROM {{ ref('conformed_medication_statement') }}
    WHERE NOT COALESCE(lds_is_deleted, FALSE)
)
SELECT
    {{ olids_clinical_record_id('c.source_record_type', 'c.source_record_id') }} AS clinical_record_id,
    c.source_record_type,
    c.source_record_id,
    c.person_id,
    c.patient_id,
    p.sk_patient_id,
    c.encounter_id,
    c.clinical_record_date,
    c.clinical_date_precision_code,
    c.clinical_date_precision_name,
    c.date_recorded,
    c.source_code,
    c.source_code_name,
    c.source_coding_system,
    c.mapped_code,
    c.mapped_code_name,
    c.mapped_coding_system,
    c.result_value,
    c.result_date,
    c.result_unit_source_code,
    c.result_unit_source_name,
    c.medication_name,
    c.provider_organisation_id,
    c.publisher_organisation_id,
    c.publisher_organisation_code,
    c.practitioner_id,
    c.source_extraction_date,
    provider.organisation_code AS provider_organisation_code,
    provider.assigning_authority_code AS provider_code_authority,
    provider.name AS provider_organisation_name,
    publisher.name AS publisher_organisation_name
FROM clinical_records AS c
LEFT JOIN {{ ref('conformed_patient') }} AS p
    ON c.patient_id = p.id
    AND c.person_id = p.person_id
LEFT JOIN {{ ref('conformed_organisation') }} AS provider
    ON c.provider_organisation_id = provider.id AND NOT COALESCE(provider.lds_is_deleted, FALSE)
LEFT JOIN {{ ref('conformed_organisation') }} AS publisher
    ON c.publisher_organisation_id = publisher.id AND NOT COALESCE(publisher.lds_is_deleted, FALSE)
