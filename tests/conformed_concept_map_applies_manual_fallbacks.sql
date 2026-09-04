WITH expected AS (
    SELECT
        column1::VARCHAR AS source_system,
        column2::VARCHAR AS source_code,
        column3::VARCHAR AS target_code
    FROM
        VALUES
        ('EMIS_RegistrationStatus_cs', 'Deceased', '725951000000101'),
        ('EMIS_and_TPP_MedicationStatement_cs', 'Acute', '1217105006'),
        ('EMIS_and_TPP_MedicationStatement_cs', 'Automatic', '1217105006'),
        ('EMIS_and_TPP_MedicationStatement_cs', 'Repeat', '182918009'),
        (
            'EMIS_and_TPP_MedicationStatement_cs',
            'Repeat Dispensing',
            '182918009'
        )
),

eligible AS (
    SELECT
        source.concept_id AS source_concept_id,
        expected.target_code
    FROM expected
    INNER JOIN {{ ref('conformed_concept') }} AS source
        ON
            expected.source_system = source.system
            AND expected.source_code = source.code
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM {{ ref('conformed_concept_map_history') }} AS candidate
            WHERE
                candidate.source_concept_id = source.concept_id
                AND candidate.concept_map_name <> 'EMIS_to_READ_or_Local_cm'
                AND candidate.target_system <> 'EMIS_READ_Local_cs'
                AND candidate.target_code IS NOT NULL
                AND NOT (
                    candidate.target_system = 'snomed_info_sct'
                    AND candidate.target_code = '138875005'
                )
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('conformed_emis_clinical_code') }} AS emis
            WHERE
                emis.concept_id = source.concept_id
                AND emis.snomed_ct_concept_id::VARCHAR <> '138875005'
        )
)

SELECT eligible.source_concept_id
FROM eligible
LEFT JOIN {{ ref('conformed_concept_map') }} AS selected
    ON eligible.source_concept_id = selected.source_concept_id
WHERE
    selected.mapping_name IS DISTINCT FROM 'manual-backfill'
    OR selected.mapped_concept_code IS DISTINCT FROM eligible.target_code
