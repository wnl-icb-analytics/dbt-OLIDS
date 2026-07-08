-- compare_legacy_row_counts
-- New-feed row counts vs legacy Data_Store_OLIDS, one row per comparable table pair.
-- Attributable tables restrict the NCL-wide legacy side to practices present in the
-- new feed (the new feed carries ~15 practices). Non-attributable pairs use full counts.
-- Legacy-only tables (no new equivalent) are shown as INFO so the gap is visible.

WITH counts AS (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ALLERGY_INTOLERANCE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."ALLERGY_INTOLERANCE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'APPOINTMENT' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."APPOINTMENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."APPOINTMENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."APPOINTMENT_PRACTITIONER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."APPOINTMENT_PRACTITIONER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'DIAGNOSTIC_ORDER' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."DIAGNOSTIC_ORDER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."DIAGNOSTIC_ORDER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'ENCOUNTER' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ENCOUNTER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."ENCOUNTER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'EPISODE_OF_CARE' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."EPISODE_OF_CARE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."EPISODE_OF_CARE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'MEDICATION_ORDER' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."MEDICATION_ORDER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."MEDICATION_ORDER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'MEDICATION_STATEMENT' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."MEDICATION_STATEMENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."MEDICATION_STATEMENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'OBSERVATION' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."OBSERVATION" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."OBSERVATION" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'ORGANISATION' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."ORGANISATION" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."ORGANISATION" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PATIENT' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PATIENT_ADDRESS' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_ADDRESS" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_ADDRESS" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PATIENT_CONTACT' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_CONTACT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_CONTACT" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PATIENT_UPRN' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_UPRN" WHERE publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_UPRN" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    -- legacy PRACTITIONER tables have no publisher_organisation_code; full counts, INFO only
    SELECT 'PRACTITIONER' AS table_name, 'full population (legacy not practice-scoped)' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PRACTITIONER" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PRACTITIONER" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        TRUE AS is_info

    UNION ALL

    SELECT 'PRACTITIONER_IN_ROLE' AS table_name, 'full population (legacy not practice-scoped)' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PRACTITIONER_IN_ROLE" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PRACTITIONER_IN_ROLE" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        TRUE AS is_info

    UNION ALL

    SELECT 'PROCEDURE_REQUEST' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PROCEDURE_REQUEST" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PROCEDURE_REQUEST" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'REFERRAL_REQUEST' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."REFERRAL_REQUEST" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."REFERRAL_REQUEST" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'SCHEDULE' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."SCHEDULE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."SCHEDULE" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'SCHEDULE_PRACTITIONER' AS table_name, 'new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."SCHEDULE_PRACTITIONER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."SCHEDULE_PRACTITIONER" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'LOCATION' AS table_name, 'legacy restricted to new-feed practices' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."LOCATION" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."LOCATION" WHERE lds_is_deleted = FALSE AND publisher_organisation_code IN (
    SELECT DISTINCT publisher_organisation_code
    FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    )) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'CONCEPT' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."CONCEPT") AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_TERMINOLOGY"."CONCEPT" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'CONCEPT_MAP' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."CONCEPT_MAP") AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'EMIS_CLINICAL_CODE' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."EMIS_CLINICAL_CODE") AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."REFERENCE"."PRIMARY_CARE_EMIS_CODING_CLINICAL_CODE" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'EMIS_DRUG_CODE' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."EMIS_DRUG_CODE") AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."REFERENCE"."PRIMARY_CARE_EMIS_CODING_DRUG_CODE" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'POSTCODE_HASH' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."POSTCODE_HASH") AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."REFERENCE"."POSTCODE_HASH" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PERSON' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PERSON" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PERSON" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'PATIENT_PERSON' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."PATIENT_PERSON" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PATIENT_PERSON" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'NATIONAL_DATA_OPT_OUT' AS table_name, 'full population' AS test_subject,
        (SELECT COUNT(*) FROM {TARGET_DATABASE}.OLIDS_EXPERIMENTAL_LANDING."NATIONAL_DATA_OPT_OUT" WHERE lds_is_deleted = FALSE) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."NDOO_MASKED"."PATIENT_HASH" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        FALSE AS is_info

    UNION ALL

    SELECT 'LOCATION_CONTACT' AS table_name, 'legacy-only table' AS test_subject,
        CAST(NULL AS NUMBER) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."LOCATION_CONTACT" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        TRUE AS is_info

    UNION ALL

    SELECT 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE' AS table_name, 'legacy-only table' AS test_subject,
        CAST(NULL AS NUMBER) AS new_rows,
        (SELECT COUNT(*) FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" WHERE lds_is_deleted = FALSE) AS legacy_rows,
        TRUE AS is_info
),

metrics AS (
    SELECT
        table_name,
        test_subject,
        new_rows,
        legacy_rows,
        is_info,
        ROUND(new_rows / NULLIF(legacy_rows, 0) * 100, 1) AS pct_of_legacy
    FROM counts
)

SELECT
    'compare_legacy_row_counts' AS check_name,
    table_name,
    test_subject,
    CASE
        WHEN is_info THEN 'INFO'
        -- empty on both sides proves nothing (e.g. before entitlements land)
        WHEN COALESCE(new_rows, 0) = 0 AND COALESCE(legacy_rows, 0) = 0 THEN 'INFO'
        WHEN legacy_rows > 0 AND COALESCE(new_rows, 0) = 0 THEN 'FAIL'
        WHEN pct_of_legacy < 80 OR pct_of_legacy > 120 THEN 'WARN'
        ELSE 'PASS'
    END AS status,
    CASE WHEN is_info THEN NULL ELSE pct_of_legacy END AS metric_value,
    CASE WHEN is_info THEN NULL ELSE '80-120% of legacy' END AS threshold,
    new_rows,
    legacy_rows,
    pct_of_legacy
FROM metrics
ORDER BY
    CASE WHEN is_info THEN 2
         WHEN legacy_rows > 0 AND COALESCE(new_rows, 0) = 0 THEN 0
         WHEN pct_of_legacy < 80 OR pct_of_legacy > 120 THEN 0
         ELSE 1 END,
    table_name;
