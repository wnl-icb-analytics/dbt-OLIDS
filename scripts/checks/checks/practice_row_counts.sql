-- practice_row_counts
-- New-feed row counts by practice over the 20 attributable landing tables, plus a
-- per-table TOTAL row (test_subject = 'ALL_PRACTICES'). Informational only.

WITH per_practice AS (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."ALLERGY_INTOLERANCE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."APPOINTMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."APPOINTMENT_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'DIAGNOSTIC_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."DIAGNOSTIC_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'ENCOUNTER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."ENCOUNTER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'EPISODE_OF_CARE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."EPISODE_OF_CARE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."MEDICATION_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_STATEMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."MEDICATION_STATEMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'OBSERVATION' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."OBSERVATION"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'ORGANISATION' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."ORGANISATION"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_ADDRESS' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PATIENT_ADDRESS"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_CONTACT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PATIENT_CONTACT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_UPRN' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PATIENT_UPRN"
    WHERE publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PRACTITIONER_IN_ROLE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PRACTITIONER_IN_ROLE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PROCEDURE_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."PROCEDURE_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'REFERRAL_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."REFERRAL_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."SCHEDULE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS row_count
    FROM {TARGET_DATABASE}.LANDING."SCHEDULE_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code
),

combined AS (
    SELECT table_name, practice_code, row_count FROM per_practice
    UNION ALL
    SELECT table_name, 'ALL_PRACTICES' AS practice_code, SUM(row_count) AS row_count
    FROM per_practice
    GROUP BY table_name
)

SELECT
    'practice_row_counts' AS check_name,
    table_name,
    practice_code AS test_subject,
    'INFO' AS status,
    row_count AS metric_value,
    NULL AS threshold
FROM combined
ORDER BY table_name, (practice_code = 'ALL_PRACTICES') DESC, practice_code;
