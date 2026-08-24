-- compare_legacy_practice_counts
-- Per-practice new vs legacy row counts for the 18 tables attributable on both feeds
-- (legacy PRACTITIONER and PRACTITIONER_IN_ROLE carry no publisher_organisation_code).
-- FULL OUTER JOIN on practice code: practices absent from the new feed appear with
-- legacy_rows vs 0 as INFO (coverage gap is expected during rollout); practices in the
-- new feed are judged against the 80-120% threshold.

WITH new_counts AS (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."ALLERGY_INTOLERANCE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."APPOINTMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."APPOINTMENT_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'DIAGNOSTIC_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."DIAGNOSTIC_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'ENCOUNTER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."ENCOUNTER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'EPISODE_OF_CARE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."EPISODE_OF_CARE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'EPISODE_OF_CARE_V2' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."EPISODE_OF_CARE_V2"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."MEDICATION_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_STATEMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."MEDICATION_STATEMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'OBSERVATION' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."OBSERVATION"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_ADDRESS' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."PATIENT_ADDRESS"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_CONTACT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."PATIENT_CONTACT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_UPRN' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."PATIENT_UPRN"
    WHERE publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code



    UNION ALL

    SELECT 'PROCEDURE_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."PROCEDURE_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'REFERRAL_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."REFERRAL_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."SCHEDULE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS new_rows
    FROM {TARGET_DATABASE}.LANDING."SCHEDULE_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code
),

legacy_counts AS (
    SELECT 'ALLERGY_INTOLERANCE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."ALLERGY_INTOLERANCE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."APPOINTMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'APPOINTMENT_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."APPOINTMENT_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'DIAGNOSTIC_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."DIAGNOSTIC_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'ENCOUNTER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."ENCOUNTER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'EPISODE_OF_CARE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."EPISODE_OF_CARE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'EPISODE_OF_CARE_V2' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."EPISODE_OF_CARE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_ORDER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."MEDICATION_ORDER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'MEDICATION_STATEMENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."MEDICATION_STATEMENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'OBSERVATION' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."OBSERVATION"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_ADDRESS' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_ADDRESS"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_CONTACT' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_CONTACT"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'PATIENT_UPRN' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT_UPRN"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code



    UNION ALL

    SELECT 'PROCEDURE_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."PROCEDURE_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'REFERRAL_REQUEST' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."REFERRAL_REQUEST"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."SCHEDULE"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code

    UNION ALL

    SELECT 'SCHEDULE_PRACTITIONER' AS table_name, publisher_organisation_code AS practice_code, COUNT(*) AS legacy_rows
    FROM "Data_Store_OLIDS"."OLIDS_COMMON"."SCHEDULE_PRACTITIONER"
    WHERE lds_is_deleted = FALSE AND publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code
),

joined AS (
    SELECT
        COALESCE(n.table_name, l.table_name) AS table_name,
        COALESCE(n.practice_code, l.practice_code) AS practice_code,
        COALESCE(n.new_rows, 0) AS new_rows,
        COALESCE(l.legacy_rows, 0) AS legacy_rows
    FROM new_counts n
    FULL OUTER JOIN legacy_counts l
        ON n.table_name = l.table_name AND n.practice_code = l.practice_code
),

-- practices with any rows in the new PATIENT feed; only these are judged
new_feed_practices AS (
    SELECT DISTINCT publisher_organisation_code AS practice_code
    FROM {TARGET_DATABASE}.LANDING."PATIENT"
    WHERE publisher_organisation_code IS NOT NULL
),

metrics AS (
    SELECT
        j.table_name,
        j.practice_code,
        j.new_rows,
        j.legacy_rows,
        ROUND(j.new_rows / NULLIF(j.legacy_rows, 0) * 100, 1) AS pct_of_legacy,
        nfp.practice_code IS NOT NULL AS in_new_feed
    FROM joined j
    LEFT JOIN new_feed_practices nfp
        ON j.practice_code = nfp.practice_code
)

SELECT
    'compare_legacy_practice_counts' AS check_name,
    table_name,
    practice_code AS test_subject,
    CASE
        -- practices not yet in the new feed are expected during rollout, not failures
        WHEN NOT in_new_feed THEN 'INFO'
        WHEN legacy_rows > 0 AND new_rows = 0 THEN 'FAIL'
        WHEN pct_of_legacy < 80 OR pct_of_legacy > 120 THEN 'WARN'
        ELSE 'PASS'
    END AS status,
    pct_of_legacy AS metric_value,
    '80-120% of legacy (new-feed practices only)' AS threshold,
    new_rows,
    legacy_rows,
    pct_of_legacy
FROM metrics
ORDER BY table_name, practice_code;
