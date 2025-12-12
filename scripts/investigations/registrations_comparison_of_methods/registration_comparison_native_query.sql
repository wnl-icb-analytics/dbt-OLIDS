-- Compare registration counts between PDS registry and EMIS static extract
-- Also includes OLIDS REGULAR registrations for comparison (similar to original script)
-- Using reference table: DATA_LAB_OLIDS_UAT.REFERENCE.EMIS_LIST_SIZE_2025_11_04
-- Target Date: 2025-11-04
-- NCL Configuration
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";
USE DATABASE "Data_Store_OLIDS_Alpha";
USE WAREHOUSE "WH_NCL_OLIDS_M";
WITH patient_death_date AS (
    SELECT
        ID AS PATIENT_ID,
        DEATH_YEAR,
        DEATH_MONTH,
        DEATH_YEAR IS NOT NULL AS IS_DECEASED,
        CASE
            WHEN DEATH_YEAR IS NOT NULL
            AND DEATH_MONTH IS NOT NULL THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                TO_VARCHAR(DEATH_YEAR) || '-' || TO_VARCHAR(DEATH_MONTH) || '-01'
                            )
                        )
                    ) / 2
                ),
                TO_DATE(
                    TO_VARCHAR(DEATH_YEAR) || '-' || TO_VARCHAR(DEATH_MONTH) || '-01'
                )
            )
        END AS DEATH_DATE_APPROX
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
),
patient_episodes_with_type AS (
    SELECT
        EOC.PATIENT_ID,
        EOC.ORGANISATION_ID,
        P.SK_PATIENT_ID,
        EOC.EPISODE_OF_CARE_START_DATE,
        EOC.EPISODE_OF_CARE_END_DATE,
        EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID,
        COALESCE(SC.DISPLAY, 'Unknown Type') AS EPISODE_TYPE_DISPLAY
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE EOC
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT P ON EOC.PATIENT_ID = P.ID
        LEFT JOIN patient_death_date PDD ON EOC.PATIENT_ID = PDD.PATIENT_ID
        LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT SC ON EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID = SC.ID
    WHERE
        EOC.EPISODE_OF_CARE_START_DATE IS NOT NULL
        AND EOC.PATIENT_ID IS NOT NULL
        AND EOC.ORGANISATION_ID IS NOT NULL
        AND P.SK_PATIENT_ID IS NOT NULL
        -- Episode active on target date
        AND EOC.EPISODE_OF_CARE_START_DATE <= DATE '2025-11-04'
        AND (
            EOC.EPISODE_OF_CARE_END_DATE IS NULL
            OR EOC.EPISODE_OF_CARE_END_DATE > DATE '2025-11-04'
        )
        -- Patient alive on target date
        AND (
            PDD.IS_DECEASED = FALSE
            OR PDD.DEATH_DATE_APPROX IS NULL
            OR PDD.DEATH_DATE_APPROX > DATE '2025-11-04'
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY P.SK_PATIENT_ID,
        EOC.ORGANISATION_ID
        ORDER BY
            EOC.EPISODE_OF_CARE_START_DATE DESC,
            EOC.ID
    ) = 1
),
olids_by_type AS (
    SELECT
        O.ORGANISATION_CODE AS PRACTICE_CODE,
        PE.EPISODE_TYPE_DISPLAY,
        COUNT(DISTINCT PE.SK_PATIENT_ID) AS PATIENT_COUNT
    FROM
        patient_episodes_with_type PE
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION O ON PE.ORGANISATION_ID = O.ID
    WHERE
        O.ORGANISATION_CODE IS NOT NULL
    GROUP BY
        O.ORGANISATION_CODE,
        PE.EPISODE_TYPE_DISPLAY
),
olids_type_pivot AS (
    SELECT
        PRACTICE_CODE,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY = 'Regular' THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_REGULAR
    FROM
        olids_by_type
    GROUP BY
        PRACTICE_CODE
),
emis_reference AS (
    SELECT
        CODE AS PRACTICE_CODE,
        GP_PRACTICE AS PRACTICE_NAME,
        LIST_SIZE AS EMIS_LIST_SIZE
    FROM
        DATA_LAB_OLIDS_UAT.REFERENCE.EMIS_LIST_SIZE_2025_11_04
    WHERE
        CODE IS NOT NULL
),
pds_registrations AS (
    SELECT
        REG."Primary Care Provider" AS PRACTICE_CODE,
        Prac."Organisation_Name" AS PRACTICE_NAME,
        ICB."Organisation_Name" AS ICB_NAME,
        COUNT(*) AS PDS_UNMERGED_PERSONS,
        COUNT(
            DISTINCT COALESCE(
                MERG."Pseudo Superseded NHS Number",
                REG."Pseudo NHS Number"
            )
        ) AS PDS_MERGED_PERSONS
    FROM
        "Data_Store_Registries"."pds"."PDS_Patient_Care_Practice" REG
        LEFT JOIN "Data_Store_Registries"."pds"."PDS_Person_Merger" MERG ON REG."Pseudo NHS Number" = MERG."Pseudo NHS Number"
        LEFT JOIN "Data_Store_Registries"."pds"."PDS_Person" PER ON REG."Pseudo NHS Number" = PER."Pseudo NHS Number"
        AND PER."Person Business Effective From Date" <= COALESCE(
            REG."Primary Care Provider Business Effective To Date",
            DATE '9999-12-31'
        )
        AND COALESCE(
            PER."Person Business Effective To Date",
            DATE '9999-12-31'
        ) >= REG."Primary Care Provider Business Effective From Date"
        AND DATE '2025-11-04' BETWEEN PER."Person Business Effective From Date"
        AND COALESCE(
            PER."Person Business Effective To Date",
            DATE '9999-12-31'
        )
        LEFT JOIN "Data_Store_Registries"."pds"."PDS_Reason_For_Removal" REAS ON REG."Pseudo NHS Number" = REAS."Pseudo NHS Number"
        AND REAS."Reason for Removal Business Effective From Date" <= COALESCE(
            REG."Primary Care Provider Business Effective To Date",
            DATE '9999-12-31'
        )
        AND COALESCE(
            REAS."Reason for Removal Business Effective To Date",
            DATE '9999-12-31'
        ) >= REG."Primary Care Provider Business Effective From Date"
        AND DATE '2025-11-04' BETWEEN REAS."Reason for Removal Business Effective From Date"
        AND COALESCE(
            REAS."Reason for Removal Business Effective To Date",
            DATE '9999-12-31'
        )
        INNER JOIN "Dictionary"."dbo"."Organisation" Prac ON REG."Primary Care Provider" = Prac."Organisation_Code"
        INNER JOIN "Dictionary"."dbo"."Organisation" ICB ON Prac."SK_ParentOrg_ID" = ICB."SK_OrganisationID"
        AND ICB."Organisation_Code" = '93C'
        AND Prac."EndDate" IS NULL
    WHERE
        PER."Death Status" IS NULL
        AND PER."Date of Death" IS NULL
        AND REG."Pseudo NHS Number" IS NOT NULL
        AND DATE '2025-11-04' BETWEEN REG."Primary Care Provider Business Effective From Date"
        AND COALESCE(
            REG."Primary Care Provider Business Effective To Date",
            DATE '9999-12-31'
        )
        AND REAS."Reason for Removal" IS NULL
    GROUP BY
        REG."Primary Care Provider",
        Prac."Organisation_Name",
        ICB."Organisation_Name"
),
final_results AS (
    SELECT
        COALESCE(PDS.PRACTICE_CODE, EMIS.PRACTICE_CODE, OLIDS.PRACTICE_CODE) AS PRACTICE_CODE,
        COALESCE(PDS.PRACTICE_NAME, EMIS.PRACTICE_NAME) AS PRACTICE_NAME,
        PDS.ICB_NAME,
        PDS.PDS_MERGED_PERSONS,
        EMIS.EMIS_LIST_SIZE,
        OLIDS.TYPE_REGULAR AS OLIDS_REGULAR_REGISTRATIONS,
        (
            EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
        ) AS DIFFERENCE,
        ROUND(
            100.0 * (
                EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
            ) / NULLIF(PDS.PDS_MERGED_PERSONS, 0),
            2
        ) AS DIFFERENCE_PCT,
        CASE
            WHEN PDS.PDS_MERGED_PERSONS IS NULL THEN 'EMIS Only'
            WHEN EMIS.EMIS_LIST_SIZE IS NULL THEN 'PDS Only'
            WHEN ABS(
                100.0 * (
                    EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
                ) / NULLIF(PDS.PDS_MERGED_PERSONS, 0)
            ) >= 20 THEN 'Major Difference (±20%+)'
            WHEN ABS(
                100.0 * (
                    EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
                ) / NULLIF(PDS.PDS_MERGED_PERSONS, 0)
            ) >= 5 THEN 'Moderate Difference (5-20%)'
            WHEN ABS(
                100.0 * (
                    EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
                ) / NULLIF(PDS.PDS_MERGED_PERSONS, 0)
            ) >= 1 THEN 'Small Difference (1-5%)'
            WHEN ABS(
                100.0 * (
                    EMIS.EMIS_LIST_SIZE - PDS.PDS_MERGED_PERSONS
                ) / NULLIF(PDS.PDS_MERGED_PERSONS, 0)
            ) >= 0.5 THEN 'Good Match (0.5-1%)'
            ELSE 'Excellent Match (<0.5%)'
        END AS MATCH_CATEGORY,
        -- PDS vs OLIDS REGULAR comparison (similar to original script's TYPE_REGULAR_PCT_MATCH_PDS)
        ROUND(
            100.0 * OLIDS.TYPE_REGULAR / NULLIF(PDS.PDS_MERGED_PERSONS, 0),
            2
        ) AS OLIDS_REGULAR_PCT_OF_PDS
    FROM
        pds_registrations PDS
        FULL OUTER JOIN emis_reference EMIS ON PDS.PRACTICE_CODE = EMIS.PRACTICE_CODE
        LEFT JOIN olids_type_pivot OLIDS ON COALESCE(PDS.PRACTICE_CODE, EMIS.PRACTICE_CODE) = OLIDS.PRACTICE_CODE
)
SELECT
    *
FROM
    final_results
WHERE
    MATCH_CATEGORY != 'EMIS Only'
ORDER BY
    EMIS_LIST_SIZE DESC NULLS LAST,
    PRACTICE_CODE;