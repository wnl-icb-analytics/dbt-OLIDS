/*
================================================================================
GP REGISTRATION PASS - OLIDS Definitive Registration Count Methodology
================================================================================

PURPOSE:
    Calculate the definitive count of registered patients per GP practice from
    OLIDS (One London Integrated Data Service) primary care data.

AUDIENCE:
    London ICBs, One London team, and anyone needing to validate OLIDS 
    registration data against other sources (PDS, EMIS extracts, etc.)

OUTPUT:
    One row per practice with the best registered patient count as of a given
    snapshot date. This represents patients with an active Regular GP
    registration episode who are alive and meet all data quality criteria.

METHODOLOGY OVERVIEW:
    1. Start with all Episode of Care records (GP registration episodes)
    2. Apply patient eligibility filters (exclude sensitive/test patients)
    3. Apply data quality filters (exclude soft-deleted, invalid records)
    4. Filter to Regular registration type only
    5. Filter to active episodes as of snapshot date
    6. Exclude deceased patients
    7. Deduplicate by person (not patient) to handle NHS number mergers
    8. Count distinct persons per practice

DATA SOURCES:
    - Data_Store_OLIDS_Alpha.OLIDS_COMMON.EPISODE_OF_CARE
    - Data_Store_OLIDS_Alpha.OLIDS_MASKED.PATIENT
    - Data_Store_OLIDS_Alpha.OLIDS_COMMON.PATIENT_PERSON
    - Data_Store_OLIDS_Alpha.OLIDS_COMMON.ORGANISATION
    - Data_Store_OLIDS_Clinical_Validation.OLIDS_TERMINOLOGY.CONCEPT_MAP

VERSION: 1.0
DATE: 2026-02-02
AUTHOR: NCL ICB Analytics Team

================================================================================
CONFIGURATION
================================================================================
*/

-- Set your Snowflake context
-- Adjust role and warehouse as appropriate for your ICB
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";
USE DATABASE "Data_Store_OLIDS_Alpha";
USE WAREHOUSE "WH_NCL_OLIDS_M";

/*
================================================================================
QUERY START
================================================================================
*/

WITH 

/*
================================================================================
STEP 1: DEFINE SNAPSHOT DATE
================================================================================
The snapshot date determines which registrations are considered "active".
Change this to match your comparison dataset (e.g., EMIS extract date, PDS date).
*/

config AS (
    SELECT DATE '2025-11-04' AS snapshot_date  -- Adjust to your target date
),

/*
================================================================================
STEP 2: PATIENT ELIGIBILITY
================================================================================
Not all patient records should be included in registration counts.
We exclude:
  - is_spine_sensitive = TRUE : Patients flagged as sensitive in Spine
  - is_confidential = TRUE    : Patients with confidentiality flags
  - is_dummy_patient = TRUE   : Test/dummy patient records
  - sk_patient_id IS NULL     : Records without a valid pseudonymised ID

These exclusions align with NHS data protection requirements and ensure
we only count real, non-sensitive patients.
*/

eligible_patients AS (
    SELECT
        id AS patient_id,
        sk_patient_id,
        death_year,
        death_month
    FROM "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
    WHERE sk_patient_id IS NOT NULL           -- Must have valid pseudo ID
        AND is_spine_sensitive = FALSE        -- Not Spine sensitive
        AND is_confidential = FALSE           -- Not confidential
        AND is_dummy_patient = FALSE          -- Not a test patient
),

/*
================================================================================
STEP 3: DECEASED PATIENT HANDLING
================================================================================
We exclude patients who were deceased on or before the snapshot date.

OLIDS provides death_year and death_month but not exact date. We approximate
the death date as the midpoint of the death month. If only death_year is
available, we use July 1st of that year.

This approximation is conservative - a patient recorded as dying in November
would have an approximate death date around November 15th.
*/

patient_death_dates AS (
    SELECT
        patient_id,
        sk_patient_id,
        death_year,
        death_month,
        death_year IS NOT NULL AS is_deceased,
        CASE
            -- If we have both year and month, use midpoint of month
            WHEN death_year IS NOT NULL AND death_month IS NOT NULL
                THEN DATEADD(
                    DAY,
                    FLOOR(DAY(LAST_DAY(TO_DATE(death_year || '-' || LPAD(death_month, 2, '0') || '-01'))) / 2),
                    TO_DATE(death_year || '-' || LPAD(death_month, 2, '0') || '-01')
                )
            -- If only year, use July 1st (midpoint of year)
            WHEN death_year IS NOT NULL
                THEN TO_DATE(death_year || '-07-01')
            ELSE NULL
        END AS death_date_approx
    FROM eligible_patients
),

/*
================================================================================
STEP 4: PATIENT TO PERSON MAPPING
================================================================================
A single person may have multiple patient records (e.g., NHS number changes,
mergers, or registrations at different practices). The PATIENT_PERSON table
provides the canonical person_id for each patient_id.

When counting registrations, we count PERSONS not PATIENTS to avoid
double-counting individuals who have undergone NHS number mergers.
*/

patient_to_person AS (
    SELECT
        patient_id,
        person_id
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PATIENT_PERSON
    WHERE patient_id IS NOT NULL
        AND person_id IS NOT NULL
),

/*
================================================================================
STEP 5: EPISODE TYPE AND STATUS MAPPINGS
================================================================================
Episode types and statuses in EPISODE_OF_CARE are stored as concept IDs.
The CONCEPT_MAP table provides source_code mappings for these concepts.

Registration types include:
  - Regular              : Standard GP registration (what we want)
  - Temporary            : Short-term registration (excluded)
  - Emergency            : Emergency access (excluded)
  - Immediately Necessary: Urgent care (excluded)
  - Private              : Private patients (excluded)
  - Pre Registration     : Not yet registered (excluded)
  - Others...            : Various clinical services (excluded)

Only "Regular" registrations count towards official list size.

Episode statuses include:
  - Active, Left, etc.
  - We exclude "Left" status with no end date (data quality issue)
*/

episode_type_map AS (
    SELECT source_code_id, source_code
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_TERMINOLOGY.CONCEPT_MAP
    WHERE source_code = 'Regular'
),

episode_status_map AS (
    SELECT source_code_id, source_code
    FROM "Data_Store_OLIDS_Clinical_Validation".OLIDS_TERMINOLOGY.CONCEPT_MAP
    WHERE source_code = 'Left'
),

/*
================================================================================
STEP 6: FILTER REGISTRATION EPISODES
================================================================================
From Episode of Care, we select only records that meet ALL criteria:

DATA QUALITY FILTERS:
  - lds_is_deleted = FALSE       : Not soft-deleted
  - lds_start_date_time IS NOT NULL : Has valid processing timestamp
  - episode_of_care_start_date IS NOT NULL : Has valid start date
  - patient_id IS NOT NULL       : Linked to a patient
  - organisation_id IS NOT NULL  : Linked to a practice

REGISTRATION TYPE FILTER:
  - episode_type_source_concept_id matches "Regular" concept

EPISODE STATUS FILTER:
  - Exclude episodes where status = "Left" but end_date is NULL
    (This is a data quality issue: marked as left but never closed)

TEMPORAL FILTERS (as of snapshot date):
  - Episode started on or before snapshot date
  - Episode not ended before snapshot date (end_date is NULL or > snapshot)

DECEASED FILTER:
  - Patient not deceased on or before snapshot date
*/

filtered_episodes AS (
    SELECT
        eoc.id AS episode_id,
        eoc.patient_id,
        ptp.person_id,
        pdd.sk_patient_id,
        eoc.organisation_id,
        eoc.record_owner_organisation_code AS practice_code,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE eoc
    CROSS JOIN config

    -- Join to eligible patients (applies patient-level filters)
    INNER JOIN patient_death_dates pdd
        ON eoc.patient_id = pdd.patient_id

    -- Join to get canonical person_id
    INNER JOIN patient_to_person ptp
        ON eoc.patient_id = ptp.patient_id

    -- Join to verify episode type is Regular
    INNER JOIN episode_type_map etm
        ON eoc.episode_type_source_concept_id = etm.source_code_id

    -- Left join to check for "Left" status
    LEFT JOIN episode_status_map esm
        ON eoc.episode_status_source_concept_id = esm.source_code_id

    WHERE
        -- Data quality filters
        COALESCE(eoc.lds_is_deleted, FALSE) = FALSE
        AND eoc.lds_start_date_time IS NOT NULL
        AND eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.patient_id IS NOT NULL
        AND eoc.organisation_id IS NOT NULL

        -- Exclude "Left" episodes with no end date (data quality issue)
        -- These are registrations marked as ended but never properly closed
        AND NOT (
            esm.source_code IS NOT NULL  -- Status is "Left"
            AND eoc.episode_of_care_end_date IS NULL
        )

        -- Episode active as of snapshot date
        AND eoc.episode_of_care_start_date <= config.snapshot_date
        AND (
            eoc.episode_of_care_end_date IS NULL
            OR eoc.episode_of_care_end_date > config.snapshot_date
        )

        -- Patient alive as of snapshot date
        AND (
            pdd.is_deceased = FALSE
            OR pdd.death_date_approx IS NULL
            OR pdd.death_date_approx > config.snapshot_date
        )
),

/*
================================================================================
STEP 7: DEDUPLICATE BY PERSON AND PRACTICE
================================================================================
A person may have multiple episode records at the same practice (e.g.,
re-registration, data corrections). We keep only one record per person
per practice, taking the most recent episode.

This ensures each person is counted exactly once per practice.
*/

deduplicated_registrations AS (
    SELECT
        person_id,
        sk_patient_id,
        practice_code,
        organisation_id,
        episode_of_care_start_date
    FROM filtered_episodes
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, organisation_id
        ORDER BY episode_of_care_start_date DESC, episode_id DESC
    ) = 1
),

/*
================================================================================
STEP 8: GET PRACTICE DETAILS
================================================================================
Join to Organisation table to get practice names for the output.
*/

practice_details AS (
    SELECT
        id AS organisation_id,
        organisation_code AS practice_code,
        name AS practice_name
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION
    WHERE organisation_code IS NOT NULL
),

/*
================================================================================
STEP 9: AGGREGATE TO PRACTICE LEVEL
================================================================================
Count distinct persons per practice. This is the definitive registration count.
*/

practice_registration_counts AS (
    SELECT
        dr.practice_code,
        pd.practice_name,
        COUNT(DISTINCT dr.person_id) AS registered_patient_count,
        (SELECT snapshot_date FROM config) AS snapshot_date
    FROM deduplicated_registrations dr
    LEFT JOIN practice_details pd
        ON dr.organisation_id = pd.organisation_id
    WHERE dr.practice_code IS NOT NULL
    GROUP BY dr.practice_code, pd.practice_name
)

/*
================================================================================
FINAL OUTPUT
================================================================================
One row per practice with the definitive registered patient count.

Columns:
  - practice_code           : ODS code for the practice
  - practice_name           : Practice name from OLIDS
  - registered_patient_count: Count of distinct persons with active Regular
                              registration as of snapshot date
  - snapshot_date           : The date used for active registration calculation

To filter to your ICB's practices, join to your organisation reference data
or add a WHERE clause filtering on practice_code patterns.

Example for NCL practices only:
  INNER JOIN "Dictionary"."dbo"."OrganisationMatrixPracticeView" org
      ON practice_code = org."PracticeCode"
      AND org."STPCode" = 'QMJ'
*/

SELECT
    practice_code,
    practice_name,
    registered_patient_count,
    snapshot_date
FROM practice_registration_counts
ORDER BY registered_patient_count DESC, practice_code;

/*
================================================================================
VALIDATION NOTES
================================================================================

EXPECTED BEHAVIOUR:
- Regular registration type only (no Temporary, Emergency, etc.)
- Persons counted, not patients (handles NHS number mergers)
- Sensitive/confidential/dummy patients excluded
- Deceased patients excluded as of snapshot date
- Soft-deleted records excluded
- "Left" episodes with no end date excluded (data quality fix)

COMPARISON TO OTHER SOURCES:
When comparing to PDS or EMIS list sizes:
- Small differences (<2%) are expected due to timing/data latency
- Larger differences may indicate:
  - Different snapshot dates
  - Different filtering logic
  - Data quality issues in either source
  - Practices not fully onboarded to OLIDS

ADAPTING FOR YOUR ICB:
1. Change the USE ROLE/WAREHOUSE statements to match your access
2. Change the snapshot_date in the config CTE to match your comparison dataset
3. Add ICB-specific practice filters if needed (see example in final SELECT)

================================================================================
*/
