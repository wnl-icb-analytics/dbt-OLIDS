#!/usr/bin/env python3
"""Profile Synapse person reconciliation candidates.

Only aggregate counts are printed. No person, patient or sk values are emitted.
"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from snowflake_env import get_connection, load_env

PATIENT = 'OLIDS_ENGINEERING.SYNAPSE_STABLE.PATIENT'
PERSON = 'OLIDS_ENGINEERING.SYNAPSE_STABLE.PERSON'
RAW_PATIENT = '"Data_Store_OLIDS"."OLIDS_MASKED"."PATIENT"'

FORBIDDEN_SQL = {
    'alter', 'call', 'copy', 'create', 'delete', 'drop', 'grant', 'insert',
    'merge', 'put', 'remove', 'revoke', 'truncate', 'update', 'use',
}


SK_MULTIPLICITY_SQL = f"""
WITH sk_person_counts AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT person_id) AS person_count
    FROM {PATIENT}
    WHERE sk_patient_id IS NOT NULL
    GROUP BY sk_patient_id
),
bucketed AS (
    SELECT
        CASE
            WHEN person_count = 1 THEN '1 person'
            WHEN person_count = 2 THEN '2 persons'
            WHEN person_count = 3 THEN '3 persons'
            ELSE '4+ persons'
        END AS persons_per_sk,
        CASE
            WHEN person_count = 1 THEN 1
            WHEN person_count = 2 THEN 2
            WHEN person_count = 3 THEN 3
            ELSE 4
        END AS sort_order
    FROM sk_person_counts
)
SELECT
    persons_per_sk,
    COUNT(*) AS sk_count,
    ROUND(100 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_sks
FROM bucketed
GROUP BY persons_per_sk, sort_order
ORDER BY sort_order
"""

SK_MULTIPLICITY_SUMMARY_SQL = f"""
WITH sk_person_counts AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT person_id) AS person_count
    FROM {PATIENT}
    WHERE sk_patient_id IS NOT NULL
    GROUP BY sk_patient_id
)
SELECT
    COUNT(*) AS total_sks,
    COUNT_IF(person_count > 1) AS multi_person_sks,
    ROUND(100 * COUNT_IF(person_count > 1) / NULLIF(COUNT(*), 0), 2) AS pct_multi_person
FROM sk_person_counts
"""

DOB_CORROBORATION_SQL = f"""
WITH member_persons AS (
    SELECT DISTINCT
        pat.sk_patient_id,
        pat.person_id,
        per.birth_year,
        per.birth_month
    FROM {PATIENT} AS pat
    LEFT JOIN {PERSON} AS per
        ON pat.person_id = per.id
    WHERE pat.sk_patient_id IS NOT NULL
      AND pat.person_id IS NOT NULL
),
sk_summary AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT person_id) AS person_count,
        COUNT_IF(birth_year IS NULL OR birth_month IS NULL) AS missing_dob_persons,
        COUNT(DISTINCT IFF(
            birth_year IS NULL OR birth_month IS NULL,
            NULL,
            TO_VARCHAR(birth_year) || '-' || LPAD(TO_VARCHAR(birth_month), 2, '0')
        )) AS dob_count
    FROM member_persons
    GROUP BY sk_patient_id
),
classified AS (
    SELECT
        CASE
            WHEN missing_dob_persons > 0 THEN 'missing DOBs'
            WHEN dob_count = 1 THEN 'same birth_year+birth_month'
            ELSE 'differing birth_year+birth_month'
        END AS dob_class
    FROM sk_summary
    WHERE person_count > 1
)
SELECT
    dob_class,
    COUNT(*) AS sk_count,
    ROUND(100 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_multi_person_sks
FROM classified
GROUP BY dob_class
ORDER BY
    CASE dob_class
        WHEN 'same birth_year+birth_month' THEN 1
        WHEN 'differing birth_year+birth_month' THEN 2
        ELSE 3
    END
"""

REVERSE_SK_SQL = f"""
WITH person_sk_counts AS (
    SELECT
        person_id,
        COUNT(DISTINCT sk_patient_id) AS sk_count
    FROM {PATIENT}
    WHERE person_id IS NOT NULL
      AND sk_patient_id IS NOT NULL
    GROUP BY person_id
),
bucketed AS (
    SELECT
        CASE
            WHEN sk_count = 1 THEN '1 sk'
            WHEN sk_count = 2 THEN '2 sks'
            WHEN sk_count = 3 THEN '3 sks'
            ELSE '4+ sks'
        END AS sks_per_person,
        CASE
            WHEN sk_count = 1 THEN 1
            WHEN sk_count = 2 THEN 2
            WHEN sk_count = 3 THEN 3
            ELSE 4
        END AS sort_order
    FROM person_sk_counts
)
SELECT
    sks_per_person,
    COUNT(*) AS person_count,
    ROUND(100 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_persons
FROM bucketed
GROUP BY sks_per_person, sort_order
ORDER BY sort_order
"""

REVERSE_SK_SUMMARY_SQL = f"""
WITH person_sk_counts AS (
    SELECT
        person_id,
        COUNT(DISTINCT sk_patient_id) AS sk_count
    FROM {PATIENT}
    WHERE person_id IS NOT NULL
      AND sk_patient_id IS NOT NULL
    GROUP BY person_id
)
SELECT
    COUNT(*) AS total_linked_persons,
    COUNT_IF(sk_count > 1) AS multi_sk_persons,
    ROUND(100 * COUNT_IF(sk_count > 1) / NULLIF(COUNT(*), 0), 2) AS pct_multi_sk
FROM person_sk_counts
"""

SOURCE_NULL_SK_SQL = f"""
SELECT
    COUNT(*) AS source_patient_rows,
    COUNT_IF(sk_patient_id IS NULL) AS null_sk_rows,
    ROUND(100 * COUNT_IF(sk_patient_id IS NULL) / NULLIF(COUNT(*), 0), 2) AS pct_null_sk
FROM {RAW_PATIENT}
"""

DOB_CONSISTENCY_SQL = f"""
WITH linked AS (
    SELECT
        pat.id AS patient_id,
        pat.person_id,
        pat.birth_year AS patient_birth_year,
        pat.birth_month AS patient_birth_month,
        per.birth_year AS person_birth_year,
        per.birth_month AS person_birth_month
    FROM {PATIENT} AS pat
    INNER JOIN {PERSON} AS per
        ON pat.person_id = per.id
    WHERE pat.person_id IS NOT NULL
),
counts AS (
    SELECT
        COUNT(*) AS linked_pairs,
        COUNT_IF(
            patient_birth_year IS NOT NULL
            AND patient_birth_month IS NOT NULL
            AND person_birth_year IS NOT NULL
            AND person_birth_month IS NOT NULL
        ) AS comparable_pairs,
        COUNT_IF(
            patient_birth_year IS NOT NULL
            AND patient_birth_month IS NOT NULL
            AND person_birth_year IS NOT NULL
            AND person_birth_month IS NOT NULL
            AND patient_birth_year = person_birth_year
            AND patient_birth_month = person_birth_month
        ) AS matching_pairs,
        COUNT_IF(
            patient_birth_year IS NOT NULL
            AND patient_birth_month IS NOT NULL
            AND person_birth_year IS NOT NULL
            AND person_birth_month IS NOT NULL
            AND (
                patient_birth_year != person_birth_year
                OR patient_birth_month != person_birth_month
            )
        ) AS disagreeing_pairs,
        COUNT_IF(patient_birth_year IS NULL OR patient_birth_month IS NULL) AS missing_patient_dob,
        COUNT_IF(person_birth_year IS NULL OR person_birth_month IS NULL) AS missing_person_dob,
        COUNT_IF(
            patient_birth_year IS NULL
            OR patient_birth_month IS NULL
            OR person_birth_year IS NULL
            OR person_birth_month IS NULL
        ) AS missing_either_dob
    FROM linked
)
SELECT
    'linked_pairs' AS metric,
    linked_pairs AS pair_count,
    ROUND(100 * linked_pairs / NULLIF(linked_pairs, 0), 2) AS pct_of_linked_pairs,
    NULL AS pct_of_comparable_pairs
FROM counts
UNION ALL
SELECT
    'comparable_pairs',
    comparable_pairs,
    ROUND(100 * comparable_pairs / NULLIF(linked_pairs, 0), 2),
    ROUND(100 * comparable_pairs / NULLIF(comparable_pairs, 0), 2)
FROM counts
UNION ALL
SELECT
    'matching_pairs',
    matching_pairs,
    ROUND(100 * matching_pairs / NULLIF(linked_pairs, 0), 2),
    ROUND(100 * matching_pairs / NULLIF(comparable_pairs, 0), 2)
FROM counts
UNION ALL
SELECT
    'disagreeing_pairs',
    disagreeing_pairs,
    ROUND(100 * disagreeing_pairs / NULLIF(linked_pairs, 0), 2),
    ROUND(100 * disagreeing_pairs / NULLIF(comparable_pairs, 0), 2)
FROM counts
UNION ALL
SELECT
    'missing_patient_dob',
    missing_patient_dob,
    ROUND(100 * missing_patient_dob / NULLIF(linked_pairs, 0), 2),
    NULL
FROM counts
UNION ALL
SELECT
    'missing_person_dob',
    missing_person_dob,
    ROUND(100 * missing_person_dob / NULLIF(linked_pairs, 0), 2),
    NULL
FROM counts
UNION ALL
SELECT
    'missing_either_dob',
    missing_either_dob,
    ROUND(100 * missing_either_dob / NULLIF(linked_pairs, 0), 2),
    NULL
FROM counts
"""

VERDICT_SQL = f"""
WITH candidate_persons AS (
    SELECT DISTINCT
        pat.sk_patient_id,
        pat.person_id,
        per.birth_year,
        per.birth_month
    FROM {PATIENT} AS pat
    LEFT JOIN {PERSON} AS per
        ON pat.person_id = per.id
    WHERE pat.sk_patient_id IS NOT NULL
      AND pat.person_id IS NOT NULL
),
candidate_stats AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT person_id) AS candidate_count,
        COUNT_IF(birth_year IS NULL OR birth_month IS NULL) AS missing_candidate_dobs,
        COUNT(DISTINCT IFF(
            birth_year IS NULL OR birth_month IS NULL,
            NULL,
            TO_VARCHAR(birth_year) || '-' || LPAD(TO_VARCHAR(birth_month), 2, '0')
        )) AS candidate_dob_count,
        MAX(birth_year) AS candidate_birth_year,
        MAX(birth_month) AS candidate_birth_month
    FROM candidate_persons
    GROUP BY sk_patient_id
),
assessed AS (
    SELECT
        CASE
            WHEN per.sk_patient_id IS NULL OR COALESCE(cs.candidate_count, 0) = 0
                THEN 'mint without flag'
            WHEN cs.candidate_count = 1
                 AND per.birth_year IS NOT NULL
                 AND per.birth_month IS NOT NULL
                 AND cs.missing_candidate_dobs = 0
                 AND per.birth_year = cs.candidate_birth_year
                 AND per.birth_month = cs.candidate_birth_month
                THEN 'alias cleanly'
            ELSE 'mint with flag'
        END AS verdict
    FROM {PERSON} AS per
    LEFT JOIN candidate_stats AS cs
        ON per.sk_patient_id = cs.sk_patient_id
)
SELECT
    verdict,
    COUNT(*) AS person_count,
    ROUND(100 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_of_persons
FROM assessed
GROUP BY verdict
ORDER BY
    CASE verdict
        WHEN 'alias cleanly' THEN 1
        WHEN 'mint with flag' THEN 2
        ELSE 3
    END
"""

EDGE_CASES_SQL = f"""
WITH candidate_persons AS (
    SELECT DISTINCT
        pat.sk_patient_id,
        pat.person_id,
        per.birth_year,
        per.birth_month
    FROM {PATIENT} AS pat
    LEFT JOIN {PERSON} AS per
        ON pat.person_id = per.id
    WHERE pat.sk_patient_id IS NOT NULL
      AND pat.person_id IS NOT NULL
),
candidate_stats AS (
    SELECT
        sk_patient_id,
        COUNT(DISTINCT person_id) AS candidate_count,
        COUNT_IF(birth_year IS NULL OR birth_month IS NULL) AS missing_candidate_dobs,
        COUNT(DISTINCT IFF(
            birth_year IS NULL OR birth_month IS NULL,
            NULL,
            TO_VARCHAR(birth_year) || '-' || LPAD(TO_VARCHAR(birth_month), 2, '0')
        )) AS candidate_dob_count,
        MAX(birth_year) AS candidate_birth_year,
        MAX(birth_month) AS candidate_birth_month
    FROM candidate_persons
    GROUP BY sk_patient_id
),
person_assessment AS (
    SELECT
        per.id AS person_id,
        per.sk_patient_id,
        per.birth_year,
        per.birth_month,
        COALESCE(cs.candidate_count, 0) AS candidate_count,
        COALESCE(cs.missing_candidate_dobs, 0) AS missing_candidate_dobs,
        COALESCE(cs.candidate_dob_count, 0) AS candidate_dob_count,
        cs.candidate_birth_year,
        cs.candidate_birth_month
    FROM {PERSON} AS per
    LEFT JOIN candidate_stats AS cs
        ON per.sk_patient_id = cs.sk_patient_id
),
person_sk_counts AS (
    SELECT
        person_id,
        COUNT(DISTINCT sk_patient_id) AS sk_count
    FROM {PATIENT}
    WHERE person_id IS NOT NULL
      AND sk_patient_id IS NOT NULL
    GROUP BY person_id
),
edge_cases AS (
    SELECT
        'multi-candidate sk with same DOB' AS pattern,
        COUNT(*) AS affected_persons
    FROM person_assessment
    WHERE candidate_count > 1
      AND missing_candidate_dobs = 0
      AND candidate_dob_count = 1
    UNION ALL
    SELECT
        'multi-candidate sk with differing DOB',
        COUNT(*)
    FROM person_assessment
    WHERE candidate_count > 1
      AND missing_candidate_dobs = 0
      AND candidate_dob_count > 1
    UNION ALL
    SELECT
        'multi-candidate sk with missing DOB',
        COUNT(*)
    FROM person_assessment
    WHERE candidate_count > 1
      AND missing_candidate_dobs > 0
    UNION ALL
    SELECT
        'single candidate but missing DOB',
        COUNT(*)
    FROM person_assessment
    WHERE candidate_count = 1
      AND (
          birth_year IS NULL
          OR birth_month IS NULL
          OR missing_candidate_dobs > 0
      )
    UNION ALL
    SELECT
        'single candidate but DOB disagrees',
        COUNT(*)
    FROM person_assessment
    WHERE candidate_count = 1
      AND birth_year IS NOT NULL
      AND birth_month IS NOT NULL
      AND missing_candidate_dobs = 0
      AND (
          birth_year != candidate_birth_year
          OR birth_month != candidate_birth_month
      )
    UNION ALL
    SELECT
        'person linked to more than one sk',
        COUNT(*)
    FROM person_sk_counts
    WHERE sk_count > 1
)
SELECT
    pattern,
    affected_persons
FROM edge_cases
WHERE affected_persons > 0
ORDER BY affected_persons DESC, pattern
LIMIT 3
"""


def validate_select(sql):
    """Reject accidental write statements before sending SQL to Snowflake."""
    compact = ' '.join(sql.strip().lower().split())
    if not compact.startswith(('select ', 'with ')):
        raise ValueError('Only SELECT statements are allowed')
    words = set(compact.replace('(', ' ').replace(')', ' ').split())
    blocked = sorted(words & FORBIDDEN_SQL)
    if blocked:
        raise ValueError(f"Blocked non-read keyword(s): {', '.join(blocked)}")


def normalise(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return f"{int(value):,}"
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if value is None:
        return ''
    return str(value)


def fetch_table(conn, sql):
    validate_select(sql)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        headers = [col[0].lower() for col in cur.description]
        rows = [[normalise(value) for value in row] for row in cur.fetchall()]
        return headers, rows
    finally:
        cur.close()


def print_table(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("(no rows)")
        return
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers))
    ]
    header_line = '  '.join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    print(header_line)
    print('  '.join('-' * width for width in widths))
    for row in rows:
        print('  '.join(row[i].ljust(widths[i]) for i in range(len(headers))))


def run(conn, title, sql):
    headers, rows = fetch_table(conn, sql)
    print_table(title, headers, rows)


def main():
    env = load_env()
    conn = get_connection(env)
    try:
        print('Synapse person reconciliation profile')
        print(f'Built tables: {PATIENT}, {PERSON}')
        print(f'Raw source: {RAW_PATIENT}')
        run(conn, '1. sk multiplicity', SK_MULTIPLICITY_SQL)
        run(conn, '1. sk multiplicity summary', SK_MULTIPLICITY_SUMMARY_SQL)
        run(conn, '2. multi-person sk DOB corroboration', DOB_CORROBORATION_SQL)
        run(conn, '3. persons linked to multiple sks', REVERSE_SK_SQL)
        run(conn, '3. reverse direction summary', REVERSE_SK_SUMMARY_SQL)
        run(conn, '4. source null-sk rate', SOURCE_NULL_SK_SQL)
        run(conn, '5. patient vs person DOB consistency', DOB_CONSISTENCY_SQL)
        run(conn, '6. verdict under exactly-one sk + DOB rule', VERDICT_SQL)
        run(conn, '6. top edge-case patterns', EDGE_CASES_SQL)
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
