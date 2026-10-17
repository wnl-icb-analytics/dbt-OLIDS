{% macro gp2gp_registration_starts() %}
    -- Distinct starts prevent overlapping joins after re-registration on one record.
    SELECT
        patient_id,
        registration_start_date,
        LEAD(registration_start_date, 1, '9999-12-31'::DATE) OVER (
            PARTITION BY patient_id ORDER BY registration_start_date
        ) AS next_registration_start_date
    FROM (
        SELECT DISTINCT
            patient_id,
            episode_of_care_start_date::DATE AS registration_start_date
        FROM {{ ref('conformed_episode_of_care') }}
        WHERE
            episode_type_source_code = 'Regular'
            AND NOT COALESCE(lds_is_deleted, FALSE)
            AND patient_id IS NOT NULL
            AND episode_of_care_start_date IS NOT NULL
    )
{% endmacro %}

{% macro gp2gp_copy_flags(rows, extra_partition_columns=[]) %}
    {# rows contains live, dated events for multi-record persons. Its registration
       start is the latest Regular start on or before the recorded date. #}
    {%- set partition -%}
        person_id, mapped_concept_code, clinical_date, match_value
        {%- for column in extra_partition_columns %}, {{ column }}{% endfor %}
    {%- endset %}
    SELECT
        id,
        original_id AS gp2gp_original_id
    FROM (
        SELECT
            id,
            patient_id,
            clinical_date,
            date_recorded,
            registration_start_date,
            FIRST_VALUE(patient_id) OVER (
                PARTITION BY {{ partition }}
                ORDER BY date_recorded ASC NULLS LAST, patient_id, id
            ) AS first_patient_id,
            FIRST_VALUE(id) OVER (
                PARTITION BY {{ partition }}
                ORDER BY date_recorded ASC NULLS LAST, patient_id, id
            ) AS original_id,
            FIRST_VALUE(date_recorded) OVER (
                PARTITION BY {{ partition }}
                ORDER BY date_recorded ASC NULLS LAST, patient_id, id
            ) AS first_date_recorded
        FROM {{ rows }}
        WHERE mapped_concept_code IS NOT NULL
    )
    WHERE
        patient_id <> first_patient_id
        -- Ties and missing chronology do not establish a transfer sequence.
        AND first_date_recorded IS NOT NULL
        AND date_recorded IS NOT NULL
        AND first_date_recorded < date_recorded
        AND (
            DATEDIFF('day', clinical_date, date_recorded) > 1
            -- The registration join already requires recording on or after the start.
            OR clinical_date < registration_start_date
        )
{% endmacro %}

{% macro gp2gp_flags(model, match_column, extra_partition_columns=[]) %}
    WITH multi_record_persons AS (
        SELECT person_id
        FROM {{ ref('conformed_patient') }}
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        HAVING COUNT(DISTINCT id) > 1
    ),

    registration_starts AS (
        {{ gp2gp_registration_starts() }}
    ),

    candidate_rows AS (
        SELECT
            src.id,
            src.person_id,
            src.patient_id,
            src.mapped_concept_code,
            src.clinical_effective_date AS clinical_date,
            src.date_recorded,
            src.{{ match_column }} AS match_value
            {% for column in extra_partition_columns %}
                , src.{{ column }}
            {% endfor %}
        FROM {{ ref(model) }} AS src
        INNER JOIN multi_record_persons USING (person_id)
        WHERE
            NOT COALESCE(src.lds_is_deleted, FALSE)
            AND src.clinical_effective_date IS NOT NULL
            AND src.mapped_concept_code IS NOT NULL
    ),

    multi_record_keys AS (
        -- Only partitions spanning patient records can contain a transfer copy.
        SELECT
            person_id, mapped_concept_code, clinical_date, match_value
            {% for column in extra_partition_columns %}, {{ column }}{% endfor %}
        FROM candidate_rows
        GROUP BY
            person_id, mapped_concept_code, clinical_date, match_value
            {% for column in extra_partition_columns %}, {{ column }}{% endfor %}
        HAVING MIN(patient_id) <> MAX(patient_id)
    ),

    window_rows AS (
        SELECT c.*, r.registration_start_date
        FROM candidate_rows AS c
        INNER JOIN multi_record_keys AS k
            ON
                c.person_id = k.person_id
                AND c.mapped_concept_code = k.mapped_concept_code
                AND c.clinical_date = k.clinical_date
                AND EQUAL_NULL(c.match_value, k.match_value)
                {% for column in extra_partition_columns %}
                    AND EQUAL_NULL(c.{{ column }}, k.{{ column }})
                {% endfor %}
        LEFT JOIN registration_starts AS r
            ON
                c.patient_id = r.patient_id
                AND c.date_recorded::DATE >= r.registration_start_date
                AND c.date_recorded::DATE < r.next_registration_start_date
    ),

    copies AS (
        {{ gp2gp_copy_flags('window_rows', extra_partition_columns) }}
    ),

    imports AS (
        -- Inferred filing window, including records with no retained original.
        -- A separate scan avoids buffering every eligible event for both branches.
        SELECT src.id
        FROM {{ ref(model) }} AS src
        INNER JOIN registration_starts AS r
            ON
                src.patient_id = r.patient_id
                AND src.date_recorded::DATE >= r.registration_start_date
                AND src.date_recorded::DATE < r.next_registration_start_date
        WHERE
            NOT COALESCE(src.lds_is_deleted, FALSE)
            AND src.person_id IS NOT NULL
            AND src.clinical_effective_date IS NOT NULL
            AND src.clinical_effective_date < r.registration_start_date
            AND src.date_recorded::DATE <= DATEADD('day', 30, r.registration_start_date)
    )

    SELECT
        COALESCE(copies.id, imports.id) AS id,
        copies.id IS NOT NULL AS is_gp2gp_copy,
        copies.gp2gp_original_id,
        imports.id IS NOT NULL AS is_gp2gp_import
    FROM copies
    FULL OUTER JOIN imports
        ON copies.id = imports.id
{% endmacro %}
