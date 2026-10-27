{{ config(alias='gp2gp_copy_link', cluster_by=['person_id', 'gp2gp_original_id']) }}

{% set evidence_kinds = ['observation', 'medication_statement', 'medication_order'] %}

{% for evidence_kind in evidence_kinds %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT
        '{{ evidence_kind }}' AS evidence_kind,
        copy.id AS copy_record_id,
        copy.gp2gp_original_id,
        copy.person_id,
        copy.patient_id,
        copy.publisher_organisation_id,
        copy.publisher_organisation_code,
        copy.clinical_effective_date,
        copy.date_recorded,
        copy.lds_is_deleted
    FROM {{ ref('stable_' ~ evidence_kind) }} AS copy
    WHERE
        copy.is_gp2gp_copy
        AND NOT COALESCE(copy.lds_is_deleted, FALSE)
        AND EXISTS (
            SELECT 1
            FROM {{ ref('stable_' ~ evidence_kind) }} AS original
            WHERE
                original.id = copy.gp2gp_original_id
                AND original.person_id = copy.person_id
        )
{% endfor %}
