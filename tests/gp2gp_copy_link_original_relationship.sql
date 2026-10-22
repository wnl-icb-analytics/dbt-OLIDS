-- Return failure counts only, so failed tests cannot expose record identifiers.
{% set evidence_kinds = ['observation', 'medication_statement', 'medication_order'] %}

{% for evidence_kind in evidence_kinds %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT
        '{{ evidence_kind }}' AS evidence_kind,
        COUNT(*) AS missing_original_count
    FROM {{ ref('stable_gp2gp_copy_link') }} AS link
    WHERE
        link.evidence_kind = '{{ evidence_kind }}'
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('stable_' ~ evidence_kind) }} AS original
            WHERE
                original.id = link.gp2gp_original_id
                AND original.person_id = link.person_id
        )
    HAVING COUNT(*) > 0
{% endfor %}
