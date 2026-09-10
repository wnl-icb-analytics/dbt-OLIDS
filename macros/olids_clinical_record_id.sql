{% macro olids_clinical_record_id(record_type, record_id) -%}
    UUID_STRING(
        '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
        'olids:clinical_record:' || {{ record_type }} || ':' || {{ record_id }}::VARCHAR
    )::UUID
{%- endmacro %}
