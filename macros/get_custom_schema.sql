{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name == 'olids_landing' -%}
        OLIDS_EXPERIMENTAL_LANDING
    {%- elif custom_schema_name == 'olids_base' -%}
        OLIDS_EXPERIMENTAL_BASE
    {%- elif custom_schema_name == 'olids' -%}
        OLIDS_EXPERIMENTAL
    {%- elif custom_schema_name == 'dbt_base' -%}
        OLIDS_EXPERIMENTAL_BASE
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
