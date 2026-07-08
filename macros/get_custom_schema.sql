{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name == 'olids_landing' -%}
        OLIDS_EXPERIMENTAL_LANDING
    {%- elif custom_schema_name == 'olids_conformed' -%}
        OLIDS_EXPERIMENTAL_CONFORMED
    {%- elif custom_schema_name == 'olids_stable' -%}
        OLIDS_EXPERIMENTAL_STABLE
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
