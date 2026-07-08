{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name == 'olids_landing' -%}
        LANDING
    {%- elif custom_schema_name == 'olids_conformed' -%}
        CONFORMED
    {%- elif custom_schema_name == 'olids_stable' -%}
        STABLE
    {%- elif custom_schema_name == 'synapse_base' -%}
        SYNAPSE_BASE
    {%- elif custom_schema_name == 'synapse_stable' -%}
        SYNAPSE_STABLE
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
