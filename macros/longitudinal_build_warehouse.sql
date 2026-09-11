{% macro longitudinal_build_warehouse(restore=false) -%}
    {# Hooks evaluate the run mode at execution time, outside the parse cache. #}
    {%- if execute and target.role | upper == 'DBT_ADMIN' -%}
        {%- if restore -%}
            use warehouse {{ adapter.quote(target.warehouse) }}
        {%- elif not is_incremental() -%}
            use warehouse {{ adapter.quote('WH_WNL_OLIDS_L') }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
