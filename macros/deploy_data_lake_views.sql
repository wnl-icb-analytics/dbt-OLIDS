{% macro deploy_data_lake_views() %}
    {% if execute and flags.WHICH in ('run', 'build') %}
        {% set stable_built = [] %}
        {% for r in results %}
            {% if r.status == 'success' and r.node.resource_type == 'model'
                  and r.node.schema | lower in ('olids_stable', 'stable') %}
                {% do stable_built.append(r.node.name) %}
            {% endif %}
        {% endfor %}
        {% if stable_built | length > 0 %}
            {# Snowflake forbids mixing positional and named arguments: all named #}
            {% do run_query("CALL DATA_LAKE.CONTROL.DEPLOY_DATA_LAKE_VIEWS_FOR_SINGLE_SOURCE_MAPPING(SOURCE_DATABASE => 'OLIDS_ENGINEERING', SOURCE_SCHEMA => 'STABLE', DESTINATION_SCHEMA => 'OLIDS_EXPERIMENTAL', FORCE_DEPLOY => TRUE)") %}
            {% do log('data_lake OLIDS_EXPERIMENTAL views redeployed (' ~ stable_built | length ~ ' stable models built)', info=True) %}
        {% endif %}
    {% endif %}
{% endmacro %}
