{% macro longitudinal_remove_withdrawn_records(source_model, keys) -%}
    {%- if is_incremental() -%}
        -- Natural keys avoid regenerating UUIDs across the full source history.
        delete from {{ this }} as previous
        where not exists (
            select 1 from {{ ref(source_model) }} as current_source
            where
                {% for key in keys %}
                previous.{{ key }} = current_source.{{ key }}
                {% if not loop.last %}and{% endif %}
                {% endfor %}
        )
    {%- endif -%}
{%- endmacro %}
