{% macro longitudinal_remove_withdrawn_records(source_model, key) -%}
    {%- if is_incremental() -%}
        -- Reconcile narrow keys daily, including records removed from the filtered spine.
        delete from {{ this }} as previous
        where not exists (
            select 1 from {{ ref(source_model) }} as current_source
            where previous.{{ key }} = current_source.{{ key }}
        )
    {%- endif -%}
{%- endmacro %}
