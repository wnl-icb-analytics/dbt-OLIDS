{% macro longitudinal_delivery_filter() -%}
    {%- if is_incremental() -%}
        left join (
            select source_record_type, max(source_extraction_date) as last_extraction_at
            from {{ this }}
            group by source_record_type
        ) as watermark using (source_record_type)
        where current_records.source_extraction_date is null
            or current_records.source_extraction_date >= coalesce(
                watermark.last_extraction_at, '1900-01-01'::timestamp_ntz
            )
    {%- endif -%}
{%- endmacro %}
