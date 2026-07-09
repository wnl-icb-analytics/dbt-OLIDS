{% macro encode_crockford32(int_expr, width=6) %}
    {#-
    Renders a non-negative integer as fixed-width uppercase Crockford base32
    (no I, L, O, U). Fixed width keeps ids sortable and visually uniform.
    -#}
    {%- set alphabet = "'0123456789ABCDEFGHJKMNPQRSTVWXYZ'" -%}
    {%- for p in range(width - 1, -1, -1) -%}
        SUBSTR({{ alphabet }}, (FLOOR(({{ int_expr }}) / POWER(32, {{ p }})) % 32)::INT + 1, 1){% if not loop.last %} || {% endif %}
    {%- endfor -%}
{% endmacro %}
