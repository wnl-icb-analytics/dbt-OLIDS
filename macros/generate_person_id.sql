{% macro generate_person_id(column) %}
    {#-
    Generates a deterministic 16-digit numeric person_id from a source key.
    Range [10^15, 9*10^15): every value stays below 2^53, so ids survive
    doubles-based tools (Excel, JavaScript) uncorrupted. Collision chance
    at 7.9M persons is ~0.4%; per-run uniqueness tests guard the rest.
    -#}
    ABS(MOD(MD5_NUMBER_LOWER64({{ column }}), 8 * POWER(10, 15)::NUMBER)) + POWER(10, 15)::NUMBER
{% endmacro %}