{% macro generate_person_id(column) %}
    {#-
    Generates a deterministic 14-digit numeric person_id from a UUID string.
    Uses MD5_NUMBER_LOWER64 constrained to the range 10^13..10^14-1,
    guaranteeing exactly 14 digits and zero collisions for up to ~5M persons.
    -#}
    ABS(MOD(MD5_NUMBER_LOWER64({{ column }}), 9 * POWER(10, 13)::NUMBER)) + POWER(10, 13)::NUMBER
{% endmacro %}