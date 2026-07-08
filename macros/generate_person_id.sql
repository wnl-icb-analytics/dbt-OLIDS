{% macro generate_person_id(column) %}
    {#-
    Generates a deterministic 15-digit numeric person_id from a source key.
    15 digits is Excel's significant-figure limit, so ids survive CSV round
    trips uncorrupted (16 digits get their last digit zeroed on entry).
    Collision chance at 7.9M persons is ~3%: verify uniqueness over the full
    population before consumers onboard (bump a version string in the hash
    input if a collision appears); per-run uniqueness tests guard after that.
    -#}
    ABS(MOD(MD5_NUMBER_LOWER64({{ column }}), 9 * POWER(10, 14)::NUMBER)) + POWER(10, 14)::NUMBER
{% endmacro %}