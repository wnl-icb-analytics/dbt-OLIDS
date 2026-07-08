{% macro generate_person_id_legacy(column) %}
    {#-
    Frozen 14-digit person_id used only by the synapse tree. Do not change:
    downstream consumers of the legacy OLIDS output rely on these exact values.
    Retires with the synapse tree (#255).
    -#}
ABS(MOD(MD5_NUMBER_LOWER64({{ column }}), 9 * POWER(10, 13)::NUMBER)) + POWER(10, 13)::NUMBER
{% endmacro %}
