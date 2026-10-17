{{ config(materialized='table', transient=true) }}

{{ gp2gp_flags('conformed_observation', 'result_value', ['source_entity']) }}
