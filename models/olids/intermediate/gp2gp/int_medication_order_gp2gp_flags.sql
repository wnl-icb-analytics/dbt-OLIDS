{{ config(materialized='table', transient=true) }}

{{ gp2gp_flags('conformed_medication_order', 'quantity_value') }}
