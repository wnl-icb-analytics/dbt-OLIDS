{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'ncl', 'practices'],
        cluster_by=['practice_code'])
}}

/*
NCL Practices Lookup
GP practices commissioned by North Central London (sub-ICB 93C: Barnet, Camden,
Enfield, Haringey, Islington). NWL (W2U3Z) is excluded until the COMPASS bridge
lands (#239, #266).

Foundation filter for the OLIDS tree: joined at landing to cut the scan, and again
at conformed as a guard.

Source is active practices only. A practice that closes drops out of the lookup and
its history leaves the pipeline with it; switch to PRACTICE_ALL if that becomes a
problem.
*/

SELECT
    practice_code,
    practice_name,
    sub_icb_code,
    sub_icb_name,
    registered_borough_name,
    pcn_code,
    pcn_name
FROM {{ source('reference', 'PRACTICE_CURRENT') }}
WHERE
    sub_icb_code = '93C'
    AND practice_code IS NOT NULL
