{{ config(
    materialized='incremental',
    unique_key= ['kycdocumentid'],
    depends_on=['inc_kycs_stg'],
    on_schema_change='append_new_columns'

)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_kycs_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id AS id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    stg.kycdocumentid,
    stg.nationalid,
    stg.mobilenumber,
    stg.kycreferenceno,
    stg.fullname,
    stg.clientname_en,
    stg.branchname_en,
    stg.hash_column,
    stg.kyc_modifiedat_local,
    stg.kyc_registeredat_local,
    stg.kyc_returnedat_local,
    stg.kyc_submittedat_local,
    stg.kyc_approvedat_local,
    stg.utc,
    stg.registeredby_name,
    stg.createdby_name,
    stg.kyc_channel,
    stg.kyc_status,
    stg.kyc_substatus,
    stg.meeza_response_code,
    stg.return_reason,
    stg.failure_reason,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate

FROM {{ source('dbt-dimensions', 'inc_kycs_stg') }} stg
JOIN {{ source('dbt-dimensions', 'inc_kycs_dimension')}} final
    ON stg.kycdocumentid = final.kycdocumentid 
WHERE stg.loaddate > final.loaddate AND stg.hash_column != final.hash_column AND final.currentflag = true

{% else %}
-- do nothing (extremely high comparison date)
SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.kycdocumentid,
    stg.nationalid,
    stg.mobilenumber,
    stg.kycreferenceno,
    stg.fullname,
    stg.clientname_en,
    stg.branchname_en,
    stg.hash_column,
    stg.kyc_modifiedat_local,
    stg.kyc_registeredat_local,
    stg.kyc_returnedat_local,
    stg.kyc_submittedat_local,
    stg.kyc_approvedat_local,
    stg.utc,
    stg.registeredby_name,
    stg.createdby_name,
    stg.kyc_channel,
    stg.kyc_status,
    stg.kyc_substatus,
    stg.meeza_response_code,
    stg.return_reason,
    stg.failure_reason,
    stg.loaddate

FROM {{ source('dbt-dimensions', 'inc_kycs_stg') }} stg

{% endif %}