{{
    config(
        materialized="incremental",
        unique_key= ["kycdocumentid"],
        on_schema_change='append_new_columns',
	    incremental_strategy = 'merge'
	)
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_kycs_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('inc_kycs_stg_update') %}
{% set _ = ref('inc_kycs_stg_new') %}
{% set _ = ref('inc_kycs_stg') %}

SELECT

    id,
    operation,
    currentflag,
    expdate,
    kycdocumentid,
    nationalid,
    mobilenumber,
    kycreferenceno,
    fullname,
    clientname_en,
    branchname_en,
    hash_column,
    kyc_modifiedat_local,
    kyc_registeredat_local,
    kyc_returnedat_local,
    kyc_submittedat_local,
    kyc_approvedat_local,
    utc,
    registeredby_name,
    createdby_name,
    kyc_channel,
    kyc_status,
    kyc_substatus,
    meeza_response_code,
    return_reason,
    failure_reason,
    loaddate

FROM {{ ref("inc_transactions_stg_update") }}

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    kycdocumentid,
    nationalid,
    mobilenumber,
    kycreferenceno,
    fullname,
    clientname_en,
    branchname_en,
    hash_column,
    kyc_modifiedat_local,
    kyc_registeredat_local,
    kyc_returnedat_local,
    kyc_submittedat_local,
    kyc_approvedat_local,
    utc,
    registeredby_name,
    createdby_name,
    kyc_channel,
    kyc_status,
    kyc_substatus,
    meeza_response_code,
    return_reason,
    failure_reason,
    loaddate

FROM {{ref("inc_kycs_stg_exp")}}

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    kycdocumentid,
    nationalid,
    mobilenumber,
    kycreferenceno,
    fullname,
    clientname_en,
    branchname_en,
    hash_column,
    kyc_modifiedat_local,
    kyc_registeredat_local,
    kyc_returnedat_local,
    kyc_submittedat_local,
    kyc_approvedat_local,
    utc,
    registeredby_name,
    createdby_name,
    kyc_channel,
    kyc_status,
    kyc_substatus,
    meeza_response_code,
    return_reason,
    failure_reason,
    loaddate

FROM {{ ref("inc_transactions_stg_new") }}