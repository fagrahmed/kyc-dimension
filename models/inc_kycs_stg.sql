
{{ config(
    materialized='incremental',
    unique_key= ['kycdocumentid'],
    on_schema_change='append_new_columns'

)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_kycs_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'inc_kycs_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists =stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}


SELECT
    md5(random()::text || '-' || COALESCE(k.kycdocumentid, '') || '-' || COALESCE(k.lastmodifiedts, '') || '-' || COALESCE(kr.lastmodifiedat, '') || '-' || now()::text) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    k.kycdocumentid,
    k.customernationalid as nationalid,
    k.mobileno as mobilenumber,
    k.kycreferenceno,
    k.fullname,
    k.client->>'name'->>'en' as clientname_en,
    k.branchdata->>'branchNameEn' as branchname_en,

    md5(
        COALESCE(k.client->>'name'->>'en', '') || '::' || COALESCE(k.submittedts, '') || '::' || COALESCE(k.kycstatus, '') || '::' ||
        COALESCE(k.verifiedts, '') || '::' || COALESCE(k.returnedts, '') || '::' || COALESCE(k.lastmodifiedts, '') || '::' ||
        COALESCE(k.submittedts, '') || '::' || COALESCE(k.failurereason, '') 
    ) AS hash_column,

    (k.lastmodifiedts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as kyc_modifiedat_local,
    (k.registeredts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as kyc_registeredat_local,
    (k.returnedts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as kyc_returnedat_local,
    (k.submittedts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as kyc_submittedat_local,
    (k.verifiedts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as kyc_approvedat_local,
    3 AS utc,

    k.registeredby->>'userName' as registeredby_name,
    CASE 
        WHEN k.createdby_aibyte_transform = 'SELF' THEN k.createdby_aibyte_transform 
        ELSE k.createdby_aibyte_transform::jsonb ->> 'userName' 
    END AS createdby_name,
    k.kycchannel as kyc_channel,
    k.kycstatus as kyc_status,
    k.kycsubstatus as kyc_substatus,
    k.meezaregresponsecode as meeza_response_code,
    kr.comment as return_reason,
    k.failurereason as failure_reason,

    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as loaddate

FROM {{source('axis_kyc', 'kycdocument')}} k
LEFT JOIN {{source('axis_kyc', 'kycreturncomments')}} kr on k.kycdocumentid = kr.kycdocumentid

{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (k._airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'inc_kycs_dimension') }}), '1900-01-01'::timestamp)
        OR (kr._airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'inc_kycs_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
