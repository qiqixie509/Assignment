-- stg_subscriptions.sql
{{ config(
    materialized='view',
    incremental_strategy='merge',
    unique_key=['subscription_id', 'user_id'],
    partition_by = ['created_at']
)}}


{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

WITH raw_data AS (
    SELECT
        *
    FROM 
        {{ source('raw_data', 'subscriptions') }}
),
cleaned_data AS (
    select 
        subscription_id,
        user_id,
        plan_id,
        try_cast(price as double) as price,
        status,
        upper(trim(currency)) as currency,
        try_cast(start_date as date) as start_date,
        try_cast(created_at as timestamp) as created_at,
        to_date(created_at) as created_date,
        try_cast(end_date as date) as end_date
    from raw_data
    where to_date(created_at) BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and subscription_id is not null and user_id is not null
)

SELECT * FROM cleaned_data
qualify row_number() over (partition by subscription_id, user_id, plan_id order by created_at desc) = 1
