-- stg_marketing_spend.sql
{{ config(
    materialized='view',
    incremental_strategy='merge',
    unique_key=['spend_date', 'channel'],
    partition_by = ['spend_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with raw_data as (
    SELECT
    *
FROM 
    {{ source('raw_data', 'marketing_spend') }}
),
cleaned_data as (
    SELECT
        cast(date as date) as spend_date,
        COALESCE(trim(channel), 'unknown') AS channel,
        cast(spend as double) as spend,
        to_date(current_date()) as event_date
    FROM raw_data
    where date BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and date is not null and channel is not null and spend is not null
)

SELECT * FROM cleaned_data
qualify row_number() over (partition by spend_date, channel order by spend_date desc) = 1