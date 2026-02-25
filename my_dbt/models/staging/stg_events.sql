-- stg_events.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by = ['event_date']

) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}


WITH raw_source AS (
    SELECT
    *
FROM 
    {{ source('raw_data', 'events') }}
),
cleaned_data AS (
    SELECT
        event_id,
        user_id,
        refers_to_event_id,

        COALESCE(trim(acquisition_channel), 'unknown') AS channel,
        event_type as event_type,
        page as page,
        schema_version as schema_version,

        try_cast (amount as double) as amount,
        case 
            when currency is null or trim(currency) = '' then 'USD'
            else upper(trim(currency))
        end as currency,
        try_cast (tax as double) as tax, 
        to_timestamp(timestamp) as o_ts,
        to_date(timestamp) as event_date
        
    FROM raw_source
    WHERE to_date(timestamp) BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_id IS NOT NULL and user_id is not null

)

SELECT * FROM cleaned_data
-- deduplicate
qualify row_number() over (partition by event_id, user_id, event_type, amount, currency, tax order by event_date desc) = 1
