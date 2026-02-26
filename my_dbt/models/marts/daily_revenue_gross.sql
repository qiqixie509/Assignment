-- daily_revenue_gross.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with revenue_gross as (
    select 
        event_date,
        sum(amount_in_euros) as gross_revenue
    from {{ ref('int_amounts_to_euros') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type = 'purchase'
    group by event_date
    order by event_date
)

select * from revenue_gross