-- daily_revenue_gross.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with purchase_events_in_euros as (
    select 
        user_id,
        event_date,
        amount_in_euros
    from {{ ref('int_amounts_to_euros') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type = 'purchase'
)

select 
    event_date,
    sum(amount_in_euros) as gross_revenue
from purchase_events_in_euros
group by 1
order by 1