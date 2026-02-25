-- daily_revenue_net.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with gross_revenue as (
    select 
        event_date,
        gross_revenue
    from {{ ref('daily_revenue_gross') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
),
refund_events as (
    select 
        event_date,
        amount_in_euros
    from {{ ref('int_amounts_to_euros') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type = 'refund'
),
gross_refunds as (
    select 
        event_date,
        sum(amount_in_euros) as refund_amount
    from refund_events
    group by 1
)

select 
    event_date,
    gross_revenue - refund_amount as net_revenue
from gross_revenue
left join gross_refunds using (event_date)
order by 1
