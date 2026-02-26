-- daily_revenue_net.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with net_revenue as (
    select 
    cast(o_ts as date) as date_day,
    sum(
        case 
            when event_type = 'purchase' then amount_in_euros - tax_in_euros
            when event_type = 'refund' then -amount_in_euros
            ELSE 0
        END
    ) AS net_revenue,
    max(event_date) as event_date
    from {{ ref('int_amounts_to_euros') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type in ('purchase', 'refund')
    group by to_date(o_ts)
    order by 1
)

select * from net_revenue

