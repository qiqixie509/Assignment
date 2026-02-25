-- mrr_daily.sql
{{ config(
    materialized = 'table',
    incremental_strategy = 'merge',
    partition_by = ['created_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with active_subscriptions_daily as (
    select 
        created_date,
        cast(sum(price) as numeric(10,2)) as total_price,
        count(subscription_id) as active_subscriptions
    from {{ ref('stg_subscriptions') }}
    where created_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and status = 'active'
    group by 1
)

select * from active_subscriptions_daily