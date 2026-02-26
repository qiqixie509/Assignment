-- mrr_daily.sql
{{ config(
    materialized = 'table',
    incremental_strategy = 'merge',
    partition_by = ['event_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with data_spine as (
    select 
        date_day
    from {{ ref('int_data_spine') }}
),
subs as (
    select 
        subscription_id,
        user_id,
        start_date,
        end_date,
        price,
        status,
        event_date
    from {{ ref('stg_subscriptions') }}
    where to_date(event_date) BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
),
expanded as (
    select 
        d.date_day,
        s.subscription_id,
        s.user_id,
        s.price as mrr_value,
        s.status,
        s.event_date
    from data_spine d
    cross join subs s
    where d.date_day between s.start_date and s.end_date
    order by date_day, subscription_id
)

select * from expanded