-- cac_by_channel.sql
{{
  config(
    materialized = 'table',
    incremental_strategy='merge',
    partition_by = ['event_date']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with marketing_spend as (
    select 
        spend_date,
        channel,
        case 
            when spend < 0 then 0
            else spend
        end as spend
    from {{ ref('stg_marketing_spend') }}
    where spend_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
),
new_user_counts as (
    select
        event_date,
        channel,
        count(user_id) as new_user_count
    from {{ ref('daily_active_users') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and is_new_user = 1
    group by 1,2
)

select 
    n.event_date,
    n.channel,
    n.new_user_count,
    m.spend,
    case 
        when n.new_user_count = 0 then 0
        else cast(round(m.spend / n.new_user_count, 2) as numeric(12,2))
    end as cac
from new_user_counts n
left join marketing_spend m
    on n.event_date = m.spend_date
    and n.channel = m.channel