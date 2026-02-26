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
signup_events as (
    select 
        event_date,
        channel,
        user_id
    from {{ ref('stg_events') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type = 'signup'
),
new_user_counts as (
    select
        dau.event_date,
        se.channel,
        count(dau.user_id) AS new_user_count
    from {{ ref('daily_active_users') }} dau
    inner join signup_events se
    on dau.user_id = se.user_id
    where dau.event_date BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
                        AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and dau.is_new_user = 1
    group by 1,2
)

select 
    coalesce(n.event_date, m.spend_date) as event_date,
    coalesce(n.channel, m.channel) as channel,
    n.new_user_count,
    m.spend,
    CASE 
        WHEN COALESCE(new_user_count, 0) > 0 
            THEN CAST(spend / new_user_count AS NUMERIC(12,4))
        ELSE NULL
    END AS cac
from marketing_spend m
left join new_user_counts n
    on m.spend_date = n.event_date
    and m.channel = n.channel