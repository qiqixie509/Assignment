-- int_users_first_event.sql
{{ config(
    materialized = 'table',
    unique_key = ['user_id'],
    incremental_strategy = 'merge'
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with users_first_event as (
    select 
        user_id,
        min(event_date) as first_event_date
    from {{ ref('stg_events')}}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    group by 1
)

select 
    c.*
from users_first_event c
left join {{this}} t
on c.user_id = t.user_id
where t.user_id is null


