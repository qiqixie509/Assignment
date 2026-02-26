-- daily_active_users.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date'],
    unique_key = ['user_id', 'event_date']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with daily_events as (
    select 
        user_id,
        event_date
    from {{ ref('stg_events') }}
    where event_date between TO_DATE(CAST({{ start_date }} AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST({{ start_date }} AS STRING), 'yyyyMMdd'), {{ interval_days }})
    group by 1, 2
),
new_users as (
    select
        e.user_id,
        e.event_date,
        u.first_event_date,
        case 
            when u.first_event_date = e.event_date then 1
            else 0
        end as is_new_user
    from daily_events e
    left join {{ ref('int_users_first_event') }} u
        on e.user_id = u.user_id
    order by event_date
)

select 
    *
from new_users
