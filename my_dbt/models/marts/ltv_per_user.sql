-- ltv_per_user.sql
{{ config(
    materialized='table',          
    unique_key='user_id' 
) }}

{% set start_date = var('start_date') %}
{% set interval_days = var('interval', 1) %}

-- Get new revenue after signup
with int_users_signup as (
    select
        user_id,
        min(event_date) as signup_date,
        first(channel) as channel
    from {{ ref('stg_events') }}
    where event_type = 'signup'
    group by user_id
),

new_revenue as (
    select
        s.user_id,
        s.channel,
        sum(
            case
                when e.event_type = 'purchase' then e.amount_in_euros
                when e.event_type = 'refund' then -e.amount_in_euros
                else 0
            end
        ) as revenue_to_add
    from int_users_signup s
    left join {{ ref('int_amounts_to_euros') }} e
        on s.user_id = e.user_id
        and e.event_date >= s.signup_date
        and e.event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
            and DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    group by s.user_id, s.channel
)

-- Incremental merge logic
{% if is_incremental() %}

select
    COALESCE(h.user_id, n.user_id) as user_id,
    COALESCE(h.channel, n.channel) as channel,
    COALESCE(h.ltv, 0) + COALESCE(n.revenue_to_add, 0) as ltv,
    current_timestamp() as updated_at
from {{ this }} h
full outer join new_revenue n
    on h.user_id = n.user_id
    and h.channel = n.channel

{% else %}

select
    user_id,
    channel,
    revenue_to_add as ltv,
    current_timestamp() as updated_at
from new_revenue

{% endif %}