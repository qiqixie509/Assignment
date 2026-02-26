{{ config(
    materialized='incremental',
    unique_key=['user_id','event_date'],
    incremental_strategy='merge'
) }}

{% set start_date = var('start_date') %}
{% set interval_days = var('interval', 1) %}


with new_revenue as (
    select 
        user_id,
        event_date,
        sum (
            case
                when event_type = 'purchase' then amount_in_euros - coalesce(tax_in_euros,0)
                when event_type = 'refund' then -amount_in_euros
            end
        ) as revenue_to_add
    from {{ ref('int_amounts_to_euros') }}
    where event_type in ('purchase','refund')
      and event_date between
            to_date('{{ start_date }}','yyyyMMdd')
        and date_add(to_date('{{ start_date }}','yyyyMMdd'), {{ interval_days }})
    group by 1,2
),
users as (

    select
        user_id,
        first(channel) as channel
    from {{ ref('stg_events') }}
    where event_type = 'signup'
    group by user_id
),
yesterday_ltv as (

    {% if is_incremental() %}

        select 
            user_id,
            channel,
            ltv,
            event_date
        from {{ this}}
        where event_date < to_date('{{ start_date }}','yyyyMMdd')

    {% else %}

        -- first run: no history yet
        select
            cast(null as string) as user_id,
            cast(null as date) as event_date,
            cast(null as string) as channel,
            cast(null as double) as ltv
        where 1=0

    {% endif %}

)

select
    coalesce(d.user_id, y.user_id) as user_id,
    coalesce(y.channel, u.channel, 'unknown') as channel,
    coalesce(d.event_date, date_add(to_date('{{ start_date }}','yyyyMMdd'), 0)) as event_date,
    coalesce(y.ltv,0) + coalesce(d.revenue_to_add,0) as ltv
from yesterday_ltv y
full outer join new_revenue d
    on y.user_id = d.user_id
left join users u
    on coalesce(d.user_id, y.user_id) = u.user_id