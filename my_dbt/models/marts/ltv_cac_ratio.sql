-- ltv_cac_ratio.sql
{{ config(
    materialized='table'
) }}

with channel_ltv AS (
    select
        channel,
        sum(ltv) as total_ltv,
        count(user_id) as total_users
    from {{ ref('ltv_per_user') }}
    group by 1
),

channel_cac AS (
    select
        channel,
        cast(avg(cac) as numeric(12,2)) as avg_cac
    from {{ ref('cac_by_channel') }}
    group by 1
),
ltv_cac_ratio as (
    select
        l.channel,
        l.total_ltv,
        l.total_users,
        c.avg_cac,
        case 
            when c.avg_cac = 0 then null
            else cast(round((l.total_ltv / l.total_users) / c.avg_cac, 2) as numeric(12,2))
        end as ltv_cac_ratio
    from channel_ltv l
    left join channel_cac c
        on l.channel = c.channel
    order by l.channel
)

select * from ltv_cac_ratio