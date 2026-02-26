-- ltv_cac_ratio.sql
{{
  config(
    materialized='table',
    incremental_strategy='merge',
    unique_key=['event_date', 'channel']
  )
}}

{% set start_date = var('start_date') %}
{% set interval_days = var('interval', 1) %}

with ltv_by_channel as (
    select
        event_date,
        channel,
        cast(sum(ltv) as numeric(12,4)) as total_ltv
    from {{ ref('ltv_per_user') }}
    where event_date between to_date('{{ start_date }}','yyyyMMdd')
      and date_add(to_date('{{ start_date }}','yyyyMMdd'), {{ interval_days }})
    group by 1,2
),

cac as (
    select
        event_date,
        channel,
        cac
    from {{ ref('cac_by_channel') }}
    where event_date between to_date('{{ start_date }}','yyyyMMdd')
      and date_add(to_date('{{ start_date }}','yyyyMMdd'), {{ interval_days }})
)

select
    l.event_date,
    l.channel,
    l.total_ltv,
    c.cac,
    case 
        when c.cac is null or c.cac = 0 then null
        else cast(l.total_ltv / c.cac as numeric(12,2))
    end as ltv_cac_ratio
from ltv_by_channel l
left join cac c
    on l.event_date = c.event_date
    and l.channel = c.channel
order by l.event_date, l.channel