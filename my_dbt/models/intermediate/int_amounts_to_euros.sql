-- int_amounts_to_euros.sql
{{ config(
    materialized='table',
    incremental_strategy='merge',
    partition_by = ['event_date']
)}}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}


with currency_rates as (
    select 
        from_currency,
        rate,
        date
    from {{ ref('stg_currency_rates') }}
    where date BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})

),
purchase_refund_events as (
    select 
        event_id,
        user_id,
        amount,
        channel,
        tax,
        currency,
        event_type,
        o_ts,
        event_date
    from {{ ref('stg_events') }}
    where event_date BETWEEN TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    AND event_type in ('purchase', 'refund')
),
amounts_to_euros as (
    select 
        e.event_id,
        e.user_id,
        e.amount,
        e.channel,
        e.tax,
        e.currency,
        e.event_type,
        e.o_ts,
        e.event_date,
        r.rate,
        case 
            when e.currency = 'EUR' then cast(e.amount as numeric(12,2))
            else cast(round(e.amount * r.rate, 2) as numeric(12,2))
        end as amount_in_euros,
        case 
            when e.currency = 'EUR' then cast(e.tax as numeric(12,2))
            else cast(round(e.tax * r.rate, 2) as numeric(12,2))
        end as tax_in_euros
    from purchase_refund_events e
    left join 
    currency_rates r
    on e.currency = r.from_currency
    and e.event_date = r.date
)

select * from amounts_to_euros

