-- weekly_cohort_retention.sql
{{ config(
    materialized = 'table',
    incremental_strategy = 'merge',
    partition_by = ['cohort_week']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}

with cohorts as (
    select
        user_id,
        event_date,
        cast(date_trunc('week',event_date) as date) as cohort_week
    from {{ ref('stg_events') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
    and event_type = 'signup'
),
events as (
    select 
        user_id,
        event_date
    from {{ ref('stg_events') }}
    where event_date between TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd')
    AND DATE_ADD(TO_DATE(CAST('{{ start_date }}' AS STRING), 'yyyyMMdd'), {{ interval_days }})
),
events_with_weeks as (
    select 
        e.user_id,
        e.event_date,
        c.cohort_week,
        c.event_date as signup_date,
        floor(datediff(e.event_date, c.event_date) / 7) as week_number
    from events e
    left join cohorts c
    using (user_id)
    where e.event_date >= c.event_date
),
cohort_size as (
    select 
        cohort_week,
        count(user_id) as cohort_size
    from cohorts
    group by 1
),
retention_raw as (
    select 
        cohort_week,
        week_number,
        count(distinct user_id) as retained_users
    from events_with_weeks
    group by 1,2
),
retention_pivot as (
    select
        r.cohort_week,
        cast(max(case when week_number = 0 then retained_users end) * 1.0 
            / cs.cohort_size as numeric(10,2)) as week_0,
        cast(max(case when week_number = 1 then retained_users end) * 1.0 
            / cs.cohort_size as numeric(10,2)) as week_1,
        cast(max(case when week_number = 2 then retained_users end) * 1.0 
            / cs.cohort_size as numeric(10,2)) as week_2,
        cast(max(case when week_number = 3 then retained_users end) * 1.0 
            / cs.cohort_size as numeric(10,2)) as week_3,
        cast(max(case when week_number = 4 then retained_users end) * 1.0 
            / cs.cohort_size as numeric(10,2)) as week_4
    from retention_raw r
    left join cohort_size cs using (cohort_week)
    group by 1, cs.cohort_size
    order by 1
)

select * from retention_pivot

    
