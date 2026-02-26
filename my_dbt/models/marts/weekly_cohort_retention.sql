-- weekly_cohort_retention.sql
{{ config(
    materialized = 'table',
    partition_by = ['cohort_week']
) }}

{% set interval_days = var('interval', 1) %}
{% set start_date = var('start_date') %}
{% set max_weeks = 4 %}


with cohorts as (
    select
        user_id,
        event_date as signup_date,
        cast(date_trunc('week', event_date) as date) as cohort_week
    from {{ ref('stg_events') }}
    where event_type = 'signup'
      and event_date between 
            to_date('{{ start_date }}','yyyyMMdd')
        and date_add(to_date('{{ start_date }}','yyyyMMdd'), {{ interval_days }})
),

-- Events limited to retention horizon
events as (

    select 
        e.user_id,
        e.event_date,
        c.signup_date,
        c.cohort_week
    from {{ ref('stg_events') }} e
    inner join cohorts c
        on e.user_id = c.user_id
    where e.event_date >= c.signup_date
      and e.event_date <= date_add(c.signup_date, {{ max_weeks * 7 }})

),

-- Compute week number
events_with_weeks as (
    select 
        user_id,
        cohort_week,
        floor(datediff(event_date, signup_date) / 7) as week_number
    from events
),

-- Cohort size
cohort_size as (

    select 
        cohort_week,
        count(distinct user_id) as cohort_size
    from cohorts
    group by 1
),

-- Retained users per week
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
    left join cohort_size cs
        on r.cohort_week = cs.cohort_week
    group by 1, cs.cohort_size
    order by 1
)

select * from retention_pivot