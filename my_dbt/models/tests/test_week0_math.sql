with cohorts as (

    select
        user_id,
        event_date as signup_date,
        cast(date_trunc('week', event_date) as date) as cohort_week
    from {{ ref('stg_events') }}
    where event_type = 'signup'

),

cohort_size as (

    select
        cohort_week,
        count(distinct user_id) as cohort_size
    from cohorts
    group by 1

),

week0_retained as (

    select
        c.cohort_week,
        count(distinct e.user_id) as retained_users
    from {{ ref('stg_events') }} e
    inner join cohorts c
        on e.user_id = c.user_id
    where e.event_date >= c.signup_date
      and e.event_date < date_add(c.signup_date, 7)
    group by 1

),

recalculated as (

    select
        w.cohort_week,
        cast(w.retained_users * 1.0 / cs.cohort_size as numeric(10,4)) as expected_week_0
    from week0_retained w
    join cohort_size cs
        on w.cohort_week = cs.cohort_week

),

final_comparison as (

    select
        r.cohort_week,
        r.week_0 as model_week_0,
        rc.expected_week_0,
        abs(r.week_0 - rc.expected_week_0) as diff
    from {{ ref('weekly_cohort_retention') }} r
    join recalculated rc
        on r.cohort_week = rc.cohort_week

)

select *
from final_comparison
where diff > 0.01