with recalculated as (

    select
        user_id,
        coalesce(sum(
            case
                when event_type = 'purchase' then amount_in_euros - coalesce(tax_in_euros,0)
                when event_type = 'refund' then -amount_in_euros
                else 0
            end
        ),0) as expected_ltv
    from {{ ref('int_amounts_to_euros') }}
    group by 1

)

select
    l.user_id,
    l.ltv,
    r.expected_ltv
from {{ ref('ltv_per_user') }} l
join recalculated r using (user_id)
where abs(l.ltv - r.expected_ltv) > 0.01