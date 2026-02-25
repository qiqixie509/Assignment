{{config(
    materialized='view'
)}}

with raw_source as (
    select 
        cast(date as date) as date,
        from_currency,
        to_currency,
        rate
    from {{ source('raw_data', 'currency_rates') }}
)

select 
    date,
    from_currency,
    to_currency,
    rate
from raw_source