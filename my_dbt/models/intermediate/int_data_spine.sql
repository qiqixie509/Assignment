-- int_data_spine.sql
{{ config(materialized='table') }}

-- Set start and end dates
{% set start_date = '2026-01-02' %}
{% set end_date = '2026-02-09' %}

WITH dates AS (
    SELECT sequence(to_date('{{ start_date }}'), to_date('{{ end_date }}'), interval 1 day) AS date_array
),

date_spine AS (
    SELECT explode(date_array) AS date_day
    FROM dates
)

SELECT *
FROM date_spine
ORDER BY date_day