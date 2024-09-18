{{ config(materialized='table') }}
--tag this as full_reload refresh only
SELECT
    TO_VARCHAR(date, 'YYYYMMDD')::int as date_pk
    ,date::date as date
    ,year
    ,month
    ,day
    ,day_of_week
    ,is_weekend
FROM
    {{ ref('dim_date_br') }}