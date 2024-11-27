{{
    config(
        materialized='incremental',
        unique_key='date_pk',
        on_schema_change='fail'
    )
}}


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