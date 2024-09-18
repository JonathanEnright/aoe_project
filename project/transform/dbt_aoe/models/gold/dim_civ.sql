{{ config(materialized='table') }}
-- Either incremental, or read from silver table if that table is full history

SELECT DISTINCT
    MD5(civ) as civ_pk
    ,civ as civ_name
    ,''::TEXT as civ_weaknesses --placeholder, to be filled in later
    ,''::TEXT as civ_strengths --placeholder, to be filled in later
    ,CURRENT_DATE() AS load_date
FROM
    {{ ref('players_br') }}