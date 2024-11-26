{{
    config(
        materialized='incremental',
        unique_key='fact_pk',
        on_schema_change='fail'
    )
}}


WITH cte AS (
SELECT
    pm.id as fact_pk
    ,dm.match_pk as match_fk
    ,dp.player_pk as player_fk
    ,dc.civ_pk as civ_fk
    ,dd.date_pk as date_fk
    ,pm.team
    ,pm.winner
    ,pm.match_rating_diff
    ,pm.new_rating
    ,pm.old_rating
    ,pm.source
    ,pm.file_date
    ,CURRENT_DATE() as load_date
FROM
    {{ ref('player_match_sr') }} as pm
INNER JOIN
    {{ ref('dim_civ') }} as dc
    ON pm.civ = dc.civ_name
INNER JOIN
    {{ ref('dim_match') }} as dm
    ON pm.game_id = dm.game_id
INNER JOIN
    {{ ref('dim_player') }} as dp
    ON pm.profile_id = dp.profile_id
INNER JOIN 
    {{ ref('dim_date') }} as dd
    ON dm.game_date = dd.date
)

SELECT * FROM cte
{% if is_incremental() %}
    where file_date > (select max(file_date) from {{ this }})
    or fact_pk IS NULL
{% endif %}
