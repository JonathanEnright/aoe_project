{{
    config(
        materialized='incremental',
        unique_key='match_pk',
        on_schema_change='fail'
    )
}}


WITH 
    deduplicated AS (
        {{ deduplicate_by_key('_ref(matches_sr)', 'CONCAT(game_id,source)', 'ldts') }}
    )
    ,cte AS (
SELECT
    MD5(CONCAT(game_id::TEXT,'~',source)) as match_pk
    ,game_id
    ,map
    ,avg_elo
    ,game_duration_secs
    ,actual_duration_secs
    ,game_started_timestamp
    ,game_date
    ,team_0_elo
    ,team_1_elo
    ,leaderboard
    ,mirror
    ,patch
    ,rsrc
    ,CURRENT_DATE() AS load_date
    ,source
    ,file_date
FROM
    deduplicated
)

SELECT * FROM cte
{% if is_incremental() %}
    where file_date > (select max(file_date) from {{ this }})
    or match_pk is NULL
{% endif %}
