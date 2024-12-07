{{
    config(
        materialized='incremental',
        unique_key='game_id',
        on_schema_change='fail'
    )
}}


with cte as (
SELECT
    game_id
    ,map
    ,game_type
    ,game_speed
    ,starting_age
    ,num_players
    ,avg_elo
    ,SUBSTR(duration::TEXT, 0, 4)::int as game_duration_secs
    ,SUBSTR(irl_duration::TEXT, 0, 4)::int as actual_duration_secs
    ,started_timestamp as game_started_timestamp
    ,started_timestamp::date as game_date
    ,team_0_elo::int as team_0_elo
    ,team_1_elo::int as team_1_elo
    ,leaderboard
    ,mirror
    ,patch
    ,rsrc
    ,ldts
    ,source
    ,file_date
FROM
    {{ ref('matches_br') }}
WHERE
    leaderboard = 'random_map' --filter for 1v1 RM's in this analysis
)

SELECT * FROM cte
{% if is_incremental() %}
    where file_date > (select max(file_date) from {{ this }})
{% endif %}
