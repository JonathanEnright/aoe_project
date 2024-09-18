{{ config(materialized='table') }}
--This should be loaded as incremental


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
    ,TO_TIMESTAMP(started_timestamp/1000000000) as game_started_timestamp
    ,TO_TIMESTAMP(started_timestamp/1000000000)::date as game_date
    ,team_0_elo::int as team_0_elo
    ,team_1_elo::int as team_1_elo
    ,leaderboard
    ,mirror
    ,patch
    ,rsrc
    ,ldts
    ,source
FROM
    {{ ref('matches_br') }}
WHERE
    leaderboard = 'random_map' --filter for 1v1 RM's in this analysis