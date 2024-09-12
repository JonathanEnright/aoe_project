{{ config(materialized='view') }}


WITH
   landing_data AS (
   SELECT 
      *
      ,metadata$filename AS rsrc
      ,metadata$file_last_modified AS ldts
   FROM
      {{source('aoe_ext', 'matches_ext')}}
   )
   ,extract_fields AS (
SELECT
   VALUE AS v
   ,v:avg_elo::DEC(38,2) AS avg_elo
   ,v:duration::INT AS duration
   ,v:game_id::INT AS game_id
   ,v:game_speed::VARCHAR AS game_speed
   ,v:game_type::VARCHAR AS game_type
   ,v:irl_duration::INT AS irl_duration
   ,v:leaderboard::VARCHAR AS leaderboard
   ,v:map::VARCHAR AS map
   ,v:mirror::BOOLEAN AS mirror
   ,v:num_players::INT AS num_players
   ,v:patch::VARCHAR AS patch
   ,v:raw_match_type::VARCHAR AS raw_match_type
   ,v:replay_enhanced::BOOLEAN AS replay_enhanced
   ,v:started_timestamp::INT AS started_timestamp
   ,v:starting_age::VARCHAR AS starting_age
   ,v:team_0_elo::DEC(38,2) AS team_0_elo
   ,v:team_1_elo::DEC(38,2) AS team_1_elo
   ,rsrc::VARCHAR AS rsrc
   ,ldts::TIMESTAMP_NTZ(9) AS ldts
FROM
   landing_data
)
SELECT 
   ef.*
   EXCLUDE (v)
FROM
   extract_fields as ef



-- {
--   "avg_elo": 1.349000000000000e+03,
--   "duration": 3085500000000,
--   "game_id": "326121997",
--   "game_speed": "normal",
--   "game_type": "random_map",
--   "irl_duration": 1815000000000,
--   "leaderboard": "random_map",
--   "map": "arabia",
--   "mirror": false,
--   "num_players": 2,
--   "patch": 111772,
--   "raw_match_type": 6,
--   "replay_enhanced": false,
--   "started_timestamp": 1720918659000000000,
--   "starting_age": "dark",
--   "team_0_elo": 1.344000000000000e+03,
--   "team_1_elo": 1.354000000000000e+03
-- }