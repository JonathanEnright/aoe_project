{{ config(materialized='table') }}


WITH
   landing_data AS (
   SELECT 
      *
   FROM
      {{ ref('v_matches_raw') }}
   )
SELECT
   value: avg_elo::DEC(38,2) AS avg_elo
   ,value:duration::INT AS duration
   ,value:game_id::INT AS game_id
   ,value:game_speed::VARCHAR AS game_speed
   ,value:game_type::VARCHAR AS game_type
   ,value:irl_duration::INT AS irl_duration
   ,value:leaderboard::VARCHAR AS leaderboard
   ,value:map::VARCHAR AS map
   ,value:mirror::BOOLEAN AS mirror
   ,value:num_players::INT AS num_players
   ,value:patch::VARCHAR AS patch
   ,value:raw_match_type::VARCHAR AS raw_match_type
   ,value:replay_enhanced::BOOLEAN AS replay_enhanced
   ,value:started_timestamp::VARCHAR::TIMESTAMP_NTZ AS started_timestamp
   ,value:starting_age::VARCHAR AS starting_age
   ,value:team_0_elo::DEC(38,2) AS team_0_elo
   ,value:team_1_elo::DEC(38,2) AS team_1_elo
   ,rsrc::VARCHAR AS rsrc
   ,ldts::TIMESTAMP_NTZ(9) AS ldts
   ,source
   ,file_date
FROM
   landing_data