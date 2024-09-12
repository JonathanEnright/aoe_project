{{ config(materialized='view') }}


WITH
   landing_data AS (
   SELECT 
      *
      ,metadata$filename AS rsrc
      ,metadata$file_last_modified AS ldts
   FROM
      {{source('aoe_ext', 'players_ext')}}
   )
 ,extract_fields AS (
SELECT
   VALUE AS v
   ,v:civ::VARCHAR AS civ
   ,v:game_id::INT AS game_id
   ,v:match_rating_diff::INT AS match_rating_diff
   ,v:new_rating::INT AS new_rating
   ,v:old_rating::INT AS old_rating
   ,v:profile_id::INT AS profile_id
   ,v:replay_summary_raw::ARRAY AS replay_summary_raw
   ,v:team::VARCHAR AS team
   ,v:winner::BOOLEAN AS winner
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
--   "civ": "franks",
--   "game_id": "324635668",
--   "match_rating_diff": -1.000000000000000e+01,
--   "new_rating": 1100,
--   "old_rating": 1084,
--   "profile_id": 19281471,
--   "replay_summary_raw": "{}",
--   "team": 0,
--   "winner": true
-- }