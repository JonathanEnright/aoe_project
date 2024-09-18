{{ config(materialized='table') }}


WITH
   landing_data AS (
   SELECT 
      *
   FROM
      {{ ref('v_players_raw') }}
   )
SELECT
   value:civ::VARCHAR AS civ
   ,value:game_id::INT AS game_id
   ,value:match_rating_diff::INT AS match_rating_diff
   ,value:new_rating::INT AS new_rating
   ,value:old_rating::INT AS old_rating
   ,value:profile_id::INT AS profile_id
   ,value:replay_summary_raw::ARRAY AS replay_summary_raw
   ,value:team::VARCHAR AS team
   ,value:winner::BOOLEAN AS winner
   ,rsrc::VARCHAR AS rsrc
   ,ldts::TIMESTAMP_NTZ(9) AS ldts
   ,source
FROM
   landing_data