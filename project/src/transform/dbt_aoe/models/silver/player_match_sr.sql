{{ config(materialized='table') }}

--change this to incremental
   
SELECT
    MD5(CONCAT(game_id,'~',profile_id)) as id
    ,game_id
    ,team
    ,profile_id
    ,civ
    ,winner
    ,match_rating_diff
    ,new_rating
    ,old_rating
    ,source
FROM
    {{ ref('players_br') }}