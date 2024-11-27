{{ config(materialized='table') }}


WITH 
    deduplicated AS (
        {{ deduplicate_by_key('_ref(player_leaderboard_stats_sr)', 'CONCAT(profile_id,source)', 'ldts') }}
    )
SELECT
    MD5(CONCAT(profile_id::TEXT,'~',source)) as player_pk
    ,profile_id
    ,gaming_name
    ,country_code
    ,country_name
    ,statgroup_id
    ,wins
    ,losses
    ,current_rank
    ,current_rating
    ,last_match_date
    ,CURRENT_DATE() AS load_date
    ,source
FROM
    deduplicated