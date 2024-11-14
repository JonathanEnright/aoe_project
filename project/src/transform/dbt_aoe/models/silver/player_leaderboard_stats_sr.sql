{{ config(materialized='table') }}

WITH
    stat_group_data AS (
        SELECT
            profile_id
            ,alias as gaming_name
            ,personal_statgroup_id
            ,country
        FROM
            {{ ref('statgroup_br') }}
    )
    ,country_codes AS (
        SELECT
            Country AS country_name
            ,Country_code
        FROM
            {{ ref('country_list') }}
    )
    ,leaderboard_data AS (
        SELECT
            statgroup_id
            ,wins
            ,losses
            ,rank as current_rank
            ,rating as current_rating
            ,TO_TIMESTAMP(lastmatchdate) as last_match_date
            ,ldts
            ,rsrc
            ,source
        FROM
            {{ ref('leaderboards_br') }}
    )
SELECT
    sgd.profile_id
    ,sgd.gaming_name
    ,sgd.country AS country_code
    ,COALESCE(cc.country_name, 'Unknown') AS country_name
    ,ld.statgroup_id
    ,ld.wins
    ,ld.losses
    ,ld.current_rank
    ,ld.current_rating
    ,ld.last_match_date
    ,ld.ldts
    ,ld.rsrc
    ,ld.source
FROM
    leaderboard_data as ld
INNER JOIN
    stat_group_data as sgd
    ON ld.statgroup_id = sgd.personal_statgroup_id
LEFT JOIN
    country_codes AS cc
    ON sgd.country = cc.country_code
