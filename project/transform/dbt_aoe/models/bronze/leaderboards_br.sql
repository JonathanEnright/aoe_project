{{ config(materialized='table') }}


WITH
    landing_data AS (
    SELECT
        VALUE AS json_col
        ,rsrc::VARCHAR as rsrc
        ,ldts::TIMESTAMP_NTZ(9) as ldts
    FROM
        {{ ref('v_relic_raw') }} 
)
SELECT DISTINCT
    value:disputes::int as disputes
    ,value:drops::int as drops
    ,value:highestrank::int as highestrank
    ,value:highestranklevel::int as highestranklevel
    ,value:highestrating::int as highestrating
    ,value:lastmatchdate::int as lastmatchdate
    ,value:leaderboard_id::int as leaderboard_id
    ,value:losses::int as losses
    ,value:rank::int as rank
    ,value:ranklevel::int as ranklevel
    ,value:ranktotal::int as ranktotal
    ,value:rating::int as rating
    ,value:regionrank::int as regionrank
    ,value:regionranktotal::int as regionranktotal
    ,value:statgroup_id::int as statgroup_id
    ,value:streak::int as streak
    ,value:wins::int as wins
    ,rsrc
    ,ldts
FROM
    landing_data
    ,LATERAL FLATTEN(INPUT => json_col:leaderboardStats)