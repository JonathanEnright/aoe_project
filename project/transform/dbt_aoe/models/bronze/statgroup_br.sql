{{ config(materialized='table') }}


WITH
    raw_data AS (
    SELECT
        VALUE AS json_col
        ,rsrc
        ,ldts
    FROM
        relic_raw   
    )
    ,members_stats AS (
    SELECT DISTINCT
        value:id::int as id
        ,value:name::VARCHAR as group_name
        ,value:type::int as type
        ,value:members::array as members
        ,rsrc
        ,ldts
    FROM
        raw_data
        ,LATERAL FLATTEN(INPUT => json_col:statGroups)
    )
SELECT 
    id
    ,group_name
    ,type
    ,value:alias::VARCHAR as alias
    ,value:country::VARCHAR as country
    ,value:leaderboardregion_id::int as leaderboardregion_id
    ,value:level::int as level
    ,value:name::VARCHAR as name
    ,value:personal_statgroup_id::int as personal_statgroup_id
    ,value:profile_id::int as profile_id
    ,value:xp::int as xp
    ,rsrc
    ,ldts
FROM
    members_stats
    ,LATERAL FLATTEN(INPUT => members)