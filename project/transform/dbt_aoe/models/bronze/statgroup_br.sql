{{ config(materialized='table') }}


WITH
    landing_data AS (
    SELECT
        VALUE AS json_col
        ,rsrc::VARCHAR as rsrc
        ,ldts::TIMESTAMP_NTZ(9) as ldts
        ,source
    FROM
        {{ ref('v_relic_raw') }}    
    )
    ,members_stats AS (
    SELECT DISTINCT
        value:id::int as id
        ,value:name::VARCHAR as group_name
        ,value:type::int as type
        ,value:members::array as members
        ,rsrc
        ,ldts
        ,source
    FROM
        landing_data
        ,LATERAL FLATTEN(INPUT => json_col:statGroups)
    )
    ,flattened AS (
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
    ,source
FROM
    members_stats
    ,LATERAL FLATTEN(INPUT => members)
    )
    ,deduplicated AS (
        {{ deduplicate_by_key('flattened', 'profile_id', 'ldts') }}
    )
SELECT
    *
FROM
    deduplicated