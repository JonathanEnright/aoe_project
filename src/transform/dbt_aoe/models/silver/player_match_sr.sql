{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

   
WITH cte as (
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
    ,file_date
FROM
    {{ ref('players_br') }}
WHERE
    profile_id IS NOT NULL --0.14% null from source, assume DQ issue.
)

SELECT * FROM cte
{% if is_incremental() %}
    where file_date > (select max(file_date) from {{ this }})
{% endif %}