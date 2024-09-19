--DBT Tests pass if no rows are returned.
--Hence, write the test assertion to find failing rows. 
SELECT
    country_name
FROM
    {{ ref('player_leaderboard_stats_sr') }}
WHERE
    country_name = 'Unknown'