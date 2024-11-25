{{ config(materialized='view') }}


SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
   ,'AOESTATS'::VARCHAR as source
FROM
   {{ source('aoe_ext', 'players_ext') }}
WHERE 1=1
{{ filter_load('2024-09-01', 'aoe') }}