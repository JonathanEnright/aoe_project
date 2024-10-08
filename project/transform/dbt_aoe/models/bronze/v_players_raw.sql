{{ config(materialized='view') }}


SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
   ,'AOESTATS'::VARCHAR as source
FROM
   {{ source('aoe_ext', 'players_ext') }}
