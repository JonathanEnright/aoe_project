{{ config(materialized='view') }}


SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
   ,'AOESTATS'::VARCHAR as source
   ,SPLIT(metadata$filename, '/')[2]::DATE AS file_date
FROM
   {{ source('aoe_ext', 'matches_ext') }}
WHERE 1=1
   {{ filter_load('file_date') }}