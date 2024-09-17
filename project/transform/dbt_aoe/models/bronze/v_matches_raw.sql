{{ config(materialized='view') }}


SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
FROM
   {{ source('aoe_ext', 'matches_ext') }}
 