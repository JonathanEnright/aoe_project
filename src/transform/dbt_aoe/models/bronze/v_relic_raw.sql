{{ config(materialized='view') }}


--NOTE: Due to the 2 list objects in the json schema, we will need to dedup the individual arrays
SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
   ,'RELIC_LINK_API'::VARCHAR as source
FROM
   {{ source('aoe_ext', 'relic_ext') }}