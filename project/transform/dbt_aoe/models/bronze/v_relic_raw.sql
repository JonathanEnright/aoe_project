{{ config(materialized='view') }}

--NOTE: Due to the 2 list objects in the json schema, we will need to dedup the individual arrays
SELECT DISTINCT
   *
   ,metadata$filename AS rsrc
   ,metadata$file_last_modified AS ldts
FROM
   {{ source('aoe_ext', 'relic_ext') }}
