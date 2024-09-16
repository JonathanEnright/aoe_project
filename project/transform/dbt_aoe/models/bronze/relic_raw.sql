{{ config(materialized='view') }}

--NOTE: Due to the 2 list objects in the json schema, we will need to dedup the individual arrays
--This can be done with a SELECT DISTINCT
WITH
   landing_data AS (
   SELECT 
      *
      ,metadata$filename::VARCHAR AS rsrc
      ,metadata$file_last_modified::TIMESTAMP_NTZ(9) AS ldts
   FROM
      {{source('aoe_ext', 'relic_ext')}}
   )
SELECT 
   *
FROM 
   landing_data
