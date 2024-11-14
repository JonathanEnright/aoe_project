-- This macro accepts both CTE's and table references.
-- The parameter 'tbl' expects a string, however we can manipulate this to use the 'ref' macro
-- We can pass a ref{} table by specifying the parameter in the format '_ref({table_name})'
-- Example: '_ref(v_relic_raw)'
{% macro deduplicate_by_key(tbl, pk, date_field) %}
    SELECT
        *
    FROM 
        {% if tbl is string and tbl.startswith('_ref') %}
            {{ ref(tbl[5:-1]) }}
        {% else %}
            {{ tbl }}
        {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ pk }} ORDER BY {{ date_field }} DESC) = 1
{% endmacro %}