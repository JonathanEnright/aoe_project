
{% macro filter_load(file_date) %}
{%- set env = target.name -%}

    AND {{ file_date }} > (
        SELECT
            load_start_date
        FROM
            aoe.control.load_master
        WHERE
            environment = '{{ env }}'
    )
    AND {{ file_date }} <= (
        SELECT
            load_end_date
        FROM
           aoe.control.load_master
        WHERE
            environment = '{{ env }}'  
    )
{% endmacro %}