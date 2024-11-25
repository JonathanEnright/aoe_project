
{% macro filter_load(file_date, db='aoe') %}
    AND '{{ file_date }}' > (
        SELECT
            load_start_date
        FROM
            {{db}}.control.load_master
        WHERE
            project_name = '{{ db }}'
    )
    AND '{{ file_date }}' <= (
        SELECT
            load_end_date
        FROM
            {{ db }}.control.load_master
        WHERE
            project_name = '{{ db }}'  
    )
{% endmacro %}