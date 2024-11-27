{% test recent_ldts(model, column_name, back_date_days) %}
{{ config(severity = 'warn') }}


WITH
    test_def as (
    SELECT
        MAX({{ column_name }}) as test_field
    FROM
        {{ model }}
    )
    ,perform_test as (
    SELECT
        *
    FROM
        test_def
    WHERE
        test_field >= (CURRENT_DATE()-{{back_date_days}})
    )
    ,validation_errors AS (
    SELECT
        COUNT(*) AS n_errors
    FROM
        perform_test
    )
SELECT
--If there is no max(ldts) > backdate_date, then fail
    CASE WHEN n_errors = 0 THEN 'fail' 
    ELSE 'pass'
    END AS result
FROM
    validation_errors
WHERE
    result <> 'pass' --Test expected to pass, trigger 'failed test' otherwise

{% endtest %}