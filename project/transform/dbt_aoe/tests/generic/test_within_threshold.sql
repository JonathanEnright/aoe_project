{% test within_threshold(model, column_name, lower_threshold, upper_threshold, error_lim) %}

WITH
    test_def as (
    SELECT
        {{ column_name }} as test_field
    FROM
        {{ model }}
    )
    ,perform_test as (
    SELECT
        *
    FROM
        test_def
    WHERE
        test_field <= {{ lower_threshold }}
    OR  test_field >= {{ upper_threshold }}
    )
    ,validation_errors AS (
    SELECT
        COUNT(*) AS n_errors
    FROM
        perform_test
    )
SELECT
    CASE WHEN n_errors > {{error_lim}} THEN 'fail' 
    ELSE 'pass'
    END AS result
FROM
    validation_errors
WHERE
    result <> 'pass' --Test expected to pass, trigger 'failed test' otherwise

{% endtest %}