SELECT
    *,
    CURRENT_TIMESTAMP AS CREATED_AT,
    CURRENT_USER AS CREATED_BY
FROM
    {{source('bronze', 'bronze_cities')}}
