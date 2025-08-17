SELECT
    *,
    CURRENT_TIMESTAMP AS CREATED_AT,
    CURRENT_USER AS CREATED_BY
FROM
    {{source('bronze', 'bronze_airlines')}}
WHERE
    airline_name IS NOT NULL AND
    iata_code IS NOT NULL AND
    country IS NOT NULL
