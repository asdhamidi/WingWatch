SELECT
    name,
    type,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    iso_country,
    scheduled_service,
    iata_code,
    CURRENT_TIMESTAMP AS CREATED_AT,
    CURRENT_USER AS CREATED_BY
FROM
    {{source('bronze', 'bronze_airports')}}
WHERE
    latitude_deg IS NOT NULL AND
    longitude_deg IS NOT NULL AND
    elevation_ft IS NOT NULL AND
    iso_country IS NOT NULL AND
    scheduled_service IS NOT NULL AND
    iata_code IS NOT NULL AND
    type IN ('large_airport','medium_airport') AND
    scheduled_service
