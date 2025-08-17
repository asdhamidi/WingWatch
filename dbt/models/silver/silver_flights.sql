{{
    config(
        materialized='incremental'
    )
}}

SELECT
    *,
   CURRENT_TIMESTAMP AS CREATED_AT,
   CURRENT_USER AS CREATED_BY
FROM
    {{source('bronze', 'bronze_flights')}}
WHERE
    icao24 IS NOT NULL AND
    callsign IS NOT NULL AND
    time_position IS NOT NULL AND
    last_contact IS NOT NULL AND
    longitude IS NOT NULL AND
    latitude IS NOT NULL AND
    baro_altitude IS NOT NULL AND
    on_ground IS NOT NULL AND
    velocity IS NOT NULL AND
    vertical_rate IS NOT NULL AND
    geo_altitude IS NOT NULL
