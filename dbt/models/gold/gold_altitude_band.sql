
SELECT
    FLOOR(baro_altitude/1000)*1000 AS altitude_band,
    COUNT(*) AS flight_count
FROM {{ source('silver', 'silver_flights') }}
WHERE NOT on_ground
GROUP BY 1
