
SELECT
    FLOOR(baro_altitude/1000)*1000 AS altitude_band,
    COUNT(*) AS flight_count
FROM {{ source('silver', 'silver_flights') }}
WHERE
    NOT on_ground
    AND created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
GROUP BY 1
