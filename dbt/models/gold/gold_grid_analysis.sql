SELECT
    FLOOR(latitude/0.5)*0.5 AS lat_band,
    FLOOR(longitude/0.5)*0.5 AS lon_band,
    FLOOR(baro_altitude/5000)*5000 AS alt_band,
    COUNT(*) AS traffic_count
FROM {{ source('silver', 'silver_flights') }}
WHERE
    NOT on_ground
    AND created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
GROUP BY 1,2,3
