SELECT
    callsign,
    origin_country,
    baro_altitude,
    longitude,
    latitude,
    velocity
FROM {{ source('silver', 'silver_flights')}}
WHERE
    callsign ~ '^[A-Z]{2}F[A-Z0-9]{3}$'
    OR origin_country IN ('United States', 'Russia', 'China')
    AND (callsign LIKE '%AM%' OR callsign LIKE '%XX%')
    AND created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
