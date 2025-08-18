SELECT
    callsign,
    origin_country,
    baro_altitude,
    longitude,
    latitude,
    velocity
FROM {{ source('silver', 'silver_flights') }}
WHERE
    callsign LIKE '%SST%' OR callsign LIKE '%SUP%'
    OR velocity > 661 AND baro_altitude > 30000
    AND created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
