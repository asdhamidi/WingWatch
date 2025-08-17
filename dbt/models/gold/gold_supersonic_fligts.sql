SELECT *
FROM {{ source('silver', 'silver_flights') }}
WHERE
    aircraft_type IN ('Concorde', 'Tu-144', 'Boom Overture') OR
    (callsign LIKE '%SST%' OR callsign LIKE '%SUP%') OR
    (velocity > 661 AND baro_altitude > 30000)
