WITH emergencies AS (
    SELECT
		to_timestamp(last_contact) as event_time,
        icao24,
        callsign,
        squawk,
		latitude,
		longitude,
        CASE squawk
            WHEN '7500' THEN 'hijack'
            WHEN '7600' THEN 'radio_failure'
            WHEN '7700' THEN 'general_emergency'
        END AS emergency_type
    FROM {{ source('silver', 'silver_flights') }}
    WHERE squawk IN ('7500', '7600', '7700')
    AND created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
)

SELECT
    e.*,
    a.airline_name as airline_name,
	ROUND(CAST(6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude_deg - e.latitude)/2), 2) + COS(RADIANS(e.latitude)) * COS(RADIANS(ap.latitude_deg)) * POWER(SIN(RADIANS(ap.longitude_deg - e.longitude)/2), 2))) AS NUMERIC), 2) AS distance_from_airport_km
FROM emergencies e
LEFT JOIN {{ source('silver', 'silver_airlines') }} a
ON SUBSTRING(e.callsign, 1, 3) = a.iata_code
LEFT JOIN {{ source('silver', 'silver_airports') }} ap
ON ROUND(CAST(6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude_deg - e.latitude)/2), 2) + COS(RADIANS(e.latitude)) * COS(RADIANS(ap.latitude_deg)) * POWER(SIN(RADIANS(ap.longitude_deg - e.longitude)/2), 2))) AS NUMERIC), 2)  <= 50
WHERE ap.type IN ('large_airport','medium_airport')
