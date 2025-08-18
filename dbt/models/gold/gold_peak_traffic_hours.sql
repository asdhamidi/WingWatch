SELECT
    al.airline_name as airline_name,
    EXTRACT('hour' FROM TO_TIMESTAMP(f.time_position)) AS hour,
    COUNT(*) AS flight_count,
    RANK() OVER (PARTITION BY al.airline_name  ORDER BY COUNT(*) DESC) AS rank
FROM {{ source('silver', 'silver_flights') }}f
LEFT JOIN {{ source('silver', 'silver_airlines') }} al
		ON SUBSTRING(f.CALLSIGN, 0, 4)=al.ICAO_CODE
WHERE
    f.created_at = (SELECT MAX(created_at) FROM {{ source('silver', 'silver_flights') }})
    AND NOT f.on_ground
    AND f.time_position IS NOT NULL
GROUP BY 1, 2
