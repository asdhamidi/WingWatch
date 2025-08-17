SELECT
    al.airline_name as airline_name,
    EXTRACT('hour' FROM TO_TIMESTAMP(f.time_position)) AS hour,
    COUNT(*) AS flight_count,
    RANK() OVER (PARTITION BY al.airline_name  ORDER BY COUNT(*) DESC) AS rank
FROM {{ source('silver', 'silver_flights') }}f
LEFT JOIN {{ source('silver', 'silver_airlines') }} al
		ON SUBSTRING(f.CALLSIGN, 0, 4)=al.ICAO_CODE
GROUP BY 1, 2
