SELECT
	f.callsign,
	COALESCE(al.airline_name, 'unknown') AS airline,
	ap.name AS airport_name,
	f.baro_altitude AS altitude_ft,
	f.velocity AS speed_knots,
	ROUND(CAST(6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude_deg - f.latitude)/2), 2) + COS(RADIANS(f.latitude)) * COS(RADIANS(ap.latitude_deg)) * POWER(SIN(RADIANS(ap.longitude_deg - f.longitude)/2), 2))) AS NUMERIC), 2) AS distance_from_airport_km,
	MOD(CAST(DEGREES(ATAN2(SIN(RADIANS(f.longitude - ap.longitude_deg)) * COS(RADIANS(f.latitude)), COS(RADIANS(ap.latitude_deg)) * SIN(RADIANS(f.latitude)) - SIN(RADIANS(ap.latitude_deg)) * COS(RADIANS(f.latitude)) * COS(RADIANS(f.longitude - ap.longitude_deg)))) AS INTEGER), 360) AS bearing_from_airport_deg,
	f.longitude,
	f.latitude
FROM
	{{ source('silver', 'silver_flights') }}  f
	LEFT JOIN {{ source('silver', 'silver_airlines') }} al
		ON SUBSTRING(f.CALLSIGN, 0, 4)=al.ICAO_CODE
	LEFT JOIN SILVER.SILVER_AIRPORTS ap
		ON f.latitude BETWEEN ap.latitude_deg - 0.5 AND ap.latitude_deg + 0.5
		AND f.longitude BETWEEN ap.longitude_deg - 0.5 AND ap.longitude_deg + 0.5
		AND 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude_deg - f.latitude)/2), 2) + COS(RADIANS(f.latitude)) * COS(RADIANS(ap.latitude_deg)) * POWER(SIN(RADIANS(ap.longitude_deg - f.longitude)/2), 2))) <= 50
WHERE
	ap.type IN ('large_airport','medium_airport')
	AND f.on_ground IS FALSE
	AND ((f.vertical_rate < 0
	AND 6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude_deg - f.latitude)/2), 2) + COS(RADIANS(f.latitude)) * COS(RADIANS(ap.latitude_deg)) * POWER(SIN(RADIANS(ap.longitude_deg - f.longitude)/2), 2))) <= 30))
