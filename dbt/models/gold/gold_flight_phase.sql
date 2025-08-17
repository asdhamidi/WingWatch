WITH FLIGHT_PHASE AS (
    SELECT
        baro_altitude,
		vertical_rate,
        CASE
            WHEN on_ground THEN 'on_ground'
            WHEN vertical_rate > 500 AND baro_altitude < 1000 THEN 'takeoff'
            WHEN vertical_rate > 300 AND baro_altitude BETWEEN 1000 AND 10000 THEN 'climb'
            WHEN vertical_rate BETWEEN -300 AND 300 AND baro_altitude >= 10000 THEN 'cruise'
            WHEN vertical_rate < -300 AND baro_altitude BETWEEN 10000 AND 3000 THEN 'descent'
            WHEN vertical_rate < -500 AND baro_altitude < 3000 THEN 'final_approach'
            ELSE 'other'
        END AS phase
    FROM silver.silver_flights
    WHERE baro_altitude IS NOT NULL
      AND vertical_rate IS NOT NULL
)

SELECT
    phase,
    COUNT(*) AS count,
	MIN(baro_altitude) AS min_altitude,
    MAX(baro_altitude) AS max_altitude,
    AVG(vertical_rate) AS avg_vertical_rate
FROM
    FLIGHT_PHASE
GROUP BY
    phase
