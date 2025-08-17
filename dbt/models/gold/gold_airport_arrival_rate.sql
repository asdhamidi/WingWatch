{% set haversine_distance %}
    6371 * 2 * ASIN(
        SQRT(
            POWER(SIN(RADIANS(ap.latitude_deg - f.latitude)/2), 2) +
            COS(RADIANS(f.latitude)) *
            COS(RADIANS(ap.latitude_deg)) *
            POWER(SIN(RADIANS(ap.longitude_deg - f.longitude)/2), 2)
        )
    )
{% endset %}

WITH APPROACHING_FLIGHTS AS (
    SELECT
        ap.name AS airport,
        COUNT(DISTINCT f.icao24) AS approaching_flights
    FROM {{ source('silver', 'silver_flights') }} f
    JOIN {{ source('silver', 'silver_airports') }} ap ON
        {{ haversine_distance }} <= 50
    WHERE
        f.vertical_rate < 0
        AND NOT f.on_ground
    GROUP BY 1
),
DEPARTING_FLIGHTS AS (
    SELECT
        ap.name AS airport,
        COUNT(DISTINCT f.icao24) AS departing_flights
    FROM {{ source('silver', 'silver_flights') }} f
    JOIN {{ source('silver', 'silver_airports') }} ap ON
        {{ haversine_distance }} <= 50
    WHERE
        f.vertical_rate > 0
        AND NOT f.on_ground
    GROUP BY 1
)

SELECT
    a.airport,
    COALESCE(af.approaching_flights, 0) AS approaching_flights,
    COALESCE(df.departing_flights, 0) AS departing_flights,
    COALESCE(af.approaching_flights, 0) - COALESCE(df.departing_flights, 0) AS net_traffic
FROM
    APPROACHING_FLIGHTS A JOIN
    DEPARTING_FLIGHTS D
ON A.airport = D.airport
