DROP TABLE IF EXISTS GOLD.GOLD_APPROACHING_AIRPORTS;
CREATE TABLE GOLD.GOLD_APPROACHING_AIRPORTS (
    callsign VARCHAR(20),
    airline VARCHAR(50),
    airport_name VARCHAR(100),
    altitude_ft INTEGER,
    speed_knots INTEGER,
    distance_from_airport_km NUMERIC(10, 2),
    bearing_from_airport_deg INTEGER,
    longitude NUMERIC(9, 6),
    latitude NUMERIC(9, 6)
)
