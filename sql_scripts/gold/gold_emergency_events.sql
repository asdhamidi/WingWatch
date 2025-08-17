DROP TABLE IF EXISTS GOLD.GOLD_EMERGENCY_EVENTS;
CREATE TABLE GOLD.GOLD_EMERGENCY_EVENTS (
    event_time TIMESTAMPTZ,
    icao24 VARCHAR(10),
    airline_name VARCHAR(50),
    callsign VARCHAR(20),
    squawk VARCHAR(10),
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    emergency_type VARCHAR(50),
    distance_from_airport_km NUMERIC(10, 2)
)
