DROP TABLE IF EXISTS GOLD.GOLD_SUPERSONIC_FLIGHTS;
CREATE TABLE GOLD.GOLD_SUPERSONIC_FLIGHTS (
    callsign VARCHAR(25),
    origin_country VARCHAR(100),
    baro_altitude NUMERIC(10, 2),
    longitude NUMERIC(10, 6),
    latitude NUMERIC(10, 6),
    velocity NUMERIC(10, 2)
)
