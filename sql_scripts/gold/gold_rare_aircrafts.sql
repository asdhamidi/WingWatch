DROP TABLE IF EXISTS GOLD.GOLD_RARE_AIRCRAFTS;
CREATE TABLE GOLD.GOLD_RARE_AIRCRAFTS (
    callsign VARCHAR(25),
    origin_country VARCHAR(100),
    baro_altitude NUMERIC(10, 2),
    longitude NUMERIC(10, 6),
    latitude NUMERIC(10, 6),
    velocity NUMERIC(10, 2)
)
