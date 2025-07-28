DROP TABLE IF EXISTS BRONZE.BRONZE_FLIGHTS;
CREATE TABLE BRONZE.BRONZE_FLIGHTS (
    icao24           VARCHAR(10),
    callsign         VARCHAR(20),
    origin_country   VARCHAR(100),
    time_position    BIGINT,
    last_contact     BIGINT,
    longitude        DOUBLE PRECISION,
    latitude         DOUBLE PRECISION,
    baro_altitude    DOUBLE PRECISION,
    on_ground        BOOLEAN,
    velocity         DOUBLE PRECISION,
    true_track       DOUBLE PRECISION,
    vertical_rate    DOUBLE PRECISION,
    sensors         JSONB,
    geo_altitude     DOUBLE PRECISION,
    squawk           VARCHAR(10),
    spi              BOOLEAN,
    position_source  SMALLINT
)
