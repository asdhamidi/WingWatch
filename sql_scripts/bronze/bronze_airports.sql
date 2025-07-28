DROP TABLE IF EXISTS BRONZE.BRONZE_AIRPORTS;
CREATE TABLE BRONZE.BRONZE_AIRPORTS (
    id                BIGINT,
    ident             VARCHAR(20),
    type              VARCHAR(32),
    name              VARCHAR(255),
    latitude_deg      DOUBLE PRECISION,
    longitude_deg     DOUBLE PRECISION,
    elevation_ft      INTEGER,
    continent         CHAR(2),
    iso_country       CHAR(2),
    iso_region        VARCHAR(10),
    municipality      VARCHAR(255),
    scheduled_service BOOLEAN,
    icao_code         VARCHAR(10),
    iata_code         VARCHAR(10),
    gps_code          VARCHAR(10),
    local_code        VARCHAR(10),
    home_link         TEXT,
    wikipedia_link    TEXT,
    keywords          TEXT
)
