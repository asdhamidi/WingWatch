DROP TABLE IF EXISTS SILVER.SILVER_AIRPORTS;
CREATE TABLE SILVER.SILVER_AIRPORTS (
    name              VARCHAR(255),
    type              VARCHAR(32),
    latitude_deg      DOUBLE PRECISION,
    longitude_deg     DOUBLE PRECISION,
    elevation_ft      INTEGER,
    iso_country       CHAR(2),
    scheduled_service BOOLEAN,
    iata_code         VARCHAR(10),
   	created_at TIMESTAMPTZ,
	created_by VARCHAR(25)
)
