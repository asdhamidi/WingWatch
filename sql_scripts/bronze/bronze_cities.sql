DROP TABLE IF EXISTS BRONZE.BRONZE_CITIES;
CREATE TABLE BRONZE.BRONZE_CITIES (
    id              BIGINT,
    name            VARCHAR(255),
    state_id        BIGINT,
    state_code      VARCHAR(10),
    state_name      VARCHAR(255),
    country_id      BIGINT,
    country_code    CHAR(2),
    country_name    VARCHAR(255),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    wikiDataId      VARCHAR(32)
)
