DROP TABLE IF EXISTS GOLD.GOLD_FLIGHT_PHASE;
CREATE TABLE GOLD.GOLD_FLIGHT_PHASE (
    phase         VARCHAR(20),
    count         INTEGER,
    min_altitude  DOUBLE PRECISION,
    max_altitude  DOUBLE PRECISION,
    avg_vertical_rate DOUBLE PRECISION
)
