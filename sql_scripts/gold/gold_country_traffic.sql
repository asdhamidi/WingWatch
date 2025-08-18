DROP TABLE IF EXISTS GOLD.GOLD_COUNTRY_TRAFFIC;
CREATE TABLE GOLD.GOLD_COUNTRY_TRAFFIC (
    airport_country VARCHAR(255),
    airport_type VARCHAR(32),
    avg_approaching_flights INT,
    avg_departing_flights INT,
    avg_net_traffic INT
)
