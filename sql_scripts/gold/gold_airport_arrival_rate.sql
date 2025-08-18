DROP TABLE IF EXISTS GOLD.GOLD_AIRPORT_ARRIVAL_RATE;
CREATE TABLE GOLD.GOLD_AIRPORT_ARRIVAL_RATE (
    airport_name VARCHAR(255),
    airport_country VARCHAR(255),
    airport_type VARCHAR(32),
    airport_latitude FLOAT,
    airport_longitude FLOAT,
    approaching_flights INT,
    departing_flights INT,
    net_traffic INT
)
