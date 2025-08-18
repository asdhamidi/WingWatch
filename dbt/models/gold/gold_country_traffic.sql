select
    airport_country,
    airport_type,
    avg(net_traffic) AS avg_net_traffic,
    avg(approaching_flights) AS avg_approaching_flights,
    avg(departing_flights) AS avg_departing_flights
from {{ source('gold', 'gold_airport_arrival_rate') }}
    group by 1,2
