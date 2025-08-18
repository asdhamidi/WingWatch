with country_data as (
	select
		country_name,
		country_code,
		avg(longitude) as country_longitude,
		avg(latitude) as country_latitude
	from {{ source('silver', 'silver_cities') }}
	group by 1, 2
)
select
    gaar.airport_country as airport_country,
    cd.country_longitude as country_longitude,
    cd.country_latitude as country_latitude,
    avg(gaar.net_traffic) AS avg_net_traffic,
    avg(gaar.approaching_flights) AS avg_approaching_flights,
    avg(gaar.departing_flights) AS avg_departing_flights
from {{ source('gold', 'gold_airport_arrival_rate') }} gaar
join country_data cd
on gaar.airport_country = cd.country_code
    group by 1,2,3
