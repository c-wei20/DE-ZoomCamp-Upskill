#Q1
docker run -it --entrypoint=bash python:3.12.8 / docker run -it python:3.12.8 bash
pip --version

OR

docker run -it python:3.12.8 pip --version
#Ans = 24.3.1

#Q2
#Ans postgres:5432 and db:5432
the port should use the port exposed by the postgres container not the forwarding port.

#Q3
select
    case
        when trip_distance <= 1 then 'Up to 1 mile'
        when trip_distance > 1 and trip_distance <= 3 then '1 to 3 miles'
        when trip_distance > 3 and trip_distance <= 7 then '3 to 7 miles'
        when trip_distance > 7 and trip_distance <= 10 then '7 to 10 miles'
        else 'Over 10 miles'
    end as segment,
    count(1) as num_trips
from
    green_taxi_data
where
    lpep_pickup_datetime >= '2019-10-01'
    and lpep_pickup_datetime < '2019-11-01'
    and lpep_dropoff_datetime >= '2019-10-01'
    and lpep_dropoff_datetime < '2019-11-01'
group by 
    segment

#Ans: 104,802; 198,924; 109,603; 27,678; 35,189

#Q4
WITH longest_daily_trips AS (
	SELECT
		lpep_pickup_datetime::date AS pickup_date,
		MAX(trip_distance) AS longest_trip_distance
	FROM green_taxi_data
	WHERE lpep_pickup_datetime::date IN ('2019-10-11', '2019-10-24', '2019-10-26', '2019-10-31')
	GROUP BY pickup_date
)

SELECT
	pickup_date
FROM longest_daily_trips
ORDER BY longest_trip_distance DESC
LIMIT 1

#Ans: 2019-10-31

#Q5
SELECT
	trips."PULocationID",
	zones."Zone",
	SUM(trips.total_amount) AS total_amount,
	COUNT(1) AS num_pickup
FROM green_taxi_data AS trips
LEFT JOIN taxi_zone_look_up AS zones
	   ON trips."PULocationID" = zones."LocationID"
WHERE trips.lpep_pickup_datetime::date = '2019-10-18'
GROUP BY 1,2
HAVING SUM(trips.total_amount) > 13000
ORDER BY num_pickup DESC

#Ans : East Harlem North, East Harlem South, Morningside Heights

#Q6
SELECT
	pu_zones."Zone" AS pick_up_zone,
	do_zones."Zone" AS drop_off_zone,
	trips.tip_amount
FROM green_taxi_data AS trips
LEFT JOIN taxi_zone_look_up AS pu_zones
	   ON trips."PULocationID" = pu_zones."LocationID"
LEFT JOIN taxi_zone_look_up AS do_zones
	   ON trips."DOLocationID" = do_zones."LocationID"
WHERE trips.lpep_pickup_datetime::date >= '2019-10-01'
  AND trips.lpep_pickup_datetime::date < '2019-11-01'
  AND pu_zones."Zone" = 'East Harlem North'
ORDER BY trips.tip_amount DESC
LIMIT 1

#Ans: JFK Airport

#Q7

