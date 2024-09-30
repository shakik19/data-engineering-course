--! Question 0
CREATE MATERIALIZED VIEW
    latest_dropoff_time AS
WITH
    t AS (
        SELECT
            MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM
            trip_data
    )
SELECT
    taxi_zone.Zone AS taxi_zone,
    latest_dropoff_time
FROM
    t,
    trip_data
    JOIN taxi_zone ON trip_data.DOLocationID = taxi_zone.location_id
WHERE
    trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--! Question 1
CREATE MATERIALIZED VIEW
    trip_times AS
SELECT
    EXTRACT(
        EPOCH
        FROM
            (
                trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime
            )
    ) AS trip_time,
    PULocationID,
    DOLocationID
FROM
    trip_data;

--? Main VIEW Query
CREATE MATERIALIZED VIEW
    trip_time_insights AS
SELECT
    pup.Zone AS pickup_zone,
    doff.Zone AS dropoff_zone,
    AVG(trip_times.trip_time) AS avg_trip_time,
    MAX(trip_times.trip_time) AS max_trip_time,
    MIN(trip_times.trip_time) AS min_trip_time
FROM
    trip_times
    JOIN taxi_zone AS pup ON trip_times.PULocationID = pup.location_id
    JOIN taxi_zone AS doff ON trip_times.DOLocationID = doff.location_id
WHERE
    pup.location_id != doff.location_id
GROUP BY
    1,
    2;

--? TESTING
SELECT
    *
FROM
    trip_time_insights
ORDER BY
    avg_trip_time DESC
LIMIT
    10;

--! Question 2
CREATE MATERIALIZED VIEW
    number_of_trips AS
SELECT
    pup.Zone AS pickup_zone,
    doff.Zone AS dropoff_zone,
    COUNT(trip_times.trip_time) AS trip_count,
    AVG(trip_times.trip_time) AS avg_trip_time
FROM
    trip_times
    JOIN taxi_zone AS pup ON trip_times.PULocationID = pup.location_id
    JOIN taxi_zone AS doff ON trip_times.DOLocationID = doff.location_id
WHERE
    pup.location_id != doff.location_id
GROUP BY
    1,
    2;

--? TESTING
SELECT
    *
FROM
    number_of_trips
ORDER BY
    avg_trip_time DESC
LIMIT
    10;

--! Question 3
CREATE MATERIALIZED VIEW
    busiest_zones AS
WITH
    pickups AS (
        SELECT
            taxi_zone.Zone AS pickup_zone,
            trip_data.tpep_pickup_datetime AS pickup_time
        FROM
            trip_data
            JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
    ),
    latest AS (
        SELECT
            MAX(tpep_pickup_datetime) AS pickup_time
        FROM
            trip_data
    )
SELECT
    pickups.pickup_zone,
    COUNT(*) AS pickup_counts
FROM
    latest,
    pickups
WHERE
    pickups.pickup_time > ((latest.pickup_time) - INTERVAL '17' HOUR)
GROUP BY
    1;

--? TESTING
SELECT
    *
FROM
    busiest_zones
ORDER BY
    pickup_counts DESC
LIMIT
    10;