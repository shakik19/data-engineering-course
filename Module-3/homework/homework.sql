-- Preparation

-- Creating an external table using a file in gcs
CREATE OR REPLACE EXTERNAL TABLE 
  `ny_taxi.green_taxi_data_2022`
OPTIONS (
  format ='PARQUET',
  uris = ['gs://(MY_BUCKET_NAME)/2022_green_taxi_data.parquet']
);


-- Creating a non-partitioned table from the external table created previously
CREATE OR REPLACE TABLE
  `ny_taxi.green_taxi_data_2022_non_partitioned` AS
SELECT
  *
FROM
  `ny_taxi.green_taxi_data_2022`;


-- Question 1
-- Answer -> 840392
SELECT
  COUNT(1)
FROM `ny_taxi.green_taxi_data_2022` src
WHERE
  DATE(src.lpep_pickup_datetime)
    BETWEEN '2022-01-01' AND '2022-12-31';


-- Question 2
-- Answer -> A
-- External 0B & Materialized 6.14MB
SELECT
  -- DISTINCT(ext.PULocationID) `1`,
  DISTINCT(mat.PULocationID) `2`
FROM
  `ny_taxi.green_taxi_data_2022` ext,
  `ny_taxi.green_taxi_data_2022_non_partitioned` mat;


-- Question 3
-- Answer -> 1622
SELECT
  COUNT(1)
FROM
  `ny_taxi.green_taxi_data_2022_non_partitioned` src
WHERE
  src.fare_amount = 0;


-- Question 4
-- Answer -> Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT
  *
FROM
  `ny_taxi.green_taxi_data_2022_non_partitioned`;


-- Question 5
-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
SELECT
  DISTINCT(src.PULocationID)
FROM
  `ny_taxi.green_taxi_data_2022_non_partitioned` src
  -- `ny_taxi.green_tripdata_partitoned_clustered` src
WHERE
  DATE(src.lpep_pickup_datetime)
    BETWEEN '2022-06-01' AND '2022-06-30';

-- Optional Question 8
SELECT
  *
FROM
  `ny_taxi.green_taxi_data_2022_non_partitioned` src;