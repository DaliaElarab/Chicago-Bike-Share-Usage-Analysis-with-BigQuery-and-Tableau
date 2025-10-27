/*
Author: Dalia El Araby
Project: Google Data Analytics Capstone Project
Topic: Chicago (Divvy) Bike-Share Usage Analysis
Tool: Google BigQuery
Date: 2023
Purpose: 
    - Merge all 2023 monthly trip data into one table
    - Clean and validate data
    - Analyze usage patterns between casual and annual riders
*/

-- Prepare the Data --
    
-- "Merge all months tables into one called "2023":
CREATE OR REPLACE TABLE `dalia.2023_divvy_tripdata.2023` AS
SELECT * FROM `dalia.2023_divvy_tripdata.202301`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202302`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202303`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202304-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202304-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202305-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202305-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202306-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202306-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202307-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202307-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202308-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202308-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202309-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202309-2`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202310-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202311-1`
UNION ALL
SELECT * FROM `dalia.2023_divvy_tripdata.202312`;


-- Check for Duplicates:
SELECT
    ride_id,
    COUNT(*) AS num_duplicates
FROM `dalia-first-da-project.2023_divvy_tripdata.2023`
GROUP BY ride_id, started_at, ended_at
HAVING num_duplicates > 1;


-- Check for missing values --
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(ride_id IS NULL) AS missing_ride_id,
    COUNTIF(rideable_type IS NULL) AS missing_rideable_type,
    COUNTIF(started_at IS NULL) AS missing_started_at,
    COUNTIF(ended_at IS NULL) AS missing_ended_at,
    COUNTIF(day_of_week IS NULL) AS missing_day_of_week,
    COUNTIF(ride_length IS NULL) AS missing_ride_legnth,
    COUNTIF(start_station_name IS NULL) AS missing_start_station_name,
    COUNTIF(start_station_id IS NULL) AS missing_start_station_id,
    COUNTIF(end_station_name IS NULL) AS missing_end_station_name,
    COUNTIF(end_station_id IS NULL) AS missing_end_station_id,
    COUNTIF(start_lat IS NULL) AS missing_start_lat,
    COUNTIF(start_lng IS NULL) AS missing_start_lng,
    COUNTIF(end_lat IS NULL) AS missing_end_lat,
    COUNTIF(end_lng IS NULL) AS missing_end_lng,
    COUNTIF(member_casual IS NULL) AS missing_member_casual
FROM
    `dalia-first-da-project.2023_divvy_tripdata.2023`;


-- Check for Wrong or Incorrect data --
ALTER TABLE `dalia-first-da-project.2023_divvy_tripdata.2023`
RENAME COLUMN ride_legnth TO ride_length;


--"Exclude station data (not critical)"--
--"Filter out records with missing values in end_lat and end_lng (critical)"--
CREATE OR REPLACE TABLE `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023` AS
SELECT 
  ride_id,
  rideable_type,
  started_at,
  ended_at,
  day_of_week,
  ride_length,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  member_casual
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.2023`
WHERE 
  ride_id IS NOT NULL
  AND rideable_type IS NOT NULL
  AND started_at IS NOT NULL
  AND ended_at IS NOT NULL
  AND day_of_week IS NOT NULL
  AND ride_length IS NOT NULL
  AND start_lat IS NOT NULL
  AND start_lng IS NOT NULL
  AND end_lat IS NOT NULL
  AND end_lng IS NOT NULL
  AND member_casual IS NOT NULL;


# Verify the Cleaned Data
SELECT *
FROM `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
LIMIT 10;


-- "Check for missing values":
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(ride_id IS NULL) AS missing_ride_id,
    COUNTIF(rideable_type IS NULL) AS missing_rideable_type,
    COUNTIF(started_at IS NULL) AS missing_started_at,
    COUNTIF(ended_at IS NULL) AS missing_ended_at,
    COUNTIF(day_of_week IS NULL) AS missing_day_of_week,
    COUNTIF(ride_length IS NULL) AS missing_ride_legnth,
    COUNTIF(start_lat IS NULL) AS missing_start_lat,
    COUNTIF(start_lng IS NULL) AS missing_start_lng,
    COUNTIF(end_lat IS NULL) AS missing_end_lat,
    COUNTIF(end_lng IS NULL) AS missing_end_lng,
    COUNTIF(member_casual IS NULL) AS missing_member_casual
FROM
    `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`;


#Analysis Metrics
--Number of Rides:
SELECT 
  member_casual, 
  COUNT(ride_id) AS total_rides
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;


--Rides over Time:
SELECT
  member_casual,
  EXTRACT(YEAR FROM started_at) AS year,
  EXTRACT(MONTH FROM started_at) AS month,
  COUNT(ride_id) AS total_rides
FROM
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY
  member_casual,
  year,
  month
ORDER BY
  year,
  month;


--Rideable Type Distribution:
SELECT 
  member_casual, 
  rideable_type, 
  COUNT(ride_id) AS total_rides
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  rideable_type;


--Ride Frequency:
SELECT 
  member_casual, 
  COUNT(ride_id) / COUNT(DISTINCT EXTRACT(MONTH FROM started_at)) AS avg_rides_per_month
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;


--Ride Duration (Average Ride Length):
SELECT 
  member_casual, 
  AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE)) AS avg_ride_length
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;


--Ride Start Times:
SELECT 
  member_casual, 
  EXTRACT(HOUR FROM started_at) AS hour, 
  COUNT(ride_id) AS rides
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  hour
ORDER BY 
  hour;


--Day of the Week Usage:
SELECT 
  member_casual, 
  day_of_week, 
  COUNT(ride_id) AS rides
FROM 
  `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  day_of_week
ORDER BY 
  day_of_week;


--Trip Distance (Calculate the average distance traveled using the Haversine formula):
SELECT
    member_casual,
    AVG(
        IF(
            start_lat BETWEEN -90 AND 90
            AND end_lat BETWEEN -90 AND 90
            AND start_lng BETWEEN -180 AND 180
            AND end_lng BETWEEN -180 AND 180,
            ST_DISTANCE(ST_GEOGPOINT(start_lng, start_lat), ST_GEOGPOINT(end_lng, end_lng)) / 1000,
            NULL  
        )
    ) AS avg_trip_distance_km
FROM `dalia-first-da-project.2023_divvy_tripdata.cleaned_2023`
GROUP BY member_casual;
