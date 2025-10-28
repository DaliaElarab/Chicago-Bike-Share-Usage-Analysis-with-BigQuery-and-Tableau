/*
------------------------------------------------------------ 
Author: Dalia Elaraby
Project: Google Data Analytics Capstone Project
Topic: Chicago (Divvy) Bike-Share Usage Analysis
Tool: Google BigQuery
Data Source: https://divvy-tripdata.s3.amazonaws.com/index.html
Date: 2023
Purpose: 
    - Merge all 2023 monthly trip data into one table
    - Clean and validate data
    - Analyze usage patterns between casual and annual riders
Description:
This script documents the complete analytical workflow for the Google Data Analytics
Capstone Project (Cyclistic Bike-Share case study).  The workflow is divided into three
main stages:

1. DATA PREPARATION  
   - Combine monthly CSV files for 2023 into a single BigQuery table using UNION ALL.  
   - Handle upload limitations by splitting large files into smaller chunks.

2. SCRUB AND PROCESS (DATA CLEANING AND VALIDATION)  
   - Explore the merged dataset to check for duplicates, missing, and inconsistent data.  
   - Correct a misspelled column ("ride_legnth" → "ride_length") and filter incomplete rows.  
   - Create a cleaned dataset (`cleaned_2023`) for analysis.

3. ANALYSIS METRICS (EXPLORATION AND SUMMARY STATISTICS)  
   - Generate key indicators describing usage patterns across casual and member riders.
   - Export aggregated results as CSVs for visualization in Tableau.

Output:
   - A cleaned dataset ready for analysis.
   - Analytical tables summarizing ridership trends.
------------------------------------------------------------
*/

    
-- ============================================================
-- 1: DATA PREPARATION 
-- ============================================================

-- 1.1: Uploaded the original CSV files to local storage and split large datasets into smaller parts for successful upload to BigQuery. 
-- (Some monthly files exceeded BigQuery’s direct upload limit, so they were divided into smaller chunks before import.)

-- 1.2: Combine all monthly tables into a single 2023 dataset using UNION ALL.

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


-- ============================================================
-- 2: SCRUB AND PROCESS (DATA CLEANING AND VALIDATION)
-- ============================================================

-- 2.1: Explore

-- 2.1.1: Check for duplicates

SELECT
    ride_id,
    COUNT(*) AS num_duplicates
FROM `dalia.2023_divvy_tripdata.2023`
GROUP BY ride_id, started_at, ended_at
HAVING num_duplicates > 1;                                      -- No duplicates found.

-- 2.1.2: Check for missing values

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
    `dalia.2023_divvy_tripdata.2023`;                           -- Many missing values found.                         

-- 2.1.3: Identify incorrect data — the column name "ride_legnth" was misspelled.


-- 2.2: Clean

-- 2.2.1: Correct data inconsistencies
-- The column name "ride_legnth" was misspelled; it was renamed to "ride_length" for consistency.

ALTER TABLE `dalia.2023_divvy_tripdata.2023`
RENAME COLUMN ride_legnth TO ride_length;

-- 2.2.2: Handle missing values
-- Excluded station-related data (not critical for analysis).
-- Filtered out records with missing values in end_lat and end_lng (critical fields).

CREATE OR REPLACE TABLE `dalia.2023_divvy_tripdata.cleaned_2023` AS
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
  `dalia.2023_divvy_tripdata.2023`
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


-- 2.3: Verify the cleaned data

SELECT *
FROM `dalia.2023_divvy_tripdata.cleaned_2023`
LIMIT 10;

-- Check for missing values

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
    `dalia.2023_divvy_tripdata.cleaned_2023`;                  -- No missing values found.


-- ============================================================
-- 3: ANALYSIS METRICS (EXPLORATION AND SUMMARY STATISTICS)
-- ============================================================

-- 3.1: Exploration (Explore user behavior patterns and temporal trends)

-- 3.1.1: Number of Rides

SELECT 
  member_casual, 
  COUNT(ride_id) AS total_rides
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;

-- 3.1.2: Rides Over Time

SELECT
  member_casual,
  EXTRACT(YEAR FROM started_at) AS year,
  EXTRACT(MONTH FROM started_at) AS month,
  COUNT(ride_id) AS total_rides
FROM
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY
  member_casual,
  year,
  month
ORDER BY
  year,
  month;

-- 3.1.3: Rideable Type Distribution

SELECT 
  member_casual, 
  rideable_type, 
  COUNT(ride_id) AS total_rides
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  rideable_type;

-- 3.1.4: Ride Start Times

SELECT 
  member_casual, 
  EXTRACT(HOUR FROM started_at) AS hour, 
  COUNT(ride_id) AS rides
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  hour
ORDER BY 
  hour;

-- 3.1.5: Day of the Week Usage

SELECT 
  member_casual, 
  day_of_week, 
  COUNT(ride_id) AS rides
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual, 
  day_of_week
ORDER BY 
  day_of_week;


-- 3.2: Summary Statistics (Compute key metrics summarizing riding behavior).

-- 3.2.1: Ride Frequency

SELECT 
  member_casual, 
  COUNT(ride_id) / COUNT(DISTINCT EXTRACT(MONTH FROM started_at)) AS avg_rides_per_month
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;

-- 3.2.2: Ride Duration (Average Length)

SELECT 
  member_casual, 
  AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE)) AS avg_ride_length
FROM 
  `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY 
  member_casual;

-- 3.2.3: Trip Distance (Calculate the average distance traveled using the Haversine formula)

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
FROM `dalia.2023_divvy_tripdata.cleaned_2023`
GROUP BY member_casual;


-- To compare casual vs. member riders, the resulting tables were exported to Tableau for visualization and dashboard development. 
-- You can view the interactive dashboard here:
-- https://public.tableau.com/app/profile/dalia.elaraby/viz/BikeShareDadhboard/BikeShareAnalysis
