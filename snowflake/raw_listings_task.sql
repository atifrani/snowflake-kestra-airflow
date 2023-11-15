-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Use COMPUTE_WH warehouse
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS airbnb;

USE airbnb;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE PROCEDURE IF NOT EXISTS CREATE_RAW_LISTINGS_PROC ( )
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        -- create table
        var create_stmt = " CREATE TABLE IF NOT EXISTS airbnb.bronze.raw_listings (id integer, listing_url string, name string, room_type string, minimum_nights integer, host_id integer, price string, created_at datetime, updated_at datetime);"
        -- create stram
        var stream_stmt = " CREATE STREAM IF NOT EXISTS airbnb.bronze.raw_listings_stream on table airbnb.bronze.raw_listings;"
        -- load csv file
        var load_stmt = "COPY INTO airbnb.bronze.raw_listings (id integer, listing_url string, name string, room_type string, minimum_nights integer, host_id integer, price string, created_at datetime, updated_at datetime) from 's3://logbrain-datasets/airbnb/listings.csv' FILE_FORMAT = (type = 'CSV' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ); "

        snowflake.execute( { sqlText: create_stmt });
        snowflake.execute( { sqlText: stream_stmt });
        snowflake.execute( { sqlText: load_stmt });

        return "Successfully executed.";
        $$;

SHOW PROCEDURES;

-- Create task with store prodedure
CREATE  TASK IF NOT EXISTS CREATE_RAW_LISTINGS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 MINUTE'AS 
CALL  CREATE_RAW_LISTINGS_PROC ();

SHOW TASKS;

--  Start  task
ALTER TASK CREATE_RAW_LISTINGS_TASK RESUME;

-- Check the result
SELECT * FROM airbnb.bronze.raw_listings;
