-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Use COMPUTE_WH warehouse
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS airbnb;

USE airbnb;

CREATE SCHEMA IF NOT EXISTS silver;


CREATE OR REPLACE PROCEDURE CREATE_SRC_LISTINGS_PROC ( )
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        -- create table
        var create_stmt = "CREATE TABLE IF NOT EXISTS airbnb.silver.src_listings (listing_id integer, listing_url string, listing_name string, room_type string, minimum_nights integer, host_id integer, price string, created_at datetime, updated_at datetime);"
        -- insert into table
        var load_stmt = " INSERT INTO airbnb.silver.src_listings SELECT  id as listing_id, name as listing_name, room_type, CASE WHEN minimum_nights = 0 THEN 1 ELSE minimum_nights END AS minimum_nights, host_id, REPLACE( price, '$') :: NUMBER( 10, 2) AS price, created_at, updated_at FROM tasks.bronze.raw_listings_stream;"

        snowflake.execute( { sqlText: create_stmt });
        snowflake.execute( { sqlText: load_stmt });

        return "Successfully executed.";
        $$;

SHOW PROCEDURES;

-- Create a stream

-- Create task with store prodedure
CREATE OR REPLACE TASK CREATE_SRC_LISTINGS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('tasks.bronze.raw_listings_stream')
AS CALL  CREATE_SRC_LISTINGS_PROC ();

SHOW TASKS;

--  Start  task
ALTER TASK CREATE_SRC_LISTINGS_TASK RESUME;

SELECT * FROM airbnb.silver.src_listings

SELECT * FROM airbnb.bronze.raw_listings_stream;

