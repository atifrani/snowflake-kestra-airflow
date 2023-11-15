-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Use COMPUTE_WH warehouse
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS airbnb;

USE airbnb;

CREATE SCHEMA IF NOT EXISTS gold;


CREATE PROCEDURE IF NOT EXISTS CREATE_DMT_HOSTS_LISTINGS_PROC ( )
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        -- create table
        var create_stmt = "CREATE TABLE IF NOT EXISTS airbnb.gold.src_listings (listing_id integer, listing_url string, listing_name string, room_type string, minimum_nights integer, host_id integer, hostname string, host_is_superhost string, price string, created_at datetime, updated_at datetime);"
        -- insert into table
        var load_stmt = " INSERT INTO airbnb.gold.src_listings SELECT  sls.listing_id, sls.listing_name, sls.room_type, sls.minimum_nights, sls.host_id, sht.hostname, sht.host_is_superhost, sls.price, sls.created_at, sls.updated_at FROM airbnb.silver.src_listings_stream sls inner join airbnb.silver.src_hosts sht on sls.host_id = sht.host_id;"

        snowflake.execute( { sqlText: create_stmt });
        snowflake.execute( { sqlText: load_stmt });

        return "Successfully executed.";
        $$;

SHOW PROCEDURES;


-- Create task with store prodedure
CREATE TASK IF NOT EXISTS CREATE_SRC_LISTINGS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('airbnb.silver.src_listings_stream')
AS CALL  CREATE_SRC_LISTINGS_PROC ();

SHOW TASKS;

--  Start  task
ALTER TASK CREATE_SRC_LISTINGS_TASK RESUME;

SELECT * FROM airbnb.silver.src_listings

SELECT * FROM airbnb.bronze.raw_listings_stream;

