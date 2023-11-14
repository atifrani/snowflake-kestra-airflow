-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Use COMPUTE_WH warehouse
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS airbnb;

USE airbnb;

CREATE SCHEMA IF NOT EXISTS silver;


CREATE OR REPLACE PROCEDURE CREATE_SRC_HOSTS_PROC ( )
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        -- create table
        var create_stmt = "CREATE OR REPLACE TABLE airbnb.silver.src_hosts (host_id integer, host_name string, is_superhost string, created_at datetime, updated_at datetime);"
        -- insert into table
        var load_stmt = " INSERT INTO airbnb.silver.src_hosts SELECT id AS host_id, NAME AS host_name, is_superhost, created_at, updated_at FROM tasks.bronze.raw_hosts_stream;"

        snowflake.execute( { sqlText: create_stmt });
        snowflake.execute( { sqlText: load_stmt });

        return "Successfully executed.";
        $$;

SHOW PROCEDURES;

-- Create a stream

-- Create task with store prodedure
CREATE OR REPLACE TASK CREATE_SRC_HOSTS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('tasks.bronze.raw_hosts_stream')
AS CALL  CREATE_SRC_HOSTS_PROC ();

SHOW TASKS;

--  Start  task
ALTER TASK CREATE_SRC_HOSTS_TASK RESUME;

SELECT * FROM airbnb.silver.src_hosts;

SELECT * FROM airbnb.bronze.raw_hosts_stream;

ALTER TASK CREATE_SRC_HOSTS_TASK SUSPEND;
