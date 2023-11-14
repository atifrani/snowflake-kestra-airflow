
-- Create database
CREATE OR REPLACE DATABASE DEMO_DB;

USE DEMO_DB;

-- Prepare table
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID INT AUTOINCREMENT START = 1 INCREMENT =1,
    FIRST_NAME VARCHAR(40) DEFAULT 'Axel' ,
    CREATE_DATE DATE);
    
    
-- Create task
CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    
-- List tasks
SHOW TASKS;

-- Task starting
ALTER TASK CUSTOMER_INSERT RESUME;

SHOW TASKS;

-- Task suspending
ALTER TASK CUSTOMER_INSERT SUSPEND;

-- List tasks
SHOW TASKS;

-- wait 1 minute before run the statement
SELECT * FROM CUSTOMERS;


-- Create another task using Cron
  
CREATE OR REPLACE TASK CUSTOMER_INSERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 7 * * * CET'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    
-- Cron format:
# __________ minute (0-59)
# | ________ hour (0-23)
# | | ______ day of month (1-31, or L)
# | | | ____ month (1-12, JAN-DEC)
# | | | | __ day of week (0-6, SUN-SAT, or L)
# | | | | |
# | | | | |
# * * * * *
 
SHOW TASKS;

SELECT * FROM CUSTOMERS;

-- Create a second table
CREATE OR REPLACE TABLE CUSTOMERS2 (
    CUSTOMER_ID INT,
    FIRST_NAME VARCHAR(40),
    CREATE_DATE DATE);
    
    
--  Suspend parent task
ALTER TASK CUSTOMER_INSERT SUSPEND;
    
-- Create a child task
CREATE OR REPLACE TASK CUSTOMER_INSERT2
    WAREHOUSE = COMPUTE_WH
    AFTER CUSTOMER_INSERT
    AS 
    INSERT INTO CUSTOMERS2 SELECT * FROM CUSTOMERS;
    
    
-- Create a third table
CREATE OR REPLACE TABLE CUSTOMERS3 (
    CUSTOMER_ID INT,
    FIRST_NAME VARCHAR(40),
    CREATE_DATE DATE,
    INSERT_DATE DATE DEFAULT DATE(CURRENT_TIMESTAMP));
    

-- Create a child task
CREATE OR REPLACE TASK CUSTOMER_INSERT3
    WAREHOUSE = COMPUTE_WH
    AFTER CUSTOMER_INSERT2
    AS 
    INSERT INTO CUSTOMERS3 (CUSTOMER_ID,FIRST_NAME,CREATE_DATE) SELECT * FROM CUSTOMERS2;


SHOW TASKS;

-- Alter task CUSTOMER_INSERT
ALTER TASK CUSTOMER_INSERT 
SET SCHEDULE = '1 MINUTE';

-- Resume tasks (first root task)
ALTER TASK CUSTOMER_INSERT RESUME;
ALTER TASK CUSTOMER_INSERT2 RESUME;
ALTER TASK CUSTOMER_INSERT3 RESUME;


SELECT * FROM CUSTOMERS2;

SELECT * FROM CUSTOMERS3;

-- Suspend tasks again
ALTER TASK CUSTOMER_INSERT SUSPEND;
ALTER TASK CUSTOMER_INSERT2 SUSPEND;
ALTER TASK CUSTOMER_INSERT3 SUSPEND;


-- CRON patterns examples:
-- Every minute
SCHEDULE = 'USING CRON * * * * * UTC';

-- Every day at 6am UTC timezone
SCHEDULE = 'USING CRON 0 6 * * * UTC';

-- Every hour starting at 9 AM and ending at 5 PM on Sundays 
SCHEDULE = 'USING CRON 0 9-17 * * SUN UTC';

-- TASK run every hour starting at 9 AM and ending at 5 PM on every day.
CREATE OR REPLACE TASK CUSTOMER_INSERT_CRON
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 9-17 * * * UTC'
    AS 
    INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);

ALTER TASK CUSTOMER_INSERT_CRON RESUME; 

SHOW TASKS;

ALTER TASK CUSTOMER_INSERT_CRON SUSPEND;

-- How to use store procedure in snowflake task:

-- Create a store procedure

CREATE OR REPLACE PROCEDURE CUSTOMERS_INSERT_PROCEDURE (CREATE_DATE varchar)
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var sql_command = 'INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(:1);'
        snowflake.execute(
            {
            sqlText: sql_command,
            binds: [CREATE_DATE]
            });
        return "Successfully executed.";
        $$;


-- Create task with store prodedure
CREATE OR REPLACE TASK CUSTOMER_TAKS_PROCEDURE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS CALL  CUSTOMERS_INSERT_PROCEDURE (CURRENT_TIMESTAMP);

SHOW TASKS;

--  Start  task
ALTER TASK CUSTOMER_TAKS_PROCEDURE RESUME;

SELECT * FROM CUSTOMERS;

--  Suspend  task
ALTER TASK CUSTOMER_TAKS_PROCEDURE SUSPEND;


-- How to debug tasks

-- Use the table function "TASK_HISTORY()"
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

-- See results for a specific Task in a given time
select *
from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 5,
    task_name=>'CUSTOMER_INSERT2'));
  
  


-- Cleaning scriptis:

-- Drop tasks:

DROP TASK IF EXISTS CUSTOMER_INSERT;

DROP TASK IF EXISTS CUSTOMER_INSERT2;

DROP TASK IF EXISTS CUSTOMER_INSERT3;


-- Drop tables:

DROP TABLE IF EXISTS CUSTOMERS;

DROP TABLE IF EXISTS CUSTOMERS2;

DROP TABLE IF EXISTS CUSTOMERS3;


-- Drop database:

DROP DATABASE IF EXISTS DEMO_DB;
