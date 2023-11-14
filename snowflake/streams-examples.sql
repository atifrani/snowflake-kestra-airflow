-- Create database
CREATE OR REPLACE DATABASE DEMO_DB;

USE DEMO_DB;

-- Create table sales_table
create or replace table sales_table
( id varchar,
 product varchar,
 price varchar,
 amount varchar,
 store_id varchar);

-- insert values 
insert into sales_table values
(1,'Banana',1.99,1,1),
(2,'Lemon',0.99,1,1),
(3,'Apple',1.79,1,2),
(4,'Orange Juice',1.89,1,2),
(5,'Cereals',5.98,2,1);  

-- Create table store_table
create or replace table store_table
(store_id number,
location varchar,
employees number);

-- insert values 
INSERT INTO STORE_TABLE VALUES
(1,'Paris',33)
(2,'Dijon',12);


-- Create table sales_fct_table
create or replace table sales_fct_table
(id int,
product varchar,
price number,
amount int,
store_id int,
location varchar,
employees int);

 -- Insert into final table
INSERT INTO sales_fct_table 
SELECT 
SA.id,
SA.product,
SA.price,
SA.amount,
ST.STORE_ID,
ST.LOCATION, 
ST.EMPLOYEES 
FROM sales_table SA
JOIN STORE_TABLE ST ON ST.STORE_ID=SA.STORE_ID ;

-- Create a stream object on top of sales_table ** INSERT **
create or replace stream sales_stream on table sales_table;

SHOW STREAMS;

DESC STREAM sales_stream;

-- Get changes on data using stream (INSERTS)
select * from sales_stream;

select * from sales_table;

-- insert values 
insert into sales_table values
(6,'Mango',1.99,1,2),
(7,'Garlic',0.99,1,1);

-- Get changes on data using stream (INSERTS)
select * from sales_stream;

select * from sales_table;

select * from sales_final_table;  

-- Consume stream object
INSERT INTO sales_final_table 
SELECT 
SA.id,
SA.product,
SA.price,
SA.amount,
ST.STORE_ID,
ST.LOCATION, 
ST.EMPLOYEES 
FROM SALES_STREAM SA
JOIN STORE_TABLE ST ON ST.STORE_ID=SA.STORE_ID ;

-- Get changes on data using stream (INSERTS)
select * from sales_stream;

-- *** INSERT ***

-- insert new values 
insert into sales_table  values
(8,'Paprika',4.99,1,2),
(9,'Tomato',3.99,1,2);

-- Consume stream object
INSERT INTO sales_final_table 
SELECT 
SA.id,
SA.product,
SA.price,
SA.amount,
ST.STORE_ID,
ST.LOCATION, 
ST.EMPLOYEES 
FROM SALES_STREAM SA
JOIN STORE_TABLE ST ON ST.STORE_ID=SA.STORE_ID ;

SELECT * FROM SALES_FINAL_TABLE;

SELECT * FROM SALES_TABLES;

SELECT * FROM SALES_STREAM;


-- *** UPDATE ***

-- Update value
UPDATE SALES_TABLES
SET PRODUCT ='Potato' WHERE PRODUCT = 'Banana';

-- Check stream values
SELECT * FROM SALES_STREAM;

merge into SALES_FINAL_TABLE F  -- Target table to merge changes from source table
using SALES_STREAM S  -- Stream that has captured the changes
   on  f.id = s.id 
when matched 
and S.METADATA$ACTION ='INSERT'
and S.METADATA$ISUPDATE ='TRUE' -- Indicates the record has been updated 
then update 
set f.product = s.product,
f.price = s.price,
f.amount= s.amount,
f.store_id=s.store_id;


SELECT * FROM SALES_FINAL_TABLE;

SELECT * FROM SALES_TABLES;

SELECT * FROM SALES_STREAM;

-- *** DELETE  ***

SELECT * FROM SALES_FINAL_TABLE;

SELECT * FROM SALES_TABLES;  

SELECT * FROM SALES_STREAM;

DELETE FROM SALES_RAW_STAGING
WHERE PRODUCT = 'Lemon';

-- *** Process stream  ***

merge into SALES_FINAL_TABLE F  -- Target table to merge changes from source table
using SALES_STREAM S -- Stream that has captured the changes
   on  f.id = s.id
when matched 
and S.METADATA$ACTION ='DELETE' 
and S.METADATA$ISUPDATE = 'FALSE'
then delete;


-- *** Process UPDATE,INSERT & DELETE simultaneously  ***

SELECT * FROM SALES_TABLES;  

INSERT INTO SALES_TABLES VALUES (2,'Lemon',0.99,1,1);

UPDATE SALES_TABLES
SET PRODUCT = 'Lemonade'
WHERE PRODUCT ='Lemon';
       
DELETE FROM SALES_TABLES
WHERE PRODUCT = 'Lemonade';  


SELECT * FROM SALES_TABLES;

SELECT * FROM SALES_STREAM;

SELECT * FROM SALES_FINAL_TABLE;

merge into SALES_FINAL_TABLE F      -- Target table to merge changes from source table
USING ( SELECT STRE.*,ST.location,ST.employees
        FROM SALES_STREAM STRE
        JOIN STORE_TABLE ST
        ON STRE.store_id = ST.store_id
       ) S
ON F.id=S.id
when matched                        -- DELETE condition
    and S.METADATA$ACTION ='DELETE' 
    and S.METADATA$ISUPDATE = 'FALSE'
    then delete                   
when matched                        -- UPDATE condition
    and S.METADATA$ACTION ='INSERT' 
    and S.METADATA$ISUPDATE  = 'TRUE'       
    then update 
    set f.product = s.product,
        f.price = s.price,
        f.amount= s.amount,
        f.store_id=s.store_id
when not matched                      -- INSERT NEW VALUES
    and S.METADATA$ACTION ='INSERT'
    then insert 
    (id,product,price,store_id,amount,employees,location)
    values
    (s.id, s.product,s.price,s.store_id,s.amount,s.employees,s.location);




