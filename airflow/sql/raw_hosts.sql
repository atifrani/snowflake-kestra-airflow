USE WAREHOUSE COMPUTE_WH;
USE DATABASE airbnb;
USE SCHEMA BRONZE;
                 

CREATE OR REPLACE TABLE raw_hosts_afw
                    (id integer,
                     name string,
                     is_superhost string,
                     created_at datetime,
                     updated_at datetime);
                    
COPY INTO raw_hosts_afw (id, name, is_superhost, created_at, updated_at)
                    from 's3://logbrain-datasets/airbnb/hosts.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');