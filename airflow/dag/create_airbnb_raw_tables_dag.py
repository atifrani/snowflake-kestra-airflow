from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



default_args = {
    'owner': 'axel',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='create_airbnb_raw_tables_dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 11, 9, 11),
    schedule_interval='@daily',
    template_searchpath="/opt/airflow/",

) as dag:


    bronze_table_hosts = SnowflakeOperator(
        task_id="create_raw_table_hosts",
        snowflake_conn_id='snowflake_dev',
        sql="sql/raw_hosts.sql",
        params={},
    )

    silver_table_hosts = SnowflakeOperator(
        task_id="create_raw_table_hosts",
        snowflake_conn_id='snowflake_dev',
        sql="sql/src_hosts.sql",
        params={},
    )    


    
    bronze_table_hosts >>  silver_table_hosts 