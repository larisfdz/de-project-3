from datetime import datetime, timedelta, date
import os

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

#{"aws_access_key_id":"YCAJEWXOyY8Bmyk2eJL-hlt2K", "aws_secret_access_key": "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"}
os.chdir('/lessons/')
pg_conn = BaseHook.get_connection('pg_conn')

dag = DAG(
    dag_id='etl_tables_migration',  # Имя DAG
    description='staging and mart schemes',
    catchup=False,
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2022, 10, 10),
    )

begin = DummyOperator(task_id="begin")

create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id= "pg_conn",
        sql='sql/stage0_staging_create.sql',
        dag=dag)

create_mart_tables = PostgresOperator(
        task_id="create_mart_tables",
        postgres_conn_id= "pg_conn",
        sql='sql/stage0_mart_create.sql',
        dag=dag)

create_checks = PostgresOperator(
        task_id="create_checks",
        postgres_conn_id= "pg_conn",
        sql='sql/stage1_1_staging_create_check.sql',
        dag=dag)        

update_status = PostgresOperator(
        task_id="update_status",
        postgres_conn_id= "pg_conn",
        sql='sql/stage1_staging_mart_alter.sql',
        dag=dag)    

end = DummyOperator(task_id="end")     

begin >> create_staging_tables >> create_mart_tables >> create_checks >> update_status >> end
