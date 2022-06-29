import json
import os
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from psycopg2.extensions import AsIs, register_adapter

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

os.chdir('/lessons/')
pg_conn = BaseHook.get_connection('pg_conn')

args = {
   'owner': 'airflow',  # Информация о владельце DAG
   'retries': 1,  # Количество повторений в случае неудач
   'retry_delay': timedelta(minutes=1),  # Пауза между повторами
   'email':['airflow@example.com'],
   'email_on_failure':False,
   'email_on_retry':False
}
business_dt = '{{ ds }}'
headers={
	 'X-Nickname': 'abaelardus',
	 'X-Cohort': '1',
	 'X-Project': 'True',
	 'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
    }
url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'

dag = DAG(
    dag_id='etl_tables_migration',  # Имя DAG
    description='staging and mart schemes',
    catchup=False,
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2022, 10, 10),
    params={'business_dt': business_dt}
    )

#функции--------------------------------------------------------------------------------------------------


#запрос на получение пути к файлам основного отчета
def task_id_request(ti, url, headers):
    url = url
    #headers=headers
    method_url = '/generate_report'
    #get task_id
    r = requests.post(url + method_url, headers=headers)
    response_dict = json.loads(r.content)
    task_id_1 = response_dict['task_id']
    ti.xcom_push(key='task_id_1', value=task_id_1)


#get report_id
def get_report_request(url, business_dt, ti, headers):
    url = url
    task_id_1 = str(ti.xcom_pull(task_ids='create_files_request.task_id_request', key='task_id_1'))
    print('TASK_ID: ', task_id_1)
    print(url + '/get_report?task_id=' + task_id_1)
    for i in range(10):
        r_report_id = requests.get(url + '/get_report?task_id=' + task_id_1, headers=headers).json()
        print(r_report_id)
        if r_report_id['status'] == 'SUCCESS':
            break
        else:
            time.sleep(10)
    report_id = r_report_id['data']['report_id']
    ti.xcom_push(key='report_id', value=report_id)


# получение основного отчета
def get_files_from_s3(business_dt,s3_conn, ti):
    report_id = str(ti.xcom_pull(task_ids='create_files_request.get_report_request', key='report_id'))
    url = f'https://storage.yandexcloud.net/s3-sprint3/cohort_1/abaelardus/project/{report_id}/'
    for filename in ['customer_research','user_orders_log','user_activity_log']:
        local_filename = '/lessons/' + business_dt.replace('-','') + '_' + filename + '.csv' # add date to filename
        url_file = url+filename+'.csv'
        print(url_file)
        df = pd.read_csv(url_file, index_col=0)
        df = df.drop_duplicates()
        if filename == 'user_orders_log':
            df['status'] = 'shipped'
        if 'date_id' in df.columns:
            df.rename({'date_id':'date_time'}, axis=1)
        df.to_csv(local_filename)


#загрузка основного отчета в БД
def load_file_to_pg(filename,pg_table,conn_args):
    ''' csv-files to pandas dataframe '''
    filename = filename.replace('-', '')
    f = pd.read_csv(filename, index_col=0)
    
    ''' load data to postgres '''
    cols = ','.join(list(f.columns))
    insert_stmt = f"INSERT INTO {pg_table} ({cols}) VALUES %s"
    
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()
    cur.close()
    conn.close() 


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


with TaskGroup("create_files_request", dag=dag) as create_files_request:
    task_id_request = PythonOperator(
        task_id='task_id_request',
        python_callable=task_id_request,
        op_kwargs={'url':url, 'headers':headers},
        dag=dag)
    get_report_request = PythonOperator(
        task_id='get_report_request',
        python_callable=get_report_request,
        op_kwargs={'url':url, 'business_dt': business_dt, 'headers':headers},
        dag=dag)
    task_id_request >> get_report_request

delay_bash_task: BashOperator = BashOperator(
    task_id='delay',
    bash_command="sleep 10s",
    dag=dag)

get_files_task = PythonOperator(
        task_id = 'get_files_task',
        python_callable = get_files_from_s3,
        op_kwargs = {'business_dt': business_dt,'s3_conn':'s3_conn'},
        dag = dag
        )

with TaskGroup("load_csv_group", dag=dag) as load_csv_group:
    l = []
    for filename in ['customer_research', 'user_orders_log', 'user_activity_log']:
        l.append(PythonOperator(
            task_id='load_' + filename,
            python_callable=load_file_to_pg,
            op_kwargs={'filename': business_dt + '_' + filename + '.csv',
            'pg_table': 'staging.'+ filename,
            'conn_args': pg_conn},
            dag=dag))
    l        

update_dimensions = PostgresOperator(
        task_id="update_dimensions",
        postgres_conn_id = "pg_conn",
        sql = "sql/dim_upd_sql_query.sql",
        dag=dag
    )
                                 
update_facts = PostgresOperator(
        task_id="update_facts",
        postgres_conn_id = "pg_conn",
        sql = "sql/facts_upd_sql_query.sql",
        dag=dag
    )

update_retention = PostgresOperator(
        task_id="update_retention",
        postgres_conn_id = "pg_conn",
        sql = "sql/retention_upd_sql_query.sql",
        dag=dag
    )

end = DummyOperator(task_id="end")     

begin >> create_staging_tables >> create_mart_tables >> create_checks >> update_status >> create_files_request 
create_files_request >> delay_bash_task >> get_files_task >> load_csv_group
load_csv_group >> update_dimensions >> update_facts >> update_retention >> end
