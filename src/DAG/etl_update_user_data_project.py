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
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sql import SQLCheckOperator, SQLThresholdCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

os.chdir('/lessons/')
pg_conn = BaseHook.get_connection('pg_conn')

args = {
   'owner': 'airflow',  # Информация о владельце DAG
   'retries': 2,  # Количество повторений в случае неудач
   'retry_delay': timedelta(minutes=1),  # Пауза между повторами
   'email':['airflow@example.com'],
   'email_on_failure':False,
   'email_on_retry':False
}

business_dt = '{{ yesterday_ds}}'
headers={
	 'X-Nickname': 'abaelardus',
	 'X-Cohort': '1',
	 'X-Project': 'True',
	 'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
    }
url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'

dag = DAG(
    dag_id='etl_update_user_data_project',  # Имя DAG
    description='@daily',
    schedule_interval="0 3 * * *",  # Периодичность запуска, например, "00 15 * * *"
    default_args=args,  # Базовые аргументы
    catchup=True,
    start_date=datetime(2022, 6, 24),
    end_date=datetime(2022, 10, 10),
    params={'business_dt': business_dt}
    )

#функции--------------------------------------------------------------------------------------------------


#запрос на получение пути к файлам инкремента
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
def get_report_request(url, ti, headers):
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


#get increment_id    
def get_increment_request(url, business_dt, ti, headers): 
    url = url   
    task_id_1 = str(ti.xcom_pull(task_ids='create_files_request.task_id_request', key='task_id_1'))
    report_id = str(ti.xcom_pull(task_ids='create_files_request.get_report_request', key='report_id'))
    payload = {'report_id': report_id, 'date': business_dt+'T00:00:00'}
    for i in range(10):
        increment_request = requests.get(url + '/get_increment?task_id=' + task_id_1, params=payload, headers=headers).json()
        print(increment_request)
        if increment_request['status'] == 'SUCCESS' or increment_request['status'] == 'NOT FOUND':
            break
        else:
            time.sleep(10)
    increment_id = increment_request['data']['increment_id']
    if not increment_request['data']['increment_id']:
        s3_path = 's3_path_not_available'
    else:
        s3_path = increment_request['data']['s3_path']
    ti.xcom_push(key='s3_path', value=s3_path)


#выбор пути при наличии/отсутствии файла инкремента за выбранный день
def decide_which_path(ti, options):
    s3_path = ti.xcom_pull(task_ids='create_files_request.get_increment_request', key='s3_path')
    if s3_path == 's3_path_not_available':
        return options[0]
    else:
        return options[1]


#выгрузка файлов из хранилища
def get_files_inc(business_dt, ti):
    # опишите тело функции
    s3_path = ti.xcom_pull(task_ids='create_files_request.get_increment_request', key='s3_path')
    if s3_path == 's3_path_not_available':
        pass
    else:
        for filename in ['customer_research_inc','user_orders_log_inc','user_activity_log_inc']:
            local_filename = '/lessons/' + business_dt.replace('-','') + '_' + filename + '.csv' # add date to filename
            url_file = s3_path[filename]
            print(url_file)
            df = pd.read_csv(url_file, index_col=0)
            df = df.drop_duplicates()
            if 'id' in df.columns:
                df = df.drop('id', axis=1)
            if filename == 'user_orders_log_inc' and 'status' not in df.columns:
                df['status'] = 'shipped'
            if 'date_id' in df.columns:
                df.rename({'date_id':'date_time'}, axis=1)
            df.to_csv(local_filename)


#подключение к БД
def pg_execute_query(query):
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def check_success_insert_user_order_log(context):
    #val = "('user_order_log', 'check_rows_order_log' ,current_timestamp, 0)"
    query = "INSERT INTO staging.dq_checks_results values ('user_order_log', 'check_rows_order_log' ,current_timestamp, 0) "
    execute_query = pg_execute_query(query)


def check_failure_insert_user_order_log (context):
    #val = "('user_order_log', 'check_rows_order_log' ,current_timestamp, 0)"
    query = "INSERT INTO staging.dq_checks_results values ('user_order_log', 'check_rows_order_log' ,current_timestamp, 1) "
    execute_query = pg_execute_query(query)


def check_success_insert_user_activity_log (context):
    #val = "('user_order_log', 'check_rows_order_log' ,current_timestamp, 0)"
    query = "INSERT INTO staging.dq_checks_results values ('user_activity_log', 'check_rows_order_log' ,current_timestamp, 0) "
    execute_query = pg_execute_query(query)


def check_failure_insert_user_activity_log (context):
    #val = "('user_order_log', 'check_rows_order_log' ,current_timestamp, 0)"
    query = "INSERT INTO staging.dq_checks_results values ('user_activity_log', 'check_rows_order_log' ,current_timestamp, 1) "
    execute_query = pg_execute_query(query)


#загрузка отчета в БД
def load_file_to_pg(filename,pg_table,conn_args):
    filename = filename.replace('-', '')
    f = pd.read_csv(filename, index_col=0)
    
    cols = ','.join(list(f.columns))
    insert_stmt = f"INSERT INTO {pg_table} ({cols}) VALUES %s"
    
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    conn.commit()
    cur.close()
    conn.close() 


with TaskGroup("create_files_request", dag=dag) as create_files_request:
    task_id_request = PythonOperator(
        task_id='task_id_request',
        python_callable=task_id_request,
        op_kwargs={'url':url, 'headers':headers},
        dag=dag)
    get_report_request = PythonOperator(
        task_id='get_report_request',
        python_callable=get_report_request,
        op_kwargs={'url':url, 'headers':headers},
        dag=dag)
    get_increment_request = PythonOperator(
        task_id='get_increment_request',
        python_callable=get_increment_request,
        op_kwargs={'url':url, 'business_dt': business_dt, 'headers':headers},
        dag=dag)
    task_id_request >> get_report_request >> get_increment_request

delay_bash_task: BashOperator = BashOperator(
    task_id='delay',
    bash_command="sleep 10s",
    dag=dag)

get_files_task = PythonOperator(
        task_id = 'get_files_task',
        python_callable = get_files_inc,
        op_kwargs = {'business_dt': business_dt,'s3_conn':'s3_conn'},
        dag = dag
        )
       

dummy1 = DummyOperator(
        task_id='dummy1',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=dag
        )

branch_task1 = BranchPythonOperator(
    task_id='branch_task1',
    python_callable=decide_which_path,
    op_kwargs={
        'options':['dummy1', 'get_files_inc_task']},
    dag=dag)

get_files_inc_task = PythonOperator(
        task_id = 'get_files_inc_task',
        python_callable = get_files_inc,
        op_kwargs = {'business_dt': business_dt},
        dag = dag
)        


with TaskGroup("delete_group", dag=dag) as delete_group:
    sql="DELETE FROM {{ params.table }} WHERE date_time = '" + business_dt + "'"
    delete_customer_research = PostgresOperator(
        task_id="delete_customer_research",
        postgres_conn_id= "pg_conn",
        sql=sql,
        params={'table': 'staging.customer_research'},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=dag)
    delete_user_orders_log = PostgresOperator(
        task_id="delete_user_orders_log",
        postgres_conn_id= "pg_conn",
        sql=sql,
        params={'table': 'staging.user_orders_log'},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=dag)
    delete_user_activity_log = PostgresOperator(
        task_id="delete_user_activity_log",
        postgres_conn_id= "pg_conn",
        sql=sql,
        params={'table': 'staging.user_activity_log'},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=dag)
    [delete_customer_research, delete_user_orders_log, delete_user_activity_log]                                                                   


with TaskGroup('load_inc_group', dag=dag) as load_inc_group:
    l = []
    dummy_inc = DummyOperator(
        task_id='dummy_inc',
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
    for filename in ['customer_research', 'user_orders_log', 'user_activity_log']:
        l.append(PythonOperator(
            task_id='load_' + filename + '_inc',
            python_callable=load_file_to_pg,
            op_kwargs={'filename': business_dt + '_' + filename + '_inc.csv',
            'pg_table': 'staging.'+ filename,
            'conn_args': pg_conn},
            dag=dag))
    dummy_inc >> l
                             

with TaskGroup("rows_check_task", dag=dag) as rows_check_task:
    sql_check  = SQLThresholdCheckOperator(
            task_id="check_rows_order_log",
            sql="Select count (*) from staging.user_orders_log",
            min_threshold=5,
            max_threshold=1000000,
            conn_id = "pg_conn",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            on_success_callback = check_success_insert_user_order_log, on_failure_callback = check_failure_insert_user_order_log ,
            dag=dag
            )
    
    sql_check2  = SQLThresholdCheckOperator(
            task_id="check_rows_activity_log",
            sql="Select count (*) from staging.user_activity_log",
            min_threshold=5,
            max_threshold=1000000,
            conn_id = "pg_conn",
            on_success_callback = check_success_insert_user_activity_log, on_failure_callback =  check_failure_insert_user_activity_log ,
            dag=dag
            )
    sql_check >> sql_check2

with TaskGroup("update_mart", dag=dag) as update_mart:
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
    update_dimensions >> update_facts >> update_retention

end = DummyOperator(task_id="end",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS) 

create_files_request >> delay_bash_task >> get_files_task >> branch_task1 
branch_task1 >> [dummy1, get_files_inc_task] 
get_files_inc_task >> delete_group >> load_inc_group >> rows_check_task >> update_mart 
[dummy1, update_mart] >> end
