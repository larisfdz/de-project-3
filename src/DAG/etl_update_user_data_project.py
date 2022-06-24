import pandas as pd
from datetime import datetime, timedelta, date
import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
import re
import json
import boto3
import psycopg2, psycopg2.extras
import psycopg2, psycopg2.extras
import urllib3
import pandas as pd
import os
from airflow.hooks.postgres_hook import PostgresHook
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLThresholdCheckOperator,
)

#{"aws_access_key_id":"YCAJEWXOyY8Bmyk2eJL-hlt2K", "aws_secret_access_key": "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"}
os.chdir('/lessons/')
pg_conn = BaseHook.get_connection('pg_conn')

args = {
   'owner': 'airflow',  # Информация о владельце DAG
   #'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
   'retries': 1,  # Количество повторений в случае неудач
   'retry_delay': timedelta(minutes=1),  # Пауза между повторами
   'email':['airflow@example.com'],
   'email_on_failure':False,
   'email_on_retry':False
}
business_dt = '{{ ds }}'
#дата, за которую хотим получить инкремент
date = '2022-06-18T00:00:00'

dag = DAG(
    dag_id='etl_update_user_data_project',  # Имя DAG
    description='daily updates',
    schedule_interval="0 12 * * *",  # Периодичность запуска, например, "00 15 * * *"
    default_args=args,  # Базовые аргументы
    catchup=False,
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2022, 10, 10),
    params={'business_dt': business_dt}
    )
#функции


#запрос на получение пути к файлам основного отчета и инкремента
def create_files_request(business_dt, ti):
    url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
    headers={
	 'X-Nickname': 'abaelardus',
	 'X-Cohort': '1',
	 'X-Project': 'True',
	 'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
    }
    method_url = '/generate_report'
    #get task_id
    r = requests.post(url + method_url, headers=headers, data={'date': business_dt})
    response_dict = json.loads(r.content)
    task_id_1 = response_dict['task_id']
    #get report_id
    for i in range(10):
        r_report_id = requests.get(url + '/get_report?task_id=' + task_id_1, headers=headers).json()
        print(r_report_id)
        if r_report_id['status'] == 'SUCCESS':
            break
        else:
            time.sleep(10)
    report_id = r_report_id['data']['report_id']
    #get increment_id
    payload = {'report_id': report_id, 'date': business_dt+'T00:00:00'}
    for i in range(10):
        r_increment_id = requests.get(url + '/get_increment?task_id=' + task_id_1, params=payload, headers=headers).json()
        print(r_increment_id)
        if r_increment_id['status'] == 'SUCCESS' or r_increment_id['status'] == 'NOT FOUND':
            break
        else:
            time.sleep(10)
    increment_id = r_increment_id['data']['increment_id']
    if not r_increment_id['data']['increment_id']:
        s3_path = 's3_path_not_available'
    else:
        s3_path = r_increment_id['data']['s3_path']
 
    #ti.xcom_push(key='task_id_1', value=task_id_1)
    ti.xcom_push(key='report_id', value=report_id)
    ti.xcom_push(key='increment_id', value=increment_id)
    ti.xcom_push(key='s3_path', value=s3_path)

# получение основного отчета
def get_files_from_s3(business_dt,s3_conn, ti):
    report_id = str(ti.xcom_pull(task_ids='create_files_request', key='report_id'))
    url = f'https://storage.yandexcloud.net/s3-sprint3/cohort_1/abaelardus/project/{report_id}/'
    for filename in ['customer_research','user_orders_log','user_activity_log']:
        local_filename = '/lessons/' + business_dt.replace('-','') + '_' + filename + '.csv' # add date to filename
        url_file = url+filename+'.csv'
        print(url_file)
        df = pd.read_csv(url_file, index_col=0)
        df = df.drop_duplicates()
        if filename == 'user_orders_log':
            df['status'] = 'shipped'
        df.to_csv(local_filename)

#выбор пути при наличии/отсутствии файла инкремента за текущий день
def decide_which_path(ti, options):
    s3_path = ti.xcom_pull(task_ids='create_files_request', key='s3_path')
    if s3_path == 's3_path_not_available':
        return options[0]
        #'dummy'
    else:
        return options[1]

#выгрузка файлов из хранилища
def get_files_inc(business_dt, ti):
    # опишите тело функции
    s3_path = ti.xcom_pull(task_ids='create_files_request', key='s3_path')
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
                    df.to_csv(local_filename)

#удаление записей предыдущей основной загрузки в БД
def truncate_file_pg(pg_table,conn_args):
    ''' truncate data in postgres '''
    #cols = ','.join(list(f.columns))
    trunc_stmt = f"TRUNCATE {pg_table} RESTART IDENTITY"
    
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute(trunc_stmt)
    conn.commit()
    cur.close()
    conn.close()

#загружаем резульататы проверки наличия строк в таблицах
def check_success_insert_user_activity_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'check_rows_activity_log' , current_date, 0)
          """
    )
 
def check_failure_insert_user_activity_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'check_rows_order_log' , current_date, 1)
          """
    )

def check_success_insert_user_order_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        postgres_conn_id=pg_conn,
        sql="""
            INSERT INTO staging.dq_checks_results
            values ('user_order_log', 'check_rows_order_log' ,current_date, 0)
          """
    )

def check_failure_insert_user_order_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        postgres_conn_id=pg_conn,
        sql="""
            INSERT INTO staging.dq_checks_results
            values ('user_order_log', 'user_order_log_isNull' ,current_date, 1)
          """
    )

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

#обновление таблиц mart
def pg_execute_query(query,conn_args):
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

dim_upd_sql_query = '''
-- UPDATE
with new_data as (
    select item_id, item_name, category_name
    from (
        select item_id, item_name,
        CASE WHEN UPPER(item_name) SIMILAR TO '(%ТОМАТЫ%|%ХЛЕБ%|%МОЛОКО%|%ЯБЛОКО%|%САХАР%)' THEN 'Продукты'
            ELSE 'Другие товары' END AS category_name,
            row_number() over (partition by item_id order by date_time desc) rn
        from staging.user_orders_log
        )t
    where rn = 1
)
update mart.d_item
set item_name = new_data.item_name,
    category_name = new_data.category_name
from new_data
where mart.d_item.item_id = new_data.item_id
    and mart.d_item.item_name != new_data.item_name;

-- INSERT -- не удаляйте эту строку
insert into mart.d_item (item_id, item_name, category_name)
select 
distinct item_id, last_value(item_name) over(partition by item_id order by date_time desc) as name,
 CASE WHEN UPPER(last_value(item_name) over(partition by item_id order by date_time desc)) SIMILAR TO '(%ТОМАТЫ%|%ХЛЕБ%|%МОЛОКО%|%ЯБЛОКО%|%САХАР%)' THEN 'Продукты'
            ELSE 'Другие товары' END AS category_name
from staging.user_orders_log
where item_id not in (select distinct item_id from mart.d_item)
--order by 1, 2
;

insert into mart.d_customer (customer_id, first_name, last_name)
SELECT 
DISTINCT customer_id, first_name, last_name 
FROM staging.user_orders_log
where customer_id not in (select distinct customer_id from mart.d_customer)
--order by customer_id
;

INSERT INTO mart.d_calendar (date_id, day_num, month_num, month_name, year_num)
SELECT DISTINCT date_time,
EXTRACT (DAY FROM date_time),
EXTRACT (MONTH FROM date_time),
TO_CHAR(date_time, 'month'),
EXTRACT (YEAR FROM date_time)
FROM staging.user_orders_log st
WHERE st.date_time NOT IN (select distinct date_id from mart.d_calendar)
;

insert into mart.d_city (city_id, city_name)
SELECT DISTINCT city_id, city_name
FROM staging.user_orders_log
where city_id not in (select distinct city_id from mart.d_city)
--order by customer_id
;
'''

facts_upd_sql_query = '''
-- DELETE
delete from mart.f_daily_sales
where date_id in (select distinct date_time::date from staging.user_orders_log);
-- INSERT -- не удаляйте эту строку
insert into mart.f_daily_sales
(date_id,
item_id,
customer_id,
price,
quantity,
payment_amount, 
status)
select distinct date_time::date as date_id, 
item_id, 
customer_id, 
CASE WHEN status = 'refunded' AND payment_amount > 0 
THEN payment_amount/quantity * -1 
ELSE payment_amount/quantity END AS price,
quantity, 
CASE WHEN status = 'refunded' AND payment_amount > 0 THEN payment_amount*-1
    ELSE payment_amount END AS amount,
status
from staging.user_orders_log
'''

create_files_request = PythonOperator(
    task_id='create_files_request',
    python_callable=create_files_request,
    op_kwargs={'business_dt': business_dt},
    dag=dag)

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
    #trigger_rule="all_done",
    dag=dag)

get_files_inc_task = PythonOperator(
        task_id = 'get_files_inc_task',
        python_callable = get_files_inc,
        op_kwargs = {'business_dt': business_dt},
        dag = dag
)        

with TaskGroup("trunc_group", dag=dag) as trunc_group:
    truncate_customer_research = PythonOperator(task_id='truncate_customer_research',
                                        python_callable=truncate_file_pg,
                                        op_kwargs={'pg_table': 'staging.customer_research',
                                                    'conn_args': pg_conn},
                                        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                                        dag=dag)
    truncate_user_orders_log = PythonOperator(task_id='truncate_user_orders_log',
                                        python_callable=truncate_file_pg,
                                        op_kwargs={'pg_table': 'staging.user_orders_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)
    truncate_user_activity_log = PythonOperator(task_id='truncate_user_activity_log',
                                        python_callable=truncate_file_pg,
                                        op_kwargs={'pg_table': 'staging.user_activity_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)
    truncate_customer_research >> truncate_user_orders_log >> truncate_user_activity_log                                                                    

with TaskGroup("load_csv_group", dag=dag) as load_csv_group:
    load_customer_research = PythonOperator(task_id='load_customer_research',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_customer_research.csv',
                                                    'pg_table': 'staging.customer_research',
                                                    'conn_args': pg_conn},
                                        dag=dag)

    load_user_orders_log = PythonOperator(task_id='load_user_orders_log',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_user_orders_log.csv',
                                                    'pg_table': 'staging.user_orders_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)

    load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_user_activity_log.csv',
                                                    'pg_table': 'staging.user_activity_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)
    load_customer_research >> load_user_orders_log >> load_user_activity_log

dummy2 = DummyOperator(
        task_id='dummy2',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=dag
        )

with TaskGroup("load_inc_group", dag=dag) as load_inc_group:
    load_customer_research_inc = PythonOperator(task_id='load_customer_research_inc',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_customer_research_inc.csv',
                                                    'pg_table': 'staging.customer_research',
                                                    'conn_args': pg_conn},
                                        dag=dag)

    load_user_orders_log_inc = PythonOperator(task_id='load_user_orders_log_inc',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_user_orders_log_inc.csv',
                                                    'pg_table': 'staging.user_orders_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)

    load_user_activity_log_inc = PythonOperator(task_id='load_user_activity_log_inc',
                                        python_callable=load_file_to_pg,
                                        op_kwargs={'filename': business_dt + '_user_activity_log_inc.csv',
                                                    'pg_table': 'staging.user_activity_log',
                                                    'conn_args': pg_conn},
                                        dag=dag)
    load_customer_research_inc >> load_user_orders_log_inc >> load_user_activity_log_inc                               

branch_task2 = BranchPythonOperator(
    task_id='branch_task2',
    python_callable=decide_which_path,
    op_kwargs={
        'options':['dummy2', 'load_inc_group']},
    #trigger_rule="all_done",
    dag=dag)


with TaskGroup("rows_check_task", dag=dag) as rows_check_task:
    sql_check  = SQLCheckOperator(
            task_id="check_rows_order_log",
            sql="Select count (*) from staging.user_orders_log",
            conn_id = "pg_conn",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            on_success_callback = check_success_insert_user_order_log, on_failure_callback =  check_failure_insert_user_order_log ,
            dag=dag
            )
    
    sql_check2  = SQLThresholdCheckOperator(
            task_id="check_rows_activity_log",
            sql="Select count (*) from staging.user_activity_log",
            min_threshold=5,
            max_threshold=1000000,
            conn_id = "pg_conn",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            on_success_callback = check_success_insert_user_activity_log, on_failure_callback =  check_failure_insert_user_activity_log ,
            dag=dag
            )
    sql_check >> sql_check2


update_dimensions = PythonOperator(task_id='update_dimensions',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': dim_upd_sql_query,
                                    'conn_args': pg_conn},
                                    dag=dag)
                                    
update_facts = PythonOperator(task_id='update_facts',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': facts_upd_sql_query,
                                    'conn_args': pg_conn},
                                    dag=dag)

create_files_request >> delay_bash_task >> get_files_task >> branch_task1 
branch_task1 >> [dummy1, get_files_inc_task] >> trunc_group 
trunc_group >> load_csv_group #load_customer_research >> load_user_orders_log >> load_user_activity_log
load_csv_group >> branch_task2
branch_task2 >> [dummy2, load_inc_group]
[dummy2, load_inc_group] >> rows_check_task >> update_dimensions >> update_facts
