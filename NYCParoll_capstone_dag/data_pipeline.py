from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from NYCParoll_capstone_dag.data_loader import extract_data, transform_data, load_data

defaults_args = {
    'owner': 'airflow',
    'email': 'nonsoskyokpara@gmail.com',
    'email_on_failure': True,
    'depends_on_past': False,
    'retries': 3,
    'retry delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=5)
}

with DAG(
dag_id='nycpayroll_pipeline',
default_args=defaults_args,
start_date=datetime(2024, 2, 1),
schedule_interval= '@daily',
catchup=False,
description='ETL pipeline for NYC Payroll Data'
) as dag:
    extract_task = PythonOperator(
        task_id = 'Data_Extraction',
        python_callable = extract_data
    )

    transform_task = PythonOperator(
        task_id = 'Data_Transformation',
        python_callable = transform_data
    )

    load_task = PythonOperator(
        task_id = 'Data_Loading',
        python_callable = load_data
    )

    extract_task >> transform_task >> load_task