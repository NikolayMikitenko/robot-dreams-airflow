from datetime import datetime
from airflow import DAG
import os
from common.http_to_filesystem_operator import HttpToFileSystemOperator

dag = DAG(
    dag_id='out_of_stock_dag'
    , description='http dag for dowload data from out_of_stock api to file system'
    , start_date=datetime(2021, 10, 15)
    , schedule_interval='@daily'
)

download_http_data = HttpToFileSystemOperator(
    task_id='get_data_from_http',
    dag=dag,
    config_path=os.path('/', 'home', 'user', 'airflow', 'dags', 'config', 'config.yaml'),
    app_name='out_of_stock_app',
    date="{{ ds }}",
    timeout=10,
    file_system_path=os.path.join('.', 'data'),
    #xcom_push=True
)