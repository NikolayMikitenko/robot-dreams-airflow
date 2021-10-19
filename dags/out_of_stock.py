from datetime import datetime
from airflow import DAG
import os
from common.http_to_filesystem_operator import HttpToFileSystemOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='out_of_stock_dag'
    , description='http dag for dowload data from out_of_stock api to file system'
    , start_date=datetime(2021, 10, 15)
    , schedule_interval='@daily'
)

def download_http_data(ds, context):
    o = HttpToFileSystemOperator(
        config_path=os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml'), 
        app_name='out_of_stock_app',
        date=ds,
        timeout=10,
        file_system_path=os.path.join('.', 'data'),
    )
    o.execute()

download_data = HttpToFileSystemOperator(
    task_id='get_data_from_http',
    dag=dag,
    provide_context=True,
    python_callable=download_http_data
)


#download_http_data = HttpToFileSystemOperator(
#    task_id='get_data_from_http',
#    dag=dag,
#    config_path=os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml'),
#    app_name='out_of_stock_app',
#    date="{{ ds }}",
#    timeout=10,
#    file_system_path=os.path.join('.', 'data'),
#    python_callable=empty
#    #xcom_push=True
#)