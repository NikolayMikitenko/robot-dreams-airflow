from datetime import datetime
from airflow import DAG
import os
from common.http_to_hdfs_operator import HttpToHDFSOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id='out_of_stock_hdfs_dag'
    , description='http dag for dowload data from out_of_stock api to file system'
    , start_date=datetime(2021, 4, 1)
    , end_date=datetime(2021, 4, 15)
    , schedule_interval='@daily'
)

def download_http_data(ds, **kwargs):
    HttpToHDFSOperator(
        config_path=os.path.join(os.getcwd(), 'airflow', 'dags', 'config', 'config.yaml'),
        app_name='out_of_stock_app',
        date=ds,
        timeout=10,
        hdfs_conn_id='local_webhdfs' ,
        hdfs_path=os.path.join('/', 'bronze')
    ).execute()

download_data = PythonOperator(
    task_id='get_data_from_http_to_hdfs',
    dag=dag,
    provide_context=True,
    python_callable=download_http_data
)