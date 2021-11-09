from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from common.postgres_to_hdfs_web import PostgresToWebHDFSOperator
import os
from airflow.operators.dummy_operator import DummyOperator
from common.spark_postgres_to_bronze import load_postgres_to_bronze
from common.spark_bronze_to_silver import load_postgres_bronze_to_silver
from common.spark_bronze_to_silver import load_bronze_dshop_orders_to_silver

dag = DAG(
    dag_id='postgres_data_pipeline'
    , start_date=datetime(2021, 11, 9)
    , schedule_interval='@daily'
)

tables = ['aisles', 'clients', 'departments', 'orders', 'products'] 
load_to_bronze_tasks = []

silver_tables = ['aisles', 'clients', 'departments', 'products'] 
special_table = 'orders'
load_to_silver_tasks = []

for table in tables:
    load_to_bronze_tasks.append(                
        PythonOperator(
            task_id=f'load_to_bronze_table_{table}',
            dag=dag,
            python_callable=load_postgres_to_bronze,
            provide_context=True,
            op_kwargs={'table':table, 'postgres_conn_id':'dshop'},
        )
    )

for table in tables:
    load_to_silver_tasks.append(                
        PythonOperator(
            task_id=f'load_to_silver_table_{table}',
            dag=dag,
            python_callable=load_postgres_bronze_to_silver,
            provide_context=True,
            op_kwargs={'table':table, 'project':'dshop'},
        )
    )

load_to_silver_orders_task = PythonOperator(
    task_id=f'load_to_silver_table_orders',
    dag=dag,
    python_callable=load_bronze_dshop_orders_to_silver,
    provide_context=True,
)



start_task = DummyOperator(task_id='start', dag=dag)
end_bronze_task = DummyOperator(task_id='end_bronze_task', dag=dag)


start_task >> load_to_bronze_tasks >> end_bronze_task
end_bronze_task >> load_to_silver_tasks
end_bronze_task >> load_to_silver_orders_task