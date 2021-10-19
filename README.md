# Project for robot dreams airflow ETL

##COPY ALL FILES FROM FOLDER dags to airflow/dags and add PYTHONPATH to common folder

## Folders contents
* dags - folder with dags and their configs and python modules
* dags/common - folder with dags modules
* dags/config - folder with dag configs

## Files contents
* export_data_from_postgres_to_filesystem.py - dag for load data from DB using SELECT statement
* export_datadump_from_postgres_to_filesystem.py - dag for load data from DB using batch_load or COPY method
* out_of_stock.py - dag for load data from http_api
* print_path.py - dag for debug PYTHONPATH

* common/config.py - class for work with application config
* common/http_to_filesystem_operator - class for call http
* common/postgres_dump_to_filesystem_operator.py - class inherit PythonOperator for load data dump from DB
* common/postgres_to_filesystem_operator.py - class inherit PythonOperator for load data from DB using SELECT statement

* config/config.yaml - file with http aplication config

##Task logic
1. Load data from DB - realizied with query list of DB objects and create dynamic dag
2. Load data from http - realized with using start date parameter
