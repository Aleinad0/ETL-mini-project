# Dag object
from airflow import DAG

# Operators
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# datetime
from datetime import timedelta, datetime

# Python libraries
import requests
import os
import json

# Import your functions from the 'our_functions' module
from our_functions import read_api, harmonize, clean,stage, fetch_weather_data, save_to_file, list_files

selected_station = 97200
directory_path = "/home/aleinad0/airflow/saved_json_files" #?

# Define default_args
default_args = {
    'owner': 'Daniela',
    'start_date': datetime(2022, 10, 8),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
with DAG("our_project", default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    # Creating first task
    read_api_task = PythonOperator(
        task_id='read_api',
        python_callable=read_api,
        op_args=[selected_station]  # Pass the selected_station as an argument to the function
    )
    
    #create list files
    list_files_task=PythonOperator(
        task_id= 'list_files',
        python_callable=list_files)
    
    # Creating harmonize task

    harmonize_task = PythonOperator(
        task_id='harmonize',
        python_callable=harmonize,
        provide_context= True
    )

    # Creating clean task
    clean_dataframe_task = PythonOperator(
        task_id='clean_data_frame',
        python_callable=clean,
        provide_context= True
    )

    # Create save to db task
    stage_task = PythonOperator(
        task_id='stage',
        python_callable=stage
    )

    # Creating fetch data task
    fetch_weather_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_weather_data,
        op_args=[selected_station, range(0, 39)]  # Pass station and parameters as arguments
    )

    # Create a DummyOperator to serve as a starting point
    start_task = EmptyOperator(task_id='start')

    # Create a DummyOperator to serve as an ending point
    end_task = EmptyOperator(task_id='end')

    # Set the order of execution of tasks.
    start_task >> read_api_task >> list_files_task >> fetch_weather_data_task >>harmonize_task >> clean_dataframe_task >> stage_task >> end_task
