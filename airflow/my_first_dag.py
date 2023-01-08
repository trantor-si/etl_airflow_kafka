# #########################################################
# BLOCK 1: Importing modules and setting up the DAG
# #########################################################

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# #########################################################
# BLOCK 2: DAG ARGUMENTS
# #########################################################

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'luccostajr',
    'start_date': days_ago(0),
    'email': ['luccostajr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# #########################################################
# BLOCK 3: DAG DEFINITION
# #########################################################

dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# #########################################################
# BLOCK 4: TASK DEFINITION
# #########################################################

# define the first task - extract  that extracts fields from 
# /etc/passwd file. 
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag,
)

# define the second task - transform_and_load that transforms and 
# loads data into a file.
transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag,
)

# #########################################################
# BLOCK 5: TASK PIPELINE
# #########################################################

# task pipeline
extract >> transform_and_load