# #########################################################
# BLOCK 1: Importing modules and setting up the DAG
# #########################################################

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

# #########################################################
# BLOCK 2: DAG ARGUMENTS
# #########################################################

default_args = {
    'owner': 'luccostajr',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 1, 2),
    'email': ['luccostajr@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

# #########################################################
# BLOCK 3: DAG DEFINITION
# #########################################################

dag = DAG(
    'simple_example_DAG',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=dt.timedelta(seconds=5),
)

# #########################################################
# BLOCK 4: TASK DEFINITION
# #########################################################

# t1 and t2 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_hello',
    bash_command='echo \'Greetings from Airflow! The date and time are\'',
    dag=dag,
)

t2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# #########################################################
# BLOCK 5: TASK PIPELINE
# #########################################################

t1 >> t2