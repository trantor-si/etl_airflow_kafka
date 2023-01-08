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
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# #########################################################
# BLOCK 3: DAG DEFINITION
# #########################################################

dag = DAG(
    'ETL_Log_Processing',
    default_args=default_args,
    description='ETL_Log_Processing',
    schedule_interval=timedelta(days=1),
)

# #########################################################
# BLOCK 4: TASK DEFINITION
# #########################################################

# define the first task - download task must download the server 
# access log file which is available at the URL. 
download = BashOperator(
    task_id='download',
    bash_command='sudo wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag=dag,
)

move = BashOperator(
    task_id='move',
    bash_command='sudo mv web-server-access-log.txt /home/project/',
    dag=dag,
)

# define the second task - extract that extracts fields from
# the downloaded file and saves the extracted data into a file.
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d "#" -f 1,4 /home/project/web-server-access-log.txt > /home/project/extracted.txt',
    dag=dag,
)

# define the third task - transform task must capitalize the visitorid.
transform = BashOperator(
    task_id='transform',
    bash_command='tr [:lower:] [:upper:] < /home/project/extracted.txt > /home/project/capitalized.txt',
    dag=dag,
)

# define the fourth task - load task must compress the extracted and 
# transformed data.
load = BashOperator(
    task_id='load',
    bash_command='zip /home/project/log.zip /home/project/capitalized.txt' ,
    dag=dag,
)

# #########################################################
# BLOCK 5: TASK PIPELINE
# #########################################################

# task pipeline
download >> move >> extract >> transform >> load