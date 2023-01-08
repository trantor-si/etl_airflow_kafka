# #########################################################
# BLOCK 1: Importing modules and setting up the DAG
# #########################################################

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# #########################################################
# BLOCK 2: DAG ARGUMENTS
# #########################################################

default_args = {
    'owner': 'Luciano Costa',
    'start_date': days_ago(0),
    'email': ['luciano.costa@dummyemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# #########################################################
# BLOCK 3: DAG DEFINITION
# #########################################################

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL_toll_data',
    schedule_interval=timedelta(days=1),
)

# #########################################################
# BLOCK 4: TASK DEFINITION
# #########################################################

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='gzip -d /home/project/airflow/dags/finalassignment/tolldata.tgz && tar xvf /home/project/airflow/dags/finalassignment/tolldata.tar',
    dag=dag,
)

# * Rowid  - This uniquely identifies each row. This is consistent across all the three files.
# * Timestamp - What time did the vehicle pass through the toll gate.
# * Anonymized Vehicle number - Anonymized registration number of the vehicle 
# * Vehicle type - Type of the vehicle
# Number of axles - Number of axles of the vehicle
# Vehicle code - Category of the vehicle as per the toll plaza.

extract_data_from_csv = BashOperator (
    task_id='extract_data_from_csv',
    bash_command='cut -f1-4 -d"," vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# Rowid  - This uniquely identifies each row. This is consistent across all the three files.
# Timestamp - What time did the vehicle pass through the toll gate.
# Anonymized Vehicle number - Anonymized registration number of the vehicle 
# Vehicle type - Type of the vehicle
# * Number of axles - Number of axles of the vehicle
# * Tollplaza id - Id of the toll plaza
# * Tollplaza code - Tollplaza accounting code.

extract_data_from_tsv = BashOperator (
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 tollplaza-data.tsv | tr "\t" "," > tsv_data.csv',
    dag=dag,
)

# Rowid  - This uniquely identifies each row. This is consistent across all the three files.
# Timestamp - What time did the vehicle pass through the toll gate.
# Anonymized Vehicle number - Anonymized registration number of the vehicle 
# Tollplaza id - Id of the toll plaza
# Tollplaza code - Tollplaza accounting code.
# * Type of Payment code - Code to indicate the type of payment. Example : Prepaid, Cash.
# * Vehicle Code -  Category of the vehicle as per the toll plaza.

extract_data_from_fixed_width = BashOperator (
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59- payment-data.txt | tr " " "," > fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator (
    task_id='consolidate_data',
    bash_command='paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator (
    task_id='transform_data',
    bash_command='cut -f1-3 -d"," extracted_data.csv > part1.csv; ' +
        'cut -f4 -d"," extracted_data.csv | tr a-z A-Z > part2.csv; ' +
        'cut -f5- -d"," extracted_data.csv > part3.csv; ' +
        'paste -d"," part1.csv part2.csv part3.csv > '+
        '/home/project/airflow/dags/finalassignment/staging/transformed_data.csv; ' +
        'rm part1.csv part2.csv part3.csv',
    dag=dag,
)


# #########################################################
# BLOCK 5: TASK PIPELINE
# #########################################################

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
