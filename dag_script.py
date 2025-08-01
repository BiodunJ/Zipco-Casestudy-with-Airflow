from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from Extraction import run_extraction
from Transformation import run_transformation
from loading import run_loading

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 23),
    'email': 'jobak401@outlook.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'zipco_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Zipco Foods',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the tasks in the DAG
extraction_task = PythonOperator(
    task_id='run_extraction',
    python_callable=run_extraction,
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='run_transformation',
    python_callable=run_transformation,
    dag=dag,
)

loading_task = PythonOperator(
    task_id='run_loading',
    python_callable=run_loading,
    dag=dag,
)

extraction_task >> transformation_task >> loading_task  