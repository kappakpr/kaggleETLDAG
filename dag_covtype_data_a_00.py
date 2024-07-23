from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['kappakpr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=20),
    'execution_timeout' : timedelta(minutes=25),
}

dag = DAG('covtype_data_a_00',
          default_args=default_args,
          description='Load kaggle dataset to BigQuery table',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/kaggledata_02.py',
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(
        location="us-central1",
        pipeline_name="covtype_pipeline",
        instance_name="pkinstance02",
        task_id="start_datafusion_pipeline",
        success_states=['COMPLETED'],
        pipeline_timeout=600,
    )

    run_script_task >> start_pipeline