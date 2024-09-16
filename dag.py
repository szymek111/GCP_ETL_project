"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('market_data_run',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/data_scraper.py',
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="europe-north1",
    pipeline_name="ETL_stockmarket_first_pipeline",
    instance_name="datafusion-dev",
    task_id="start_ETL_stockmarket_first_pipeline",
)
    
    run_script_task >> start_pipeline