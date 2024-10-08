from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import timedelta, datetime

# Parameters
PROJECT_ID = 'keen-quest-434917-s4'
DATASET_ID = 'stockmarket_dataset'
TABLE_ID = 'WIG20_table'

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=500)
}

dag = DAG('market_data_run',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          start_date=datetime(2024, 9, 19),
          catchup=False
          )

# SQL task 1: Find unique companies
find_companies_query = f"""
    SELECT DISTINCT ticker_name
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`;
"""

find_companies = BigQueryExecuteQueryOperator(
    task_id='find_companies',
    sql=find_companies_query,
    use_legacy_sql=False,
    dag=dag,
)

# Python script for table creation per company
def create_company_tables():
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT ticker_name
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    query_job = client.query(query)
    tickers = [row['ticker_name'] for row in query_job]

    for ticker in tickers:
        create_table_query = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{ticker}_table` AS
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE ticker_name = '{ticker}';
        """
        client.query(create_table_query)

# SQL task 2: Creating table for each company
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_company_tables,
    dag=dag,
)

# Run external Python script
run_script_task = BashOperator(
    task_id='extract_data',
    bash_command='python /home/airflow/gcs/dags/scripts/data_scraper.py',
    dag=dag,
)

# Run pipeline Data Fusion
start_pipeline = CloudDataFusionStartPipelineOperator(
    location="europe-north1",
    pipeline_name="wig20_pipeline",
    instance_name="datafusion-dev",
    task_id="start_wig20_pipeline",
    pipeline_timeout=500,
    dag=dag
)

# Task order
run_script_task >> start_pipeline >> find_companies >> create_tables_task