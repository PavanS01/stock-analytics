from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='bigquery_read_and_print_data',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=False,  # Disable catchup
) as dag:

    @task
    def read_and_print_bigquery_data():
        """
        Task to read data from BigQuery and print it.
        """
        # Initialize BigQueryHook
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False, location='US')

        # Define the SQL query
        sql_query = '''
        SELECT * FROM `agile-athlete-449216-m2.stock_staging.daily_data` LIMIT 10
        '''

        # Execute the query and fetch results
        results = bq_hook.get_records(sql=sql_query)

        # Print the results
        logging.info("Fetched data from BigQuery:")
        for row in results:
            logging.info(row)

    # Call the task
    read_and_print_bigquery_data()