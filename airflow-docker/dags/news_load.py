from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

default_args = {
    'owner': 'dez_project',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

# start_date=datetime(2025, 2, 8)
# schedule_interval='@daily'
@dag(dag_id='news_load',
     default_args=default_args,
     )
def import_daily_data():

    @task(multiple_outputs=True)
    def get_daily_api():
        API_KEY = 'T570D8WT5XC2F4UN'
        companies = ['AAPL','MSFT','GOOG','META','NVDA']
        companies_str = ','.join(companies)

        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=NVDA&time_from=20240101T0000&limit=3&apikey={API_KEY}'
        r = requests.get(url)
        data = r.json()
        data_dict = {'data': data}
        return data_dict
    
    @task
    def write_to_bigquery(data_dict):
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            use_legacy_sql=False,
            location='US'
        )
        
        records = []
        sym='NVDA'
        data=data_dict['data']['feed']
        records_str = ''
        c=0

        for d in data:
            if c==3:
                break
            input_str = d['time_published']

            # Parse the input string into a datetime object
            dt = datetime.strptime(input_str, "%Y%m%dT%H%M%S")

            #Convert to the required format (yyyy-mm-dd)
            output_str = dt.strftime("%Y-%m-%d")      
            records.append(
                f"('{sym}', \"\"\"{d['title']}\"\"\", '{d['url']}', "
                f"'{output_str}', "
                f"\"\"\"{d['summary']}\"\"\", '{d['source']}', "
                f"'{d['overall_sentiment_label']}')")
            c+=1
        
        records_str += ", ".join(records)

        sql_query = f'''
            INSERT INTO `agile-athlete-449216-m2.stock_staging.news_data` (
                Symbol, title, url, time_published, summary, source, overall_sentiment_label
            )
            VALUES {records_str};
        '''
        logging.info(sql_query)
        bq_hook.run_query(sql=sql_query)
        logging.info(f"Data successfully written to BigQuery table")
    
    data = get_daily_api()
    write_to_bigquery(data)

import_daily_data()
