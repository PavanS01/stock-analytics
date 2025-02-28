from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import os

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
        API_KEY = os.getenv('API_KEY_AV')
        companies = ['AAPL','MSFT','GOOG','META','NVDA']

        data_dict = {}
        for comp in companies:
            try:
                url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={comp}&time_from=20240101T0000&limit=3&apikey={API_KEY}'
                r = requests.get(url)
                data = r.json()
                data_dict[comp]=data
            except requests.exceptions.RequestException as e:
                print(f"Request to {url} failed: {e}")
        ret_dict = {'data': data_dict}
        return ret_dict
    
    @task
    def write_to_bigquery(data_dict):
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            use_legacy_sql=False,
            location='US'
        )
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        records_str = ''
        data_coll=data_dict['data']
        for sym in data_coll:
            c=0
            logging.info('sym', sym)
            logging.info(data_coll[sym])
            for d in data_coll[sym]['feed']:
                records = []
                if c==3:
                    break
                input_str = d['time_published']

                # Parse the input string into a datetime object
                dt = datetime.strptime(input_str, "%Y%m%dT%H%M%S")

                #Convert to the required format (yyyy-mm-dd)
                output_str = dt.strftime("%Y-%m-%d")      
                records.append(
                    f"('{sym}', '{current_date}', \"\"\"{d['title']}\"\"\", '{d['url']}', "
                    f"'{output_str}', "
                    f"\"\"\"{d['summary']}\"\"\", '{d['source']}', "
                    f"'{d['overall_sentiment_label']}')")
                c+=1
            
                records_str += ", ".join(records)
                records_str += ", "
        records_str = records_str[:-2]

        sql_query = f'''
            INSERT INTO `agile-athlete-449216-m2.stock_staging.news_data` (
                Symbol, date,  title, url, time_published, summary, source, overall_sentiment_label
            )
            VALUES {records_str};
        '''
        logging.info(sql_query)
        bq_hook.run_query(sql=sql_query)
        logging.info(f"Data successfully written to BigQuery table")
    
    data = get_daily_api()
    write_to_bigquery(data)

import_daily_data()
