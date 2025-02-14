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
@dag(dag_id='historical_load',
     default_args=default_args,
     )
def import_daily_data():

    @task(multiple_outputs=True)
    def get_daily_api():
        API_KEY = os.getenv('API_KEY_FM')
        companies = ['AAPL','MSFT','GOOG','META','NVDA']
        companies_str = ','.join(companies)

        #url = f'https://financialmodelingprep.com/api/v3/quote/{companies_str}?apikey={API_KEY}'
        start_date = '2025-01-02'
        end_date = '2025-01-03'
        url = f'https://financialmodelingprep.com/api/v3/historical-price-full/{companies_str}?from={start_date}&to={end_date}&apikey={API_KEY}'
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
        
        records_str = ''
        hist_list=data_dict['data']["historicalStockList"]
        for comp in hist_list:
            symbol = comp['symbol']
            records = []
            for day in comp['historical']:  
                records.append(
                    f"('{symbol}', '{day['date']}', {float(day['open'])}, "
                    f"{float(day['high'])}, {float(day['low'])}, "
                    f"{float(day['close'])}, {float(day['adjClose'])}, "
                    f"{int(day['volume'])}, {int(day['unadjustedVolume'])}, "
                    f"{float(day['change'])}, {float(day['changePercent'])}, "
                    f"{float(day['vwap'])}, '{day['label']}', "
                    f"{float(day['changeOverTime'])})")
        
            records_str += ", ".join(records)
            records_str += ', '
            
        records_str = records_str[:-2]
        sql_query = f'''
            INSERT INTO `agile-athlete-449216-m2.stock_staging.historical_data` (
            symbol, date, open, high, low, close, adjClose, volume, unadjustedVolume, 
            change, changePercent, vwap, label, changeOverTime)
            VALUES {records_str};
        '''
        logging.info(sql_query)
        bq_hook.run_query(sql=sql_query)
        logging.info("Data successfully written to BigQuery table")
    
    data = get_daily_api()
    write_to_bigquery(data)

import_daily_data()
