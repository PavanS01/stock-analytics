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
@dag(dag_id='import_daily_data_v1',
     default_args=default_args,
     )
def import_daily_data():

    @task(multiple_outputs=True)
    def get_daily_api():
        API_KEY = os.getenv('API_KEY_FM')
        companies = ['AAPL','MSFT','GOOG','META','NVDA']
        companies_str = ','.join(companies)

        url = f'https://financialmodelingprep.com/api/v3/quote/{companies_str}?apikey={API_KEY}'
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
        data=data_dict['data']
        for d in data:
            date_obj = datetime.fromtimestamp(int(d['timestamp']))
            date_str = date_obj.strftime('%Y-%m-%d')
            records.append(
                f"('{d['symbol']}', '{d['name']}', {float(d['price'])}, "
                f"{float(d['changesPercentage'])}, {float(d['change'])}, "
                f"{float(d['dayLow'])}, {float(d['dayHigh'])}, "
                f"{float(d['yearHigh'])}, {float(d['yearLow'])}, "
                f"{float(d['marketCap'])}, {float(d['priceAvg50'])}, "
                f"{float(d['priceAvg200'])}, '{d['exchange']}', "
                f"{int(d['volume'])}, {int(d['avgVolume'])}, "
                f"{float(d['open'])}, {float(d['previousClose'])}, "
                f"{float(d['eps'])}, {float(d['pe'])}, "
                f"{int(d['sharesOutstanding'])}, '{date_str}')")
        
        records_str = ", ".join(records)

        sql_query = f'''
            INSERT INTO `agile-athlete-449216-m2.stock_staging.daily_data` (
                symbol, name, price, changesPercentage, change, dayLow, dayHigh, yearHigh, yearLow, marketCap,
                priceAvg50, priceAvg200, exchange, volume, avgVolume, open, previousClose, eps, pe,
                sharesOutstanding, timestamp)
            VALUES {records_str};
        '''

        bq_hook.run_query(sql=sql_query)
        logging.info(f"Data successfully written to BigQuery table")
    
    data = get_daily_api()
    write_to_bigquery(data)

import_daily_data()
