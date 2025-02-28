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
@dag(dag_id='company_info_load',
     default_args=default_args,
     )
def import_daily_data():

    @task(multiple_outputs=True)
    def get_daily_api():
        API_KEY = os.getenv('API_KEY_AV')
        companies = ['AAPL','MSFT','GOOG','META','NVDA']

        data_arr = []
        for comp in companies:
            try:
                url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={comp}&apikey={API_KEY}'
                r = requests.get(url)
                data = r.json()
                data_arr.append(data)
            except requests.exceptions.RequestException as e:
                print(f"Request to {url} failed: {e}")
        data_dict = {'data': data_arr}
        return data_dict
    
    @task
    def write_to_bigquery(data_dict):
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            use_legacy_sql=False,
            location='US'
        )

        current_date = datetime.now().strftime('%Y-%m-%d')
        records = []
        data_arr=data_dict['data']
        records_str = ''
        for d in data_arr:
            records = []
            if d['AnalystRatingStrongBuy']=='-':
                d['AnalystRatingStrongBuy']=-1
            if d['AnalystRatingBuy']=='-':
                d['AnalystRatingBuy']=-1
            if d['AnalystRatingHold']=='-':
                d['AnalystRatingHold']=-1
            if d['AnalystRatingSell']=='-':
                d['AnalystRatingSell']=-1
            if d['AnalystRatingStrongSell']=='-':
                d['AnalystRatingStrongSell']=-1
            records.append(
                    f"('{d['Symbol']}', '{current_date}','{d['AssetType']}', '{d['Name']}', "
                    f"\"\"\"{d['Description']}\"\"\", '{d['Exchange']}', "
                    f"'{d['Currency']}', '{d['Country']}', "
                    f"'{d['Sector']}', '{d['Industry']}', "
                    f"{int(d['MarketCapitalization'])}, {float(d['DividendPerShare'])}, "
                    f"{float(d['AnalystTargetPrice'])}, {int(d['AnalystRatingStrongBuy'])}, "
                    f"{int(d['AnalystRatingBuy'])}, {int(d['AnalystRatingHold'])}, "
                    f"{int(d['AnalystRatingSell'])}, {int(d['AnalystRatingStrongSell'])})")
        
            records_str += ", ".join(records)
            records_str += ", "
        records_str = records_str[:-2]
        sql_query = f'''
            INSERT INTO `agile-athlete-449216-m2.stock_staging.company_info_data` (
                Symbol, date, AssetType, Name, Description, Exchange, Currency, Country, Sector, Industry,
                MarketCapitalization, DividendPerShare, AnalystTargetPrice, AnalystRatingStrongBuy,
                AnalystRatingBuy, AnalystRatingHold, AnalystRatingSell, AnalystRatingStrongSell
            )
            VALUES {records_str};
        '''
        logging.info(sql_query)
        bq_hook.run_query(sql=sql_query)
        logging.info(f"Data successfully written to BigQuery table")
    
    data = get_daily_api()
    write_to_bigquery(data)

import_daily_data()
