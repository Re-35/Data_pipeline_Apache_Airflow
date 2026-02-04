from airflow.sdk import dag, task
from airflow.sdk.bases.decorator import Task
from datetime import timedelta
from datetime import datetime
import pandas as pd
import re
from sqlalchemy import create_engine



defult_args = {'owner' : 'trainer about data pipeline', 'start_date': datetime(2026, 1, 31), 'email_on_failure' : False, 'email_on_retry' : False, 'retries' : 2, 'retry_delay' : timedelta(minutes=5)}

@dag(dag_id="estate_dag", description="dag process for data pipline of estate databases", default_args= defult_args, catchup=False, schedule='@daily') 
def dag_estate():
    
    @task
    def extract():
        eng1 = create_engine('sqlite:////opt/airflow/desktop_data/Electronic_Sell_Estate.db')
        eng2 = create_engine('sqlite:////opt/airflow/desktop_data/InPerson_Sell_Estate.db')

        df1 = pd.read_sql(con=eng1, sql='SELECT * FROM estate')
        df2 = pd.read_sql(con=eng2, sql='SELECT * FROM estate')

        df =  pd.concat([df1, df2])
        return df
    
    @task
    def transform(df):
        df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']] = df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']].fillna('لا يوجد')
        df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']] = df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']].map(lambda x : x.strip())
        df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']] = df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']].map(lambda x : re.sub('[أآإ]', 'ا', x))
        df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']] = df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']].map(lambda x : re.sub(r'ه\b', 'ة', x))
        df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']] = df[['auction_type', 'region', 'city', 'asset_type', 'instrument_type']].map(lambda x : re.sub(r'[ًٌَُّـٍِ~ْ]', '', x))
        

        df['year'] = df['year'].apply(lambda x : 2025 if x != 2025 else 2025)
        # Not sure the record related to which quarter:
        df['quarter'] = df['quarter'].fillna(0)
        df['number_auction'] = df['number_auction'].fillna(df['number_auction'].median())
        df['number_asset'] = df['number_asset'].fillna(df['number_asset'].median())
        df['total_sales'] = df['total_sales'].fillna(df['total_sales'].median())

        return df
    
    @task
    def load(df):
        eng = create_engine('sqlite:////opt/airflow/desktop_data/SoldEstates.db')

        df.to_sql('estate', con=eng, if_exists='replace', index=False)
        print('Process Done.')


    
    
    process1 = extract()
    process2 = transform(process1)
    load(process2)

dag_estate()