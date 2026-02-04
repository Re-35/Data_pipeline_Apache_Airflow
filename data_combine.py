import pandas as pd
from sqlalchemy import create_engine



def add_data(db_path, file_path):
    engine = create_engine(db_path)
    df = pd.read_csv(file_path, encoding='utf-8')
    
    df = df.rename(columns={'السنة': 'year', 'الربع' : 'quarter', 'نوع المزاد' : 'auction_type', 'عدد_المزادات' : 'number_auction', 'عدد_الأصول' : 'number_asset', 'المنطقة' : 'region', 'المدينة' : 'city', 'نوع_الأصل' : 'asset_type', 'نوع_الصك' : 'instrument_type', 'مجموع_قيمة_البيع' : 'total_sales'})

    df.to_sql(name='estate', con=engine, if_exists="append", index=False)
    print('Done add.')
