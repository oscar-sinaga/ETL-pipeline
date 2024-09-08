from src.helper.db_connector import postgres_engine_dwh
import pandas as pd
from pangres import upsert


dwh_engine = postgres_engine_dwh()

def load_sales_data(df_sales_clean,dw_table_sales = "sales"):
# insert data to data warehouse
    # df_sales_clean.to_sql(name = dw_table_sales,
    #                         con = dwh_engine,
    #                         if_exists = "append",
    #                         index = False)
    
    # Upsert
    values = {
        "name": "Testing Product",
        "main_category": "Testing Category",
        "sub_category": "Testing Sub Category",
        "image": "https://sekolahdata-assets.s3",
        "link": "https://pacmann.io/",
        "ratings": 5,
        "no_of_ratings": 30,
        "discount_price": 450,
        "actual_price": 1000
    }
    # Create a DataFrame from the values
    df_upsert = pd.DataFrame([values])

    # Add df_upsert to df_sales_clean
    df_sales_clean = pd.concat([df_sales_clean,df_upsert]).reset_index(drop='index')

    df_sales_clean.insert(0,'id',range(0,0+len(df_sales_clean)))

    df_sales_clean = df_sales_clean.set_index('id')

    # Perform the upsert operation
    upsert(con=dwh_engine, df=df_sales_clean, table_name=dw_table_sales, if_row_exists='update')

def load_marketing_data(df_marketing_clean,dw_table_marketing = "marketing"):
# insert data to data warehouse
    df_marketing_clean.to_sql(name = dw_table_marketing,
                            con = dwh_engine,
                            if_exists = "append",
                            index = False)
    
def load_scraping_data(df_scraping_clean,dw_table_scraping = "scraping"):
# insert data to data warehouse
    df_scraping_clean.to_sql(name = dw_table_scraping,
                            con = dwh_engine,
                            if_exists = "append",
                            index = False)