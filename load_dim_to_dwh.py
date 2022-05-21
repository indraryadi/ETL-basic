#!/home/indra/project/ETL-basic/venv/bin python3.10
import json
from unicodedata import category
import pandas as pd
import numpy as np
import sqlalchemy

from dags.connections.mysql import MySQL
from dags.connections.postgresql import PostgreSql

import Transform

def read_credentials():
    with open('/home/indra/project/ETL-basic/dags/credentials.json','r') as d:
        data=json.load(d)
    return data

def insert_dim_to_dwh(schema):
    cfg=read_credentials()['postgresql_warehouse']
    postgre_auth=PostgreSql(cfg)
    engine,engine_conn=postgre_auth.conn(conn_type='engine')
    
    transform=Transform.Transform()
    category=transform.dim_category()
    country=transform.dim_country()
    channel=transform.dim_channel()
    video=transform.dim_video()
    time=transform.dim_time()
        
    try:
        print("Try insert dim category into DWH...")
        category.to_sql('dim_category_airflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim country into DWH...")
        country.to_sql('dim_country_airflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim channel into DWH...")
        channel.to_sql('dim_channel_airflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim video into DWH...")
        video.to_sql('dim_video_airflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim time into DWH...")
        time.to_sql('dim_time_airflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        # print(country)
    except (Exception) as e:
        print(e)
    
if __name__=='__main__':
    insert_dim_to_dwh(schema='public')