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


def insert_fact_to_dwh(schema):
    cfg=read_credentials()['postgresql_warehouse']
    postgre_auth=PostgreSql(cfg)
    engine,engine_conn=postgre_auth.conn(conn_type='engine')
    
    transform=Transform.Transform()
    fact_video=transform.fact_video()
    
    try:
        print("Try insert fact table into DWH...")
        fact_video.to_sql('fact_video_ariflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
if __name__=='__main__':
    insert_fact_to_dwh(schema='public')