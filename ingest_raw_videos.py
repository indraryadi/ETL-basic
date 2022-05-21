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

def raw_videos(engine):
    try:    
        videoList=['CAvideos.csv','DEvideos.csv',
          'GBvideos.csv','INvideos.csv','USvideos.csv']
        df=pd.DataFrame(pd.read_csv('/home/indra/project/ETL-basic/data/{}'.format(videoList[0]),encoding="UTF-8"))
        
        df['country_code']=[videoList[0][0:2] for x in range(len(df))]

        for i in range(1,len(videoList)):
            data=pd.read_csv('/home/indra/project/ETL-basic/data/{}'.format(videoList[i]),encoding="UTF-8")
            data['country_code']=[videoList[i][0:2] for x in range(len(data))]
            loopDf=pd.DataFrame(data)
            df=pd.concat([df,loopDf],axis=0)
    
        print("Try insert raw videos data into MySQL...")
        df.to_sql(name='raw_videos_airflow',con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()

def insert_raw_data_to_mysql():
    
    cfg=read_credentials()['mysql_lake']
    mysql_auth=MySQL(cfg)
    engine,engine_conn=mysql_auth.conn()
    try:
        raw_videos(engine)
        print("Success !!!")
    except (Exception) as e:
        print(e)        
    
    engine.dispose()
    
if __name__=='__main__':
    insert_raw_data_to_mysql()