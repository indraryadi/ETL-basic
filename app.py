#!/home/indra/project/ETL-basic/venv/bin python3.10
import json
from unicodedata import category
import pandas as pd
import numpy as np
import sqlalchemy

from connections.mysql import MySQL
from connections.postgresql import PostgreSql

import Transform

def read_credentials():
    with open('credentials.json','r') as d:
        data=json.load(d)
    return data

def raw_category(engine):
    try:
        jsonList=['CA_category_id.json','DE_category_id.json','GB_category_id.json','IN_category_id.json','US_category_id.json']
        
        df=pd.DataFrame(pd.read_json('data/{}'.format(jsonList[0])))
        
        for i in range(1,len(jsonList)):
            data=pd.read_json('data/{}'.format(jsonList[i]))
            loopDf=pd.DataFrame(data)
            df=pd.concat([df,loopDf],axis=0)
        
        df['id']=np.arange(1,df.shape[0]+1)
        df=df[['id','kind','etag','items']]
        
        print("Try insert raw category data into MySQL...")
        df.to_sql(name='raw_category_airflow',con=engine,if_exists="replace",index=False,dtype={'id':sqlalchemy.types.JSON,'kind':sqlalchemy.types.JSON,'etag':sqlalchemy.types.JSON,'items':sqlalchemy.types.JSON})
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
        
def raw_videos(engine):
    try:    
        videoList=['CAvideos.csv','DEvideos.csv',
          'GBvideos.csv','INvideos.csv','USvideos.csv']
        df=pd.DataFrame(pd.read_csv('data/{}'.format(videoList[0]),encoding="UTF-8"))
        
        df['country_code']=[videoList[0][0:2] for x in range(len(df))]

        for i in range(1,len(videoList)):
            data=pd.read_csv('data/{}'.format(videoList[i]),encoding="UTF-8")
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
        raw_category(engine)
        print("Success !!!")
    except (Exception) as e:
        print(e)        
    
    engine.dispose()
#continue to Transform data

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
        # category.to_sql('dim_category',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim country into DWH...")
        # country.to_sql('dim_country',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim channel into DWH...")
        # channel.to_sql('dim_channel',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim video into DWH...")
        # video.to_sql('dim_video',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        
        print("Try insert dim time into DWH...")
        # time.to_sql('dim_time',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
        # print(country)
    except (Exception) as e:
        print(e)

def insert_fact_to_dwh(schema):
    cfg=read_credentials()['postgresql_warehouse']
    postgre_auth=PostgreSql(cfg)
    engine,engine_conn=postgre_auth.conn(conn_type='engine')
    
    transform=Transform.Transform()
    fact_video=transform.fact_video()
    
    try:
        print("Try insert fact table into DWH...")
        fact_video.to_sql('fact_video',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
    
    


if __name__=='__main__':
    insert_raw_data_to_mysql()
    # insert_dim_to_dwh(schema='public')
    # insert_fact_to_dwh(schema='public')  