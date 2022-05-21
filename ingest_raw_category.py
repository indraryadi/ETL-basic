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

def raw_category(engine):
    try:
        jsonList=['CA_category_id.json','DE_category_id.json','GB_category_id.json','IN_category_id.json','US_category_id.json']
        
        df=pd.DataFrame(pd.read_json('/home/indra/project/ETL-basic/data/{}'.format(jsonList[0])))
        
        for i in range(1,len(jsonList)):
            data=pd.read_json('/home/indra/project/ETL-basic/data/{}'.format(jsonList[i]))
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

def insert_raw_data_to_mysql():
    
    cfg=read_credentials()['mysql_lake']
    mysql_auth=MySQL(cfg)
    engine,engine_conn=mysql_auth.conn()
    try:
        raw_category(engine)
        print("Success !!!")
    except (Exception) as e:
        print(e)        
    
    engine.dispose()
    
if __name__=='__main__':
    insert_raw_data_to_mysql()