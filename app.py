import json
import pandas as pd
import numpy as np
import sqlalchemy

from connections.mysql import MySQL

def read_credentials():
    with open('credentials.json','r') as d:
        data=json.load(d)
    return data

def raw_category(engine):
    try:
        jsonList=['CA_category_id.json','DE_category_id.json','FR_category_id.json','GB_category_id.json','IN_category_id.json','US_category_id.json']
        
        df=pd.DataFrame(pd.read_json('data/{}'.format(jsonList[0])))
        
        for i in range(1,len(jsonList)):
            data=pd.read_json('data/{}'.format(jsonList[i]))
            loopDf=pd.DataFrame(data)
            df=pd.concat([df,loopDf],axis=0)
        
        df['id']=np.arange(1,df.shape[0]+1)
        df=df[['id','kind','etag','items']]
        
        print("Try insert raw category data into MySQL...")
        df.to_sql(name='raw_category',con=engine,if_exists="replace",index=False,dtype={'id':sqlalchemy.types.JSON,'kind':sqlalchemy.types.JSON,'etag':sqlalchemy.types.JSON,'items':sqlalchemy.types.JSON})
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
        
def raw_videos(engine):
    try:    
        videoList=['CAvideos.csv','DEvideos.csv','FRvideos.csv',
          'GBvideos.csv','INvideos.csv','USvideos.csv']
        df=pd.DataFrame(pd.read_csv('data/{}'.format(videoList[0]),encoding="UTF-8"))
        
        df['country_code']=[videoList[0][0:2] for x in range(len(df))]

        for i in range(1,len(videoList)):
            data=pd.read_csv('data/{}'.format(videoList[i]),encoding="UTF-8")
            data['country_code']=[videoList[i][0:2] for x in range(len(data))]
            loopDf=pd.DataFrame(data)
            df=pd.concat([df,loopDf],axis=0)
    
        print("Try insert raw videos data into MySQL...")
        df.to_sql(name='raw_videos',con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
    
def insert_raw_data_to_mysql():
    
    cfg=read_credentials()['mysql_lake']
    mysql_auth=MySQL(cfg)
    engine,engine_conn=mysql_auth.conn()
    
    raw_videos(engine)
    # raw_category(engine)        
    
    
#continue to Transform data
        
        
if __name__=='__main__':
    insert_raw_data_to_mysql()