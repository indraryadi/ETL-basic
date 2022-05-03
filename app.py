import json
import pandas as pd

from connections.mysql import MySQL

def read_credentials():
    with open('credentials.json','r') as d:
        data=json.load(d)
    return data

def insert_raw_data_to_mysql():
    cfg=read_credentials()['mysql_lake']
    mysql_auth=MySQL(cfg)
    engine,engine_conn=mysql_auth.conn()
    
    try:
        df=pd.read_csv('./data/survey_results_public.csv')
        df.columns=[i.lower() for i in df.columns.to_list()]
        print("Try insert data into MySQL...")
        df.to_sql(name='raw_survey_result_public',con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
    
    
    
        
        
        
if __name__=='__main__':
    insert_raw_data_to_mysql()