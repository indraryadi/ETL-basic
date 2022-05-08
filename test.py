import pandas as pd
import numpy as np
import json
from pytz import country_names

import sqlalchemy
from connections.mysql import MySQL

with open('credentials.json','r') as c:
    credentials=json.load(c)

mysql_auth=MySQL(credentials['mysql_lake'])
engine,con_engine=mysql_auth.conn()


df_category=pd.read_sql(sql='raw_category',con=engine)
df_video=pd.read_sql(sql='raw_videos',con=engine)

#TRANSFORM TO DIM CATEGORY
def dim_category():
    df=df_category['items']
    temp=[]
    for i in range(len(df)):        
        temp.append({"id":df[i]['id'],"category":df[i]['snippet']['title']})
    temp=pd.DataFrame(temp)
    dim_category=temp.drop_duplicates('id')
    return dim_category


#TRANSFORM TO COUNTRY
def dim_country():
    df=pd.DataFrame(df_video.drop_duplicates('country_code'))
    filt=[x for x in df['country_code']]
    temp=[]
    # print(country_code_filt)
    for i in range(len(filt)):
        country_name=""
        match filt[i]:
            case 'CA':
                country_name="Canada"
            case 'DE':
                country_name="Germany"
            case 'FR':
                country_name="France"
            case 'GB':
                country_name="Great Britain"
            case 'IN':
                country_name="India"
            case 'US':
                country_name="USA" 
        temp.append({"id":filt[i],"country_name":country_name})
    dim_country=pd.DataFrame(temp)
    return dim_country
    
if __name__=="__main__":
    a=dim_category()
    b=dim_country()
    # print(b)
    # a.to_csv("d.csv",index=False)
    # print(len(a))