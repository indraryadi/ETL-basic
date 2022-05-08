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
    
def dim_channel():
    df=pd.DataFrame(df_video.drop_duplicates('channel_title'))
    filt=df['channel_title']
    temp=[]
    
    # fil=(df_video['channel_title']=='EminemVEVO')
    # b=df_video.loc[fil,['country_code']].drop_duplicates('country_code')
    
    for i in range(1,len(filt)):
        temp.append({"id":i,"channel_name":filt.iloc[i]})
    temp=pd.DataFrame(temp)
    
    return temp
if __name__=="__main__":
    # a=dim_category()
    # b=dim_country()
    c=dim_channel()
    
    # print(len(df_video['country_code'].str.contains("CA")))
    # df=pd.read_csv('data/CAvideos.csv')
    print(c.tail(5))