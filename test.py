import pandas as pd
import numpy as np
import json

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
        temp.append({"category_id":df[i]['id'],"category":df[i]['snippet']['title']})
    temp=pd.DataFrame(temp)
    dim_category=temp.drop_duplicates('category_id')
    dim_category['category_id']=dim_category['category_id'].astype('int')
    return dim_category

#TRANSFORM TO DIM COUNTRY
def dim_country():
    df=df_video.drop_duplicates('country_code')
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
        temp.append({"country_code":filt[i],"country_name":country_name})
    dim_country=pd.DataFrame(temp)
    return dim_country
  
#TRANSFORM TO DIM CHANNEL    
def dim_channel():
    df=df_video.drop_duplicates('channel_title')
    filt=df['channel_title']
    temp=[]
    
    # fil=(df_video['channel_title']=='EminemVEVO')
    # b=df_video.loc[fil,['channel_title']].drop_duplicates('channel_title')
    
    for i in range(0,len(filt)):
        temp.append({"id_channel":i+1,"channel_title":filt.iloc[i]})
    temp=pd.DataFrame(temp)
    
    return temp

#TRANSFORM TO DIM VIDEO
def dim_video():
    df=df_video.drop_duplicates('video_id')

    temp=df[['video_id','title','tags']]
    # for i in range(len(filt)):
    #     temp.append(filt.iloc[i])
    
    # filt=(df['title']=='Eminem - Untouchable (Audio)')
    # temp=df.loc[filt]
    
    return temp

def dim_time():
    
    column=['trending_date','day','month','year']
    
    df=df_video.drop_duplicates('trending_date')
    filt=df['trending_date']
    new_df=pd.DataFrame(columns=column)
    # temp=filt.str.replace('.','-',regex=False)
    
    
    # new_df['date']=pd.DataFrame(["20"+x[0:2]+"-"+x[6:]+"-"+x[3:5] for x in temp])
    # new_df['day']=pd.DataFrame([x[3:5] for x in temp])
    # new_df['month']=pd.DataFrame([x[6:] for x in temp])
    # new_df['year']=pd.DataFrame(["20"+x[0:2] for x in temp])
    
    
    # temp=filt.str.replace('.','.',regex=False)
    temp=filt
    new_df['trending_date']=pd.DataFrame([x for x in temp])
    new_df['day']=pd.DataFrame([x[3:5] for x in temp])
    new_df['month']=pd.DataFrame([x[6:] for x in temp])
    new_df['year']=pd.DataFrame(["20"+x[0:2] for x in temp])
    
    return new_df


def fact_video():
    data=df_video
    
    column=['video_id','id_channel','id_category','country_code','trending_date']
    df=pd.DataFrame(columns=column)
    category=0
    
    df_v=dim_video()
    df_channel=dim_channel()
    df_country=dim_country()
    df_category=dim_category()
    df_time=dim_time()
    temp=data.merge(df_v,on='video_id')\
             .merge(df_channel,on='channel_title')\
             .merge(df_country,on='country_code')\
             .merge(df_category,on='category_id')\
            .merge(df_time,on='trending_date')
    # temp2=data['channel_title'].isin(df_channel['channel_title'])
    # df['id_channel']=
    df['video_id']=temp['video_id']
    df['id_channel']=temp['id_channel']
    df['country_code']=temp['country_code']
    df['id_category']=temp['category_id']
    df['trending_date']=temp['trending_date']
    return df
    

if __name__=="__main__":
    # a=dim_category()
    # b=dim_country()
    # c=dim_channel()
    # d=dim_video()
    # e=dim_time()
    f=fact_video()
    
    print(len(f))
    print(len(df_video))
    print(f.tail(5))
    # print(e)
    # filt=(f['video_id']=='jY7XC5iY3ck') #n1WpP7iowLc
    # print(f.loc[filt,['date']].value_counts())