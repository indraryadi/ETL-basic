# ETL Batch Processing

>This project is demonstrate ETL Batch Processing using pandas as a data processing and airflow as an orchestrator.The purpose of this project is to create data pipeline from raw data [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) to data lake (MySQL), transform it into dim table and fact table and store it into data warehouse (postgreSQL). For the dataset i use [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) from [Kaggle](https://www.kaggle.com)</a>.
___

### Table of Contents

- Prepare the Dataset
- Design ERD
- Extract Dataset to MySQL
- Transform Raw Data
- Load to PostgreSQL
- Create DAG

---

## 1. Prepare the Dataset

![Dataset](https://user-images.githubusercontent.com/103250258/169761777-d9c92d9c-ea11-4058-a156-f5b4f066fd9f.png)
![Category](https://user-images.githubusercontent.com/103250258/169767452-9cd01561-4472-4252-bec3-c1d8f261d34a.png)
![Video](https://user-images.githubusercontent.com/103250258/169763199-8faacd1a-77cd-4b30-a22e-36f6d3bcfc99.png)
>i download the dataset from kaggle.com and the dataset contain 10 files which is 5 video files in csv format and 5 category files in json format</p>

## 2. Design ERD
![ERD](https://user-images.githubusercontent.com/103250258/169776200-8f50a3b8-b1a1-438a-a391-79b9b8569708.jpg)
>There are 5 dim tables(video,channel,category,country, and trend time) and 1 fact table (trending video)</p>

## 3.Extract Dataset to MySQL

First i load the category and video dataset into MySQL and name it `raw_category_airflow` and `raw_video_airflow` 

```python
def raw_category(engine):
    try:
        jsonList=['CA_category_id.json','DE_category_id.json','GB_category_id.json',\
                  'IN_category_id.json','US_category_id.json']
        
        df=pd.DataFrame(pd.read_json('/home/indra/project/ETL-basic/data/{}'.format(jsonList[0])))
        
        for i in range(1,len(jsonList)):
            data=pd.read_json('/home/indra/project/ETL-basic/data/{}'.format(jsonList[i]))
            loopDf=pd.DataFrame(data)
            df=pd.concat([df,loopDf],axis=0)
        
        df['id']=np.arange(1,df.shape[0]+1)
        df=df[['id','kind','etag','items']]
        
        print("Try insert raw category data into MySQL...")
        df.to_sql(name='raw_category_airflow',con=engine,if_exists="replace",index=False,dtype=\
        {'id':sqlalchemy.types.JSON,'kind':sqlalchemy.types.JSON,'etag':sqlalchemy.types.JSON,'items':sqlalchemy.types.JSON})
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
```
## 4 Transform Raw Data
## 5 Load to PostgreSQL
## 6 Create DAG
