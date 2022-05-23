# ETL Batch Processing

>This project is demonstrate ETL Batch Processing using pandas as a data processing and airflow as an orchestrator.The purpose of this project is to create data pipeline from raw data [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) to data lake (MySQL), transform it into dim table and fact table and store it into data warehouse (postgreSQL). All the process will be scheduled to do everyday using `Airflow` as scheduler and monitoring process. For the dataset i use [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) from [Kaggle](https://www.kaggle.com)</a>.
___

### Table of Contents

- Prepare the Dataset
- Design ERD
- Extract Dataset to MySQL
- Transform Raw Data
- Load to PostgreSQL
- Create DAG
- Result

---

## 1. Prepare the Dataset
![Dataset](https://user-images.githubusercontent.com/103250258/169761777-d9c92d9c-ea11-4058-a156-f5b4f066fd9f.png)
![Category](https://user-images.githubusercontent.com/103250258/169767452-9cd01561-4472-4252-bec3-c1d8f261d34a.png)
![Video](https://user-images.githubusercontent.com/103250258/169763199-8faacd1a-77cd-4b30-a22e-36f6d3bcfc99.png)
>i download the dataset from kaggle.com and the dataset contain 10 files which is 5 video files in csv format and 5 category files in json format</p>

## 2. Design ERD
![ERD](https://user-images.githubusercontent.com/103250258/169776200-8f50a3b8-b1a1-438a-a391-79b9b8569708.jpg)
>There are 5 dim tables(video,channel,category,country, and trend time) and 1 fact table (trending video)</p>

## 3. Extract Dataset to MySQL
First i create 2 python files [`1.ingest_raw_category.py`](ingest_raw_category.py) and [`2.ingest_raw_video.py`](ingest_raw_videos.py). [`ingest_raw_category.py`](ingest_raw_category.py) has job to load the category data into MySQL and name it `raw_category_airflow`. [`ingest_raw_video.py`](ingest_raw_videos.py) has job to load the category data into MySQL and name it `raw_videos_airflow`.
 ```python
 def raw_videos(engine):
    try:    
        videoList=['CAvideos.csv','DEvideos.csv',
          'GBvideos.csv','INvideos.csv','USvideos.csv']
        df=pd.DataFrame(pd.read_csv('/home/indra/project/ETL-basic/data/{}'.format(videoList[0]),encoding="UTF-8"))
        
        ...
    
        print("Try insert raw videos data into MySQL...")
        df.to_sql(name='raw_videos_airflow',con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
        
    engine.dispose()
 ```
 >code snippet for the `ingest_raw_category.py`
## 4. Transform Raw Data
After the dataset ingest into data lake (MySQL) i transform the data base on ERD design, which has 5 dim table and 1 fact table, to do that i create module called [`Transform.py`](Transform.py) for transform the raw data.
```python
class Transform:
    ...

    #TRANSFORM TO DIM CATEGORY
    def dim_category(self):
        ...
        return dim_category
    
    #TRANSFORM TO DIM COUNTRY
    def dim_country(self):
        ...
        return dim_country
    
    #TRANSFORM TO DIM CHANNEL    
    def dim_channel(self):
        ...
        return temp

    #TRANSFORM TO DIM VIDEO
    def dim_video(self):
        ...
        return temp

    #TRANSFORM TO DIM TIME
    def dim_time(self):
        ...
        return new_df
    
    def fact_video(self):
        ...
        return df
    
    engine.dispose()
```

## 5 Load to PostgreSQL
Next after Transform the row data into dim and fact table, i create 2 files to load data into data warehouse (PostgreSQL) [`1.load_dim_to_dwh.py`](load_dim_to_dwh.py) and [`2.load_fact_to_dwh.py`](load_fact_to_dwh.py).
```python
def insert_fact_to_dwh(schema):
    ...
    try:
        print("Try insert fact table into DWH...")
        fact_video.to_sql('fact_video_ariflow',schema=schema,con=engine,if_exists="replace",index=False)
        print("Success !!!")
    except (Exception) as e:
        print(e)
```
>code snippet for load fact table to dwh (PostgreSQL).
## 6. Create DAG
Last for doing this task every day automaticaly, i use airflow as an orchestrator. To do that i need to create [`DAG`](dags/testingDAG.py) (Directed Acyclic Graph). in this file i scheduled the task to repeat daily using cron_preset on airflow that present as `@daily` and using airflow `BashOperator` for running the python using bash command. the DAG graph will be look like this:
![DAG Graph](https://user-images.githubusercontent.com/103250258/169818994-85cbd637-73c8-4ed7-9137-aea517ac6dc3.png)

## 7. Result
After the DAG running successfully the data lake (MySQL) and the data warehouse (PostgreSQL) will be look like this:
![data lake](https://user-images.githubusercontent.com/103250258/169820059-b17c9fba-26c7-4cae-bb1b-e04c23f6d75e.png)
>There are 2 tables `raw_category_airflow` and `raw_videos_airflow`.


![data warehouse](https://user-images.githubusercontent.com/103250258/169820414-bd24db1f-f323-4b16-8b94-39a8a335cf8e.png)
>There are 6 tables `dim_video`,`dim_category`,`dim_channel`,`dim_country`,`dim_time`,`fact_video`


