# ETL Batch Processing

>This project is demonstrate ETL Batch Processing using pandas as a data processing and airflow as an orchestrator.The purpose of this project is to create data pipeline from raw data [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) to data lake (MySQL), transform it into dim table and fact table and store it into data warehouse (postgreSQL). For the dataset i use [Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) from [Kaggle](https://www.kaggle.com)</a>.
___

### Table of Contents

- [Prepare the Dataset](prepare-the-dataset)
- [Design ERD](design-erd)
- [Prepare the Data Lake and Data Warehouse](#prepare-data-lake-and-data-warehouse)

## 1. Prepare the Dataset

![Dataset](https://user-images.githubusercontent.com/103250258/169761777-d9c92d9c-ea11-4058-a156-f5b4f066fd9f.png)
![Category](https://user-images.githubusercontent.com/103250258/169767452-9cd01561-4472-4252-bec3-c1d8f261d34a.png)
![Video](https://user-images.githubusercontent.com/103250258/169763199-8faacd1a-77cd-4b30-a22e-36f6d3bcfc99.png)
>The dataset contain 10 files which is 5 video files in csv format and 5 category files in json format</p>


<img alt="GitHub" src="" title="ERD" style="max-width: 100%;" width="500px" align="left">

<img alt="GitHub" src="" title="ERD" style="max-width: 100%;" width="500px" align="left">

<img alt="GitHub" src="https://user-images.githubusercontent.com/103250258/169758288-93051adc-41fc-480e-b8e9-a3343dbfe2ff.jpg" title="ERD" style="max-width: 100%;" width="500px" align="left">

