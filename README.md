# Youtube project
**📊  Analysis youtube search algorithm**
## Architectural solution
![image](images/solution.png)
## Plan:
- [Idea](#idea)
- [Goals](#goals)
- [Stack](#stack-used)
## Realization:
- [Preparation](#preparation)
- [Extract Data](#extract-from-youtube)
- [Transform-Load](#transform-and-load)
- [Analysis](#analysis)
- [Conclusion](#conclusion)
## Plan
### Idea
Find the relationship between search rank and video attributes and analyze search ranking day by day. It's useful for content creation.
I will analyze children's content because it is one of the most popular and viewed segments on YouTube.
### Goals
- Extract data from YouTube
- Clean and optimizy Data set for analysis
- Load Data set to wharehouse
- Load Data set to DB (staging --> business)
- Analysis Data set
### Stack used
- Yandex Cloud platform: 
```Virtual Private Cloud, Compute Cloud, Object Storage(S3)``` 
- Airflow
- Postgres DB
- Tableau
- Docker
- Google API
- Python, SQL
---
## Realization
### Preparation
- Deploy Virtual Machine (CPU2, RAM 8Gb, HDD 50Gb) in Compute Cloud with [Docker containers](/docker) for Airflow and Postgres (not metadata store).
- Create S3 bucket in Object Storage.
- Create connections
- Check the data to be used
- Create shemas and tables (I've defined [DAG](dag_create_tables_youtube.py) to create 2 schema with 3 tables (one is staging and two are business))
### Extract from YouTube
I've desided to use the [Google API](https://developers.google.com/youtube/v3/docs/search/list) in PythonOperator to extract information from search results with request: "мультики для малышей". There are two methods which I've used search.list() and videos.list(). With these methods, I was able to extract the following information:
```csv
date_extract, video_id, title, description, view_count, like_count,comment_count, time_published, channel_id, channel_title
```
The Operator in the [DAG](dag_youtube_s3_pd.py)
![image](images/dag.png)
### Transform and Load

### Analysis
### Conclusion
![image](images/postgres%20-%20business.png)
