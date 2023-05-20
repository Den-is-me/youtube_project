# Youtube project
**📊  Analysis youtube search algorithm**
## Architectural solution
## Plan:
- [Idea](#idea)
- [Goals](#goals)
- [Stack](#stack-used)
## Realization:
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
### Extract from YouTube
I used a Virtual Machine (CPU2, RAM 8Gb, HDD 50Gb) in Compute Cloud with Docker containers for Airflow and Postgres.
I have used the [Google API](https://developers.google.com/youtube/v3/docs/search/list) to extract information from search results with request: "мультики для малышей".
### Transform and Load
### Analysis
### Conclusion
![image](images/solution.png)
![image](images/dag.png)
![image](images/postgres%20-%20business.png)
