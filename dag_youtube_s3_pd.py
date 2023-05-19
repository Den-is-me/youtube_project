import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from googleapiclient.discovery import build
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

dag = DAG(
    dag_id='dag_tr_youtube_s3_pd.py',
    default_args={'owner': 'aiflow'},
    schedule_interval='0 5 * * *',
    start_date=days_ago(1)
)


# Extract df from youtube and push it to xcom
# Put search patameters
def extract_youtube(**context):
    # Return information about videos on one page of You Tube search
    def youtube_search(options):
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                        developerKey=DEVELOPER_KEY)
        # Call the search.list method to retrieve results matching the specified query term
        search_response = youtube.search().list(**options).execute()
        return search_response

    # Return statistics of one video
    def video_statistics(options):
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                        developerKey=DEVELOPER_KEY)
        statistics = youtube.videos().list(**options).execute()
        return statistics['items'][0]

    youtube_df = pd.DataFrame(columns=['date_extract', 'video_id', 'title', 'description', 'view_count', 'like_count', 'comment_count',
                                       'time_published', 'channel_id', 'channel_title'])
    search_params = {'q': 'мультики для малышей',  # Request to youtube
                     'part': 'snippet',
                     'regionCode': 'RU',  # Return search results for videos that can be viewed in the specified country
                     'maxResults': 50}  # Max videos in result. Acceptable values are 0 to 50
    DEVELOPER_KEY = Variable.get('DEVELOPER_KEY')
    YOUTUBE_API_SERVICE_NAME = 'youtube'
    YOUTUBE_API_VERSION = 'v3'
    date_extract = context['ds']

    for i in range(20):  # Choose how many pages to extract
        search_result = youtube_search(search_params)

        for item in search_result['items']:
            video_id = item['id'].get('videoId', None)
            video_title = item['snippet']['title']
            time_published = item['snippet']['publishedAt']
            channel_title = item['snippet']['channelTitle']
            channel_id = item['snippet']['channelId']

            # Extract video reactions
            if video_id:
                description = video_statistics({'id': video_id, 'part': 'snippet'})['snippet']['description']
                actions = video_statistics({'id': video_id, 'part': 'statistics'})
                view_count = actions['statistics'].get("viewCount", None)
                like_count = actions['statistics'].get("likeCount", None)
                comment_count = actions['statistics'].get("commentCount", None)
            if not video_id:
                description = None
                view_count = None
                like_count = None
                comment_count = None

            # Add data to dataframe
            youtube_df = youtube_df.append({
                'date_extract': date_extract,
                'video_id': video_id,
                'title': video_title,
                'description': description,
                'view_count': view_count,
                'like_count': like_count,
                'comment_count': comment_count,
                'time_published': time_published,
                'channel_id': channel_id,
                'channel_title': channel_title},
                ignore_index=True)

        try:
            search_params['pageToken'] = search_result['nextPageToken']
        except KeyError:
            break

    context['ti'].xcom_push(key='youtube_df', value=youtube_df)


# Extract df from xcom and load it to s3
def load_df_s3(**context):
    s3_conn = S3Hook(aws_conn_id='s3')
    df = context['ti'].xcom_pull(task_ids='extract_youtube', key='youtube_df')
    csv_data = df.to_csv(index=False)
    file_name = f'youtube_extract_{context["ds"]}.csv'
    s3_conn.load_string(csv_data, key=file_name, bucket_name='youtube-analytics', replace=True)
    context['ti'].xcom_push(key='file_name', value=file_name)


# Extract df from xcom and load it to s3
def load_df_postgres(**context):
    pg_conn = PostgresHook(postgres_conn_id='postgres')
    df = context['ti'].xcom_pull(task_ids='extract_youtube', key='youtube_df')
    pd.set_option('display.max_columns', None)
    print(df.head(5))
    print(df.shape)

    df.dropna(subset=['video_id', 'channel_id'], inplace=True)
    df.fillna({'comment_count': 0,
               'view_count': 0,
               'like_count': 0},
              inplace=True)
    df.drop_duplicates(subset='video_id', inplace=True)

    pg_conn.insert_rows(table='youtube',
                        rows=df.values.tolist(),
                        target_fields=['date_extract', 'video_id', 'title', 'description', 'view_count', 'like_count', 'comment_count',
                                       'time_published', 'channel_id', 'channel_title'])


t1 = PythonOperator(task_id='extract_youtube',
                    python_callable=extract_youtube,
                    dag=dag)

t2 = PythonOperator(task_id='load_df_s3',
                    python_callable=load_df_s3,
                    dag=dag)

t3 = PythonOperator(task_id='tr_s3_postgres_staging',
                    python_callable=load_df_postgres,
                    dag=dag)

t4 = PostgresOperator(task_id='load_business',
                      postgres_conn_id='postgres',
                      sql='''
                    INSERT INTO business.channels
                    SELECT DISTINCT(channel_id), channel_title
                    FROM public.youtube
                    WHERE channel_id NOT IN (SELECT channel_id
                                            FROM business.channels)
                    ON CONFLICT DO NOTHING;

                    INSERT INTO business.videos
                    SELECT rank, date_extract, video_id, title, description, view_count, like_count, comment_count, time_published, channel_id
                    FROM public.youtube;
                    ''')

t5 = PostgresOperator(task_id='clear_staging_youtube',
                      postgres_conn_id='postgres',
                      sql='TRUNCATE public.youtube')

t1 >> [t2, t3], t3 >> t4 >> t5