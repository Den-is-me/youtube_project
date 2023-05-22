import pandas as pd
import io

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_from_s3_clear_load_pd(**context):
    pg_conn = PostgresHook(postgres_conn_id='postgres')
    s3_conn = S3Hook(aws_conn_id='s3')
    s3_file = s3_conn.read_key('youtube_extract_2023-05-21.csv', bucket_name='youtube-analytics')
    df = pd.read_csv(io.StringIO(s3_file))
    print(df.head())

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


with DAG('dag_fix',
         schedule_interval='0 0 1 1 0',
         start_date=days_ago(1),
         default_args={'owner': 'airflow'}) as dag:
    t1 = PostgresOperator(task_id='create_staging_youtube',
                          postgres_conn_id='postgres',
                          sql='''
                        CREATE TABLE IF NOT EXISTS public.youtube (
                        rank serial4 NOT NULL,
                        date_extract timestamp NOT NULL,
                        video_id text NOT NULL,
                        title text NULL,
                        description text NULL,
                        view_count bigint NULL,
                        like_count int4 NULL,
                        comment_count int4 NULL,
                        time_published timestamp NULL,
                        channel_id text NOT NULL,
                        channel_title text NULL)
                    ''')

    t2 = PythonOperator(task_id='read_s3_load_pd',
                        python_callable=extract_from_s3_clear_load_pd)

    t3 = PostgresOperator(task_id='load_business',
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

    t4 = PostgresOperator(task_id='clear_staging_youtube',
                          postgres_conn_id='postgres',
                          sql='DROP TABLE public.youtube')

t1 >> t2 >> t3 >> t4