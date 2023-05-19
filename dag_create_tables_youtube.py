from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

with DAG('create_tables_youtube',
         schedule_interval='0 0 1 1 0',
         start_date=days_ago(1),
         default_args={'owner': 'airflow'}) as dag:
    t1 = PostgresOperator(task_id='create_tables',
                          postgres_conn_id='postgres',
                          sql='''
                        DROP TABLE IF EXISTS public.youtube;
                        CREATE TABLE public.youtube (
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
                        channel_title text NULL);

                        DROP TABLE IF EXISTS business.channels CASCADE;
                        CREATE TABLE business.channels (
                        channel_id TEXT PRIMARY KEY,
                        channel_title text);

                        DROP TABLE IF EXISTS business.videos;
                        CREATE TABLE business.videos (
                        rank INT,
                        date_extract timestamp NOT NULL,
                        video_id TEXT NOT NULL,
                        title TEXT,
                        description TEXT,
                        view_count bigint,
                        like_count INT,
                        comment_count INT,
                        time_published TIMESTAMP,
                        channel_id TEXT NOT NULL REFERENCES business.channels(channel_id));
                        ''')

t1