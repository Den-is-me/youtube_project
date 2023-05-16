import psycopg2
import os
import logging
from datetime import date
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(filename='add.log', level=logging.INFO)
logging.info('Starting the script ' + str(date.today()))
load_dotenv()

# Put parameters using API documentation(https://developers.google.com/youtube/v3/docs/search/list)
search_params = {'q': 'Stand up',  # Request to youtube
                 'part': 'snippet',
                 'regionCode': 'RU',  # return search results for videos that can be viewed in the specified country.
                 'maxResults': 5}  # Max videos in result
DEVELOPER_KEY = os.getenv('DEVELOPER_KEY')
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'


def youtube_search(options):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)

    # Call the search.list method to retrieve results matching the specified
    # query term.
    try:
        search_response = youtube.search().list(**options).execute()
        return search_response
    except HttpError as e:
        logging.error(f'Channel id: {channel_id} Search Error {e}')
        print(f'An HTTP error {e.resp.status} occurred:\n{e.content}')



def video_statistics(options):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)
    try:
        statistics = youtube.videos().list(**options).execute()
        return statistics['items'][0]
    except HttpError as e:
        logging.error(f'{video_id} Video statistics Error {e}')
        print(f'An HTTP error {e.resp.status} occurred:\n{e.content}')


# Connect to Postgres
connect = psycopg2.connect(host=os.getenv('host'),
                           database=os.getenv('database'),
                           user=os.getenv('user'),
                           password=os.getenv('password'))
cursor = connect.cursor()

for i in range(5):  # Choose how many pages to extract
    search_result = youtube_search(search_params)
    date_extract = date.today()

    # Insert data into Postgres
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

        # Insert video data into youtube table
        cursor.execute("""INSERT INTO youtube
                       (date_extract, video_id, title, description, view_count, like_count, comment_count, 
                       time_published, channel_id, channel_title)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                       (date_extract, video_id, video_title, description, view_count, like_count,
                        comment_count, time_published, channel_id, channel_title))
        connect.commit()

    search_params['pageToken'] = search_result['nextPageToken']
    logging.info('Page extracted')

cursor.close()
connect.close()
logging.info('Script finished.')
