import psycopg2
import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
load_dotenv()


# Put parameters using API documentation(https://developers.google.com/youtube/v3/docs/search/list)
search_params = {'q': 'Nature',  # Requset to youtube
                 'part': 'snippet',
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
        print(f'An HTTP error {e.resp.status} occurred:\n{e.content}')


# connect to Postgres
connect = psycopg2.connect(host=os.getenv('host'),
                           database=os.getenv('database'),
                           user=os.getenv('user'),
                           password=os.getenv('password'))
cursor = connect.cursor()

for i in range(8):  # choose how many pages to extract
    search_result = youtube_search(search_params)

    # Insert data into Postgres
    for item in search_result['items']:
        video_id = item['id']['videoId']
        video_title = item['snippet']['title']
        published_date = item['snippet']['publishedAt']
        description = item['snippet']['description']
        channel_title = item['snippet']['channelTitle']
        channel_id = item['snippet']['channelId']

        # Check if channel already exists in 'channel' table
        cursor.execute("SELECT channel_id FROM channel WHERE channel_id = %s;", (channel_id,))
        result = cursor.fetchone()
        if not result:
            # Insert channel data into channel table
            cursor.execute('''INSERT INTO channel (channel_id, title)
                           VALUES (%s, %s);''', (channel_id, channel_title))
            connect.commit()

        # Check if video_id already exists in video table
        cursor.execute("SELECT video_id FROM video WHERE video_id = %s;", (video_id,))
        result = cursor.fetchone()
        if not result:
            # Insert video data into 'video' table
            cursor.execute("""INSERT INTO video
                           (video_id, title, description, time_published, channel_id)
                           VALUES (%s, %s, %s, %s, %s);""",
                           (video_id, video_title, description, published_date, channel_id))
            connect.commit()

    search_params['pageToken'] = search_result['nextPageToken']

cursor.close()
connect.close()
