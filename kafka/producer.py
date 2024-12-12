import requests
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuration
API_KEY = '<your_api_key>'
CHANNEL_ID = 'UCtxD0x6AuNNqdXO9Wp5GHew'
KAFKA_TOPIC = 'testTopic'
KAFKA_SERVER = 'localhost:9092'
RUN_TIME_MINUTES = 10
INTERVAL_SECONDS = 40 # 2 minutes

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_latest_video(channel_id):
    """
    Fetch the latest video from the given channel.
    """
    url = f'https://www.googleapis.com/youtube/v3/search?part=id,snippet&channelId={channel_id}&maxResults=1&type=video&order=date&key={API_KEY}'
    response = requests.get(url).json()

    if response.get('items'):
        video = response['items'][0]
        video_id = video['id']['videoId']
        video_title = video['snippet']['title']
        return video_id, video_title
    return None, None

def get_video_views(video_id):
    """
    Fetch the current view count of a video.
    """
    url = f'https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={API_KEY}'
    response = requests.get(url).json()

    if response.get('items'):
        stats = response['items'][0]['statistics']
        return int(stats.get('viewCount', 0))
    return 0

def main():
    # Fetch the latest video
    video_id, video_title = get_latest_video(CHANNEL_ID)
    if not video_id:
        print("No video found for the channel.")
        return

    print(f"Tracking views for the latest video: {video_title} (ID: {video_id})")

    # Track view count changes
    start_time = datetime.now()
    initial_views = get_video_views(video_id)
    print(f"Initial view count: {initial_views}")

    while (datetime.now() - start_time) < timedelta(minutes=RUN_TIME_MINUTES):
        time.sleep(INTERVAL_SECONDS)
        
        current_views = get_video_views(video_id)
        view_difference = current_views - initial_views

        # Prepare the message
        message = {
            "videoId": video_id,
            "videoTitle": video_title,
            "initialViews": initial_views,
            "currentViews": current_views,
            "viewDifference": view_difference,
            "timestamp": datetime.now().isoformat()
        }

        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent to Kafka: {message}")

        # Update the initial view count for the next interval
        initial_views = current_views

    print("Producer finished after 10 minutes.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()
