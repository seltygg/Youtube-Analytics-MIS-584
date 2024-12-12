import requests
from isodate import parse_duration
from dbConnectors.mongoConnector import get_mongo_collection

# Constants
API_KEY = '<your-api-key>'  # Replace with your YouTube API key
CHANNEL_ID = 'UCBJycsmduvYEL83R_U4JriQ'  # Replace with your target channel ID
MAX_COMMENTS = 1000  # Maximum comments per video

# MongoDB Configuration
MONGO_CONNECTION_STRING = "<mongo_connection_string>"  # Replace with your MongoDB Atlas connection string
DATABASE_NAME = "MIS584"
COLLECTION_NAME = "VideoDetails"

def is_short(duration):
    """
    Checks if a video is considered a "short" (duration < 3 minutes).
    
    :param duration: ISO 8601 duration string
    :return: True if video duration is less than 3 minutes, False otherwise
    """
    parsed_duration = parse_duration(duration)
    return parsed_duration.total_seconds() < 180

def get_video_list(channel_id):
    """
    Fetches a list of video IDs from the specified YouTube channel.
    
    :param channel_id: YouTube channel ID
    :return: List of video IDs
    """
    videos = []
    url = f'https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&type=video&key={API_KEY}'
    while True:
        response = requests.get(url).json()
        video_ids = [item['id']['videoId'] for item in response.get('items', [])]

        # Filter out shorts
        videos_details_url = f'https://www.googleapis.com/youtube/v3/videos?part=contentDetails&id={"%2C".join(video_ids)}&key={API_KEY}'
        details_response = requests.get(videos_details_url).json()

        for item in details_response.get('items', []):
            if not is_short(item['contentDetails']['duration']):
                videos.append(item['id'])

        if 'nextPageToken' in response:
            url = f'https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&type=video&key={API_KEY}&pageToken={response["nextPageToken"]}'
        else:
            break
    return videos[:10]

def get_video_details(video_id):
    """
    Fetches details of a single video.
    
    :param video_id: Video ID
    :return: Dictionary containing video details
    """
    url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={API_KEY}'
    response = requests.get(url).json()
    if response['items']:
        snippet = response['items'][0]['snippet']
        stats = response['items'][0]['statistics']
        return {
            'videoId': video_id,
            'videoTitle': snippet.get('title', ''),
            'videoPublishedAt': snippet.get('publishedAt', ''),
            'videoLikes': stats.get('likeCount', '0'),
            'videoViews': stats.get('viewCount', '0'),
            'videoCommentCount': stats.get('commentCount', '0'),
        }
    return {}

def get_comments(video_id):
    """
    Fetches comments for a video.
    
    :param video_id: Video ID
    :return: List of comments data
    """
    comments_data = []
    url = f'https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&maxResults=100&key={API_KEY}'
    comment_count = 0

    while url and comment_count < MAX_COMMENTS:
        response = requests.get(url).json()
        for item in response.get('items', []):
            comment = item['snippet']['topLevelComment']['snippet']
            comment_data = {
                'commentId': item['id'],
                'authorId': comment['authorChannelId'].get('value', ''),
                'likeCount': comment.get('likeCount', '0'),
                'commentText': comment['textDisplay'],
                'commentPublishedAt': comment.get('publishedAt', ''),
                'replies': get_replies(item['id']) if item['snippet']['totalReplyCount'] > 0 else []
            }
            comments_data.append(comment_data)
            comment_count += 1
            if comment_count >= MAX_COMMENTS:
                break

        url = f'{url}&pageToken={response["nextPageToken"]}' if 'nextPageToken' in response else None

    return comments_data

def get_replies(parent_id):
    """
    Fetches replies for a comment.
    
    :param parent_id: Parent comment ID
    :return: List of replies
    """
    replies = []
    url = f'https://www.googleapis.com/youtube/v3/comments?part=snippet&parentId={parent_id}&maxResults=100&key={API_KEY}'
    while url:
        response = requests.get(url).json()
        for reply in response.get('items', []):
            replies.append({
                'commentId': reply['id'],
                'authorId': reply['snippet']['authorChannelId'].get('value', ''),
                'likeCount': reply['snippet'].get('likeCount', '0'),
                'commentText': reply['snippet']['textDisplay'],
                'commentPublishedAt': reply['snippet'].get('publishedAt', '')
            })
        url = f'{url}&pageToken={response["nextPageToken"]}' if 'nextPageToken' in response else None
    return replies

def fetch_comments_for_channel(channel_id):
    """
    Fetches comments for all videos in a channel and stores them in MongoDB.
    
    :param channel_id: YouTube channel ID
    """
    collection = get_mongo_collection(MONGO_CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME)
    total_comments = 0
    videos = get_video_list(channel_id)
    for video_id in videos:
        details = get_video_details(video_id)
        comments = get_comments(video_id)
        total_comments += len(comments)
        details['comments'] = comments

        # Insert data into MongoDB Atlas
        collection.insert_one(details)

    print(f"Total comments fetched across all videos: {total_comments}")

# Fetch comments and store in MongoDB Atlas
fetch_comments_for_channel(CHANNEL_ID)
print("Data inserted into MongoDB Atlas successfully!")
