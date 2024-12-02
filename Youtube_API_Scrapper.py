import requests
import json

API_KEY = '<your-api-key>'
CHANNEL_ID = 'UCBJycsmduvYEL83R_U4JriQ'
MAX_COMMENTS = 1000  # Maximum comments per video

def get_video_list(channel_id):
    videos = []
    url = f'https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&type=video&key={API_KEY}'
    while True:
        response = requests.get(url).json()
        video_ids = [item['id']['videoId'] for item in response.get('items', [])]

        # Retrieve content details for each video to filter out shorts based on duration
        videos_details_url = f'https://www.googleapis.com/youtube/v3/videos?part=contentDetails&id={"%2C".join(video_ids)}&key={API_KEY}'
        details_response = requests.get(videos_details_url).json()

        for item in details_response.get('items', []):
            duration = item['contentDetails']['duration']
            if not is_short(duration):  # Filter out shorts
                videos.append(item['id'])

        if 'nextPageToken' in response:
            url = f'https://www.googleapis.com/youtube/v3/search?part=id&channelId={channel_id}&maxResults=50&type=video&key={API_KEY}&pageToken={response["nextPageToken"]}'
        else:
            break
    return videos[:10]

def is_short(duration):
    # Returns True if duration is less than 60 seconds
    from isodate import parse_duration
    parsed_duration = parse_duration(duration)
    return parsed_duration.total_seconds() < 180

def get_video_details(video_id):
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
                'dislikeCount': comment.get('dislikeCount', '0'),  # API doesn’t provide dislike count
                'totalReplyCount': item['snippet'].get('totalReplyCount', 0),
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
    replies = []
    url = f'https://www.googleapis.com/youtube/v3/comments?part=snippet&parentId={parent_id}&maxResults=100&key={API_KEY}'
    while url:
        response = requests.get(url).json()
        for reply in response.get('items', []):
            replies.append({
                'commentId': reply['id'],
                'authorId': reply['snippet']['authorChannelId'].get('value', ''),
                'likeCount': reply['snippet'].get('likeCount', '0'),
                'dislikeCount': reply['snippet'].get('dislikeCount', '0'),  # API doesn’t provide dislike count
                'commentText': reply['snippet']['textDisplay'],
                'commentPublishedAt': reply['snippet'].get('publishedAt', '')
            })
        url = f'{url}&pageToken={response["nextPageToken"]}' if 'nextPageToken' in response else None
    return replies

def fetch_comments_for_channel(channel_id):
    total_comments = 0
    videos = get_video_list(channel_id)
    video_data = []
    for video_id in videos:
        details = get_video_details(video_id)
        comments = get_comments(video_id)
        total_comments += len(comments)
        details['comments'] = comments
        video_data.append(details)
    
    print(f"Total comments fetched across all videos: {total_comments}")
    return video_data

# Fetch comments and save to JSON
channel_comments_data = fetch_comments_for_channel(CHANNEL_ID)
output_data = {
    "videos": channel_comments_data
}

# Save to JSON file
with open("channel_comments_data.json", "w", encoding="utf-8") as file:
    json.dump(output_data, file, ensure_ascii=False, indent=4)

print("Data saved to channel_comments_data.json")
