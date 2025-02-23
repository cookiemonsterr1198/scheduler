import random
import os

from googleapiclient.discovery import build
from googleapiclient.discovery import build
from dateutil import tz
import pandas as pd
from tqdm import tqdm

from datetime import datetime

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

API_KEY = 'AIzaSyBsrQfvijz61DH5IoUF12uPoQ4fPin71As'
#'AIzaSyBsrQfvijz61DH5IoUF12uPoQ4fPin71As'
#'AIzaSyAeZw8emwI7Ux0D8V-ueLvmUMrrSZyFA_k'
USERNAME = 'BPSumut'
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
DATABASE_URL = "mongodb+srv://cookiemonsterr1198:dwiC@NT1K!@cluster0.qf1tn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Init
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# Step 0: Connect to Database
def connect_mongo():
    # Create a new client and connect to the server
    client = MongoClient(DATABASE_URL, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        
        db = client['insights_bpsumut']
        return db        
    except Exception as e:
        print(e)
        return
    
def save_to_mongo(data):
    db = connect_mongo()
    db.youtubes.insert_one(data)
    print("Data saved to MongoDB!")
    
# Step 1: Get channelId using the channel's username
def get_channel_id(username):
    # Get the channel ID using the user ID
    channel_response = youtube.search().list(
                        part='snippet',
                        type='channel',
                        q=username,
                        key=API_KEY).execute()

    if channel_response['items']:
        return channel_response['items'][0]['id']['channelId']
    else:
        return None
# Get!
channel_id = get_channel_id(USERNAME)

# Step 2: Get All Videos Id from Playlist
def get_video_from_playlist(channel_id):
  video_ids = []
  ids = []

  print("Get All Videos Id from Playlist..")
  # Get All Playlist
  request_playlist = youtube.playlists().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50
            )
  while request_playlist:
      response_playlist = request_playlist.execute()
      # Process the current Playlist
      for playlist in tqdm(response_playlist['items']):
        if 'youtube#playlist' in playlist['kind']:
          playlist_id = playlist['id']
          # GET ALL VIDEOS FROM PLAYLIST ID
          result = youtube.playlistItems().list(
                      part='snippet',playlistId=playlist_id, maxResults=50
                      ).execute()
          while result:
            for item in result['items']:
              video_id = item['snippet']['resourceId']['videoId']
              ids.append(video_id)
              video_ids.append({"videoId":video_id,"playlistId":playlist_id, "playlistTitle":playlist['snippet']['title']})
            # Check if there is a next page of videos on playlistid and get the next set of results
            if result.get('nextpageToken'):
              result = youtube.playlistItems().list(
                          part='snippet',playlistId=playlist_id,
                          maxResults=50, pageToken=result.get('nextPageToken')
                      )
            else:
              break

      # Check if there is a next page and get the next set of results
      if response_playlist.get('nextPageToken'):
        request_playlist = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=50,
            order='date',
            pageToken=response_playlist.get('nextPageToken')  # Get next page token if it exists
        )
      else:
        break
  return ids, video_ids

# Step 3: Get the list of video IDs from the channel
def get_video_ids(channel_id):
  video_ids = []

  print("Get the list of video IDs from the channel...")
  request = youtube.search().list(
    part='snippet',
    channelId=channel_id,
    maxResults=50,  # You can get up to 50 results per request
    order='date'  # Sort by upload date
  )
  while request:
      response = request.execute() #trigger
      for item in response['items']:
        if 'youtube#video' in item['id']['kind']:
          video_id = item['id']['videoId']
          if video_id not in video_ids: video_ids.append(video_id)
      # Check if there is a next page and get the next set of results
      if response.get('nextPageToken') :
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=50,
            order='date',
            pageToken=response.get('nextPageToken')  # Get next page token if it exists
        )
      else :
        break
  print("Done.")
  return video_ids


# Step 4: Get statistics (views, likes, comments) for each video
def get_video_stats(video_id):
  
  def get_int(prop):
    return int(prop) if prop else None

  def getPublishedAt(prop):
      if prop:
          from_zone = tz.gettz('UTC')
          to_zone =  tz.gettz('Asia/Jakarta')

          publishedAt_utc = datetime.strptime(prop, '%Y-%m-%dT%H:%M:%SZ')
          publishedAt_utc = publishedAt_utc.replace(tzinfo=from_zone)
          publishedAt = publishedAt_utc.astimezone(to_zone)

          return publishedAt

      else:
          return None

  # print(video_id)
  if (type(video_id)==dict):
    videoId = video_id['videoId']
    playlistId = video_id['playlistId']
    playlistTitle = video_id['playlistTitle']

  elif (type(video_id)==str):
    videoId = video_id
    playlistId = None
    playlistTitle = None

  # Exeute
  vd = youtube.videos().list(
    part='snippet,statistics',
    id=videoId).execute()

  if(vd['items']):
    channelId = vd['items'][0]['snippet'].get('channelId')
    if(channelId == channel_id):
      return {
          "Link": f"https://www.youtube.com/watch?v={videoId}",
        "videoId": videoId,
        'playlistId': playlistId,
        'playlistTitle': playlistTitle,
        "channelId": channelId,
        "channelTitle":vd['items'][0]['snippet'].get('channelTitle'),
        "title":vd['items'][0]['snippet'].get('title'),
        "publishedAt": getPublishedAt(vd['items'][0]['snippet'].get('publishedAt')).strftime("%Y-%m-%d %H:%M:%S"),
        'viewCount': get_int(vd['items'][0]['statistics'].get('viewCount')),
        'likeCount': get_int(vd['items'][0]['statistics'].get('likeCount')),
        'favoriteCount': get_int(vd['items'][0]['statistics'].get('favoriteCount')),
        'commentCount': get_int(vd['items'][0]['statistics'].get('commentCount')),
        'scrapedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      }
  return

# Run!
# Step 2
ids, video_ids = get_video_from_playlist(channel_id)

# Step 3
ids2 = get_video_ids(channel_id)

details = []
# Step 4
print("Get statistics (views, likes, comments) for each video..")
for id in tqdm(video_ids):
  # print(id)
  stat = get_video_stats(id)
  if stat:
    details.append(stat)
    save_to_mongo(stat)
for id2 in tqdm(ids2):
  if id2 not in ids:
    stat2 = get_video_stats(id2)
    if stat2:
      details.append(stat2)
    save_to_mongo(stat2)
details

# Export It to .csv
df = pd.DataFrame(details)
today = datetime.now().strftime("%Y-%m-%d")
df.to_csv("Youtube-Out.csv")
df