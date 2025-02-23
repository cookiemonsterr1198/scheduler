import urllib.parse

from googleapiclient.discovery import build
from googleapiclient.discovery import build
import pandas as pd
from tqdm import tqdm
from dateutil import tz
from datetime import datetime

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MONGO PARAMS
MONGO_USERNAME = urllib.parse.quote_plus("cookiemonsterr1198")
MONGO_PASSWORD = urllib.parse.quote_plus("dwiC@NT1K!")
DATABASE_NAME = "insights_bpsumut"
COLLECTION_NAME = "youtubes"

""" CLASS MONGODB """


class MongoDB:
    def __init__(self, MONGO_USERNAME, MONGO_PASSWORD, DATABASE_NAME, COLLECTION_NAME):
        self.DATABASE_URL = (
            "mongodb+srv://%s:%s@cluster0.qf1tn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
            % (MONGO_USERNAME, MONGO_PASSWORD)
        )
        self.DATABASE_NAME = DATABASE_NAME
        self.COLLECTION_NAME = COLLECTION_NAME

    # Method: Connect to Database
    def connect_mongo(self):
        # Create a new client and connect to the server
        client = MongoClient(self.DATABASE_URL, server_api=ServerApi("1"))
        try:
            client.admin.command("ping")
            print("Pinged your deployment. You successfully connected to MongoDB!")
            self.db = client[DATABASE_NAME]
        except Exception as e:
            print(e)
            return

    # Method: Clear Collections
    def clear_mongo(self):
        self.db[self.COLLECTION_NAME].delete_many({})
        print("MongoDB Collections has been cleared!")

    # Method: Store Documents
    def store_mongo(self, data):
        self.db[self.COLLECTION_NAME].insert_many(data)
        print("Data saved to MongoDB!")


# YOUTUBE PARAMS
API_KEYS = [
    "AIzaSyBsrQfvijz61DH5IoUF12uPoQ4fPin71As",
    "AIzaSyBsrQfvijz61DH5IoUF12uPoQ4fPin71As",
    "AIzaSyAeZw8emwI7Ux0D8V-ueLvmUMrrSZyFA_k",
]
API_KEY = API_KEYS[2]
USERNAME = "BPSumut"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


""" CLASS YOUTUBE """


class Youtube:
    def __init__(
        self, API_KEY, USERNAME, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION
    ):
        self.API_KEY = API_KEY
        self.USERNAME = USERNAME
        self.YOUTUBE_API_SERVICE_NAME = YOUTUBE_API_SERVICE_NAME
        self.YOUTUBE_API_VERSION = YOUTUBE_API_VERSION
        self.build()
        self.get_channel_id()

    # Method: Initialize
    def build(self):
        self.youtube = build(
            self.YOUTUBE_API_SERVICE_NAME,
            self.YOUTUBE_API_VERSION,
            developerKey=self.API_KEY,
        )

    # Method 1: Get channelId using the channel's username
    def get_channel_id(self):
        channel_response = (
            self.youtube.search()
            .list(part="snippet", type="channel", q=self.USERNAME, key=self.API_KEY)
            .execute()
        )

        if channel_response["items"]:
            self.user_id = channel_response["items"][0]["id"]["channelId"]
        else:
            self.user_id = None

    # Method 2: Get All Videos Id from Playlist
    def get_video_from_playlist(self):
        self.video_ids = []
        self.ids = []

        # Get All Playlist
        print("Get All Videos Id from Playlist..")
        request_playlist = self.youtube.playlists().list(
            part="snippet", channelId=self.user_id, maxResults=50
        )
        while request_playlist:
            response_playlist = request_playlist.execute()

            for playlist in tqdm(response_playlist["items"]):

                if "youtube#playlist" in playlist["kind"]:
                    playlist_id = playlist["id"]
                    # Get All Videos from current PlaylistId
                    result = (
                        self.youtube.playlistItems()
                        .list(part="snippet", playlistId=playlist_id, maxResults=50)
                        .execute()
                    )
                    while result:
                        for item in result["items"]:
                            video_id = item["snippet"]["resourceId"]["videoId"]
                            self.ids.append(video_id)
                            self.video_ids.append(
                                {
                                    "videoId": video_id,
                                    "playlistId": playlist_id,
                                    "playlistTitle": playlist["snippet"]["title"],
                                }
                            )
                        # Check if there is a next page of videos on playlistid and get the next set of results
                        if result.get("nextpageToken"):
                            result = self.youtube.playlistItems().list(
                                part="snippet",
                                playlistId=playlist_id,
                                maxResults=50,
                                pageToken=result.get("nextPageToken"),
                            )
                        else:
                            break

            # Check if there is a next page and get the next set of results
            if response_playlist.get("nextPageToken"):
                request_playlist = self.youtube.search().list(
                    part="snippet",
                    channelId=self.user_id,
                    maxResults=50,
                    order="date",
                    pageToken=response_playlist.get(
                        "nextPageToken"
                    ),  # Get next page token if it exists
                )
            else:
                break

    # Method 3: Get the list of video IDs from the channel
    def get_video_from_user(self):
        self.video_ids2 = []

        print("Get the list of video IDs from the channel...")
        request = self.youtube.search().list(
            part="snippet",
            channelId=self.user_id,
            maxResults=50,  # You can get up to 50 results per request
            order="date",  # Sort by upload date
        )
        while request:
            response = request.execute()  # trigger
            for item in response["items"]:
                if "youtube#video" in item["id"]["kind"]:
                    video_id = item["id"]["videoId"]
                    if video_id not in self.video_ids:
                        self.video_ids2.append(video_id)
            # Check if there is a next page and get the next set of results
            if response.get("nextPageToken"):
                request = self.youtube.search().list(
                    part="snippet",
                    channelId=self.user_id,
                    maxResults=50,
                    order="date",
                    pageToken=response.get(
                        "nextPageToken"
                    ),  # Get next page token if it exists
                )
            else:
                break
        print("Done.")
    
    # Additional Functions
    def convert_timezone(self, prop):
        from_zone = tz.gettz("UTC")
        to_zone = tz.gettz("Asia/Jakarta")
        
        timestamp_utc = prop.replace(tzinfo=from_zone)
        timestamp = timestamp_utc.astimezone(to_zone)
        return timestamp

    # Method 4: Get statistics (views, likes, comments) for each video
    def get_video_stats(self, video_id):

        def to_int(prop):
            return int(prop) if prop else None

        def getPublishedAt(prop):
            if prop:
                publishedAt_utc = datetime.strptime(prop, "%Y-%m-%dT%H:%M:%SZ")
                return self.convert_timezone(publishedAt_utc)
            else:
                return None

        # print(video_id)
        if type(video_id) == dict:
            videoId = video_id["videoId"]
            playlistId = video_id["playlistId"]
            playlistTitle = video_id["playlistTitle"]

        elif type(video_id) == str:
            videoId = video_id
            playlistId = None
            playlistTitle = None

        # Exeute
        vd = self.youtube.videos().list(part="snippet,statistics", id=videoId).execute()

        if vd["items"]:
            channelId = vd["items"][0]["snippet"].get("channelId")
            if channelId == self.user_id:
                return {
                    "Link": f"https://www.youtube.com/watch?v={videoId}",
                    "videoId": videoId,
                    "playlistId": playlistId,
                    "playlistTitle": playlistTitle,
                    "channelId": channelId,
                    "channelTitle": vd["items"][0]["snippet"].get("channelTitle"),
                    "title": vd["items"][0]["snippet"].get("title"),
                    "publishedAt": getPublishedAt(
                        vd["items"][0]["snippet"].get("publishedAt")
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "viewCount": to_int(vd["items"][0]["statistics"].get("viewCount")),
                    "likeCount": to_int(vd["items"][0]["statistics"].get("likeCount")),
                    "favoriteCount": to_int(
                        vd["items"][0]["statistics"].get("favoriteCount")
                    ),
                    "commentCount": to_int(
                        vd["items"][0]["statistics"].get("commentCount")
                    ),
                    "scrapedAt": self.convert_timezone(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                }

    # Method : Run!
    def run_statistics(self):

        print("Get statistics (views, likes, comments) for each video..")
        details = []

        # Run Method 2:
        self.get_video_from_playlist()
        for id in tqdm(self.video_ids):
            # print(id)
            stat = self.get_video_stats(id)
            if stat:
                details.append(stat)

        # Run Method 3:
        self.get_video_from_user()
        for id2 in tqdm(self.video_ids2):
            if id2 not in self.ids:
                stat2 = self.get_video_stats(id2)
                if stat2:
                    details.append(stat2)

        return details


# Initialize MongoDB
mongoDB = MongoDB(MONGO_USERNAME, MONGO_PASSWORD, DATABASE_NAME, COLLECTION_NAME)
mongoDB.connect_mongo()
mongoDB.clear_mongo()

# Initialize Youtube
youtube = Youtube(API_KEY, USERNAME, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION)
data = youtube.run_statistics()
mongoDB.store_mongo(data)  # store to mongoDB

# Export Data to .csv
df = pd.DataFrame(data)
df.to_csv("Youtube-Out.csv")
df
