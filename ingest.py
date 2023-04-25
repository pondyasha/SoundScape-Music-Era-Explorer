from dotenv import load_dotenv
import os
import base64
from google.oauth2 import service_account
from google.cloud import bigquery
import requests
from requests import post
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util
from json.decoder import JSONDecodeError
import pandas as pd


load_dotenv()
# BigQuery Set up authentication
key_path = '/Users/ashapondicherry/Desktop/SoundScape/Secrets/soundspace-384822-d35981d20967.json'
bq_credentials = service_account.Credentials.from_service_account_file(key_path)

# Create BigQuery client
bq_project_id = 'soundspace-384822'
bq_client = bigquery.Client(project=bq_project_id, credentials=bq_credentials)

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = os.getenv("REDIRECT_URI")


def get_token():
    """Retrieve an access token for the Spotify API.

        Returns:
            A string representing the access token.
    """
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type" : "client_credentials"}
    result = post(url, headers=headers, data = data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    """Generate an authentication header for the Spotify API.

        Args:
            token: A string representing the access token.

        Returns:
            A dictionary representing the authentication header.
    """
    return {"Authorization": "Bearer " + token}



spotify_token = get_token()



def search_spotify_year(query, token, offset = 0, limit=50):
    """Search for tracks on Spotify by year.

        Args:
            query: A string representing the search query.
            token: A string representing the access token.
            offset: An integer representing the offset.
            limit: An integer representing the limit.

        Returns:
            A list of dictionaries representing the tracks returned by the search query.
    """
    search_url = "https://api.spotify.com/v1/search"

    headers = {
        "Authorization" : f"Bearer {token}"
    }
    params = {
        "q" : query,
        "type" : "track",
        "offset" : offset,
        "limit" : limit,
        "market" : "US"
    }
    headers = headers
    params = params

    response = requests.get(search_url, headers = headers, params = params)
    response_data = response.json()

    if 'tracks' in response_data:
        return response_data['tracks']['items']
    else:
        print(f"Error fetching data for offset {offset}: {response_data}")
        return []


def get_top_songs_year(year, token, total = 200):
    """Retrieve the top songs for a given year from the Spotify API.

        Args:
            year: An integer representing the year to retrieve the top songs for.
            token: A string representing the access token.
            total: An integer representing the total number of songs to retrieve.

        Returns:
            A list of dictionaries representing the top songs for the given year.
    """
    tracks = []
    query = f"year:{year}"
    limit = 50

    for offset in range(0, total, limit):
        results = search_spotify_year(query,token,offset,limit)
        tracks.extend(results)

    return tracks[:total]


start_year = 2023
end_year = 2023

top_songs_by_year = {}

for year in range(start_year, end_year + 1):
    print(f"Fetching top 200 songs for {year}...")
    top_songs_by_year[year] = get_top_songs_year(year, spotify_token)
    print(f"Completed fetching top 200 songs for {year}")

for year, tracks in top_songs_by_year.items():
    for track in tracks:
        artist_id = track["artists"][0]["id"]

def get_artist_popularity(artist_id):
    """
        Given a list of artist IDs, return a dictionary containing the popularity of each artist.

        Parameters:
        artist_id (list of str): A list of artist IDs.

        Returns:
        dict: A dictionary where the keys are artist IDs and the values are the popularity scores for each artist.
    """
    artist_popularities = {}
    for artist_id in artist_ids:
        # Set up the request to the Artist API
        artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
        response = requests.get(artist_url, headers= {"Authorization": f"Bearer {get_token()}"})
        if response.status_code == 200:
            artist_data = response.json()
            artist_popularity = artist_data["popularity"]
            artist_popularities[artist_id] = artist_popularity
        else:
            print(f"Error retrieving artist information for artist ID {artist_id}.")
            artist_popularities[artist_id] = None
    return artist_popularities

def get_artist_genre(artist_id):
    """
        Given an artist ID, returns a dictionary of the artist ID and its corresponding genres from the Spotify API.

        Args:
            artist_id (str): The unique identifier for the artist on Spotify.

        Returns:
            dict: A dictionary where the keys are the input artist ID and the values are lists of genres.
    """
    artist_genres = {}
    for artist_id in artist_ids:
        # Set up the request to the Artist API
        artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
        response = requests.get(artist_url, headers= {"Authorization": f"Bearer {get_token()}"})
        if response.status_code == 200:
            artist_data = response.json()
            artist_genre = artist_data["genres"]
            artist_genres[artist_id] = artist_genre
        else:
            print(f"Error retrieving artist information for artist ID {artist_id}.")
            artist_genres[artist_id] = None
    return artist_genres

artist_popularities = {}
for year, tracks in top_songs_by_year.items():
    artist_ids = [track["artists"][0]["id"] for track in tracks]
    artist_popularities.update(get_artist_popularity(artist_ids))

artist_genres = {}
for year, tracks in top_songs_by_year.items():
    artist_ids = [track["artists"][0]["id"] for track in tracks]
    artist_genres.update(get_artist_genre(artist_ids))


for year, tracks in top_songs_by_year.items():
    top_songs_by_year[year] = []
    for track in tracks:
        artist_id = track["artists"][0]["id"]
        artist_popularity = artist_popularities[artist_id]
        genres = artist_genres[artist_id]
        track_data = {
            "track_name": track["name"],
            "year": year,
            "artist_name": track["artists"][0]["name"],
            "album_name": track["album"]["name"],
            "duration_ms": track["duration_ms"],
            "song_popularity": track["popularity"],
            "release_date": track["album"]["release_date"],
            "total_tracks_in_album": track["album"]["total_tracks"],
            "album_cover": track["album"]["images"][0],
            "artist_popularity" : artist_popularity,
            "artist_genres" : genres
        }

        top_songs_by_year[year].append(track_data)


# #
# #Convert top_songs_by_year dictionary to a DataFrame
df = pd.DataFrame([item for year in top_songs_by_year.values()
                   for item in year])
# df.to_csv("/Users/ashapondicherry/Desktop/test.csv")

# Set up the dataset ID and create the dataset
dataset_id = "soundscape_spotify_api_data"
dataset_ref = bq_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "US"
dataset = bq_client.create_dataset(dataset)

# Set up the table ID and create the table
table_id = "top_200_songs_from_1920_to_2023"
table_ref = dataset_ref.table(table_id)

schema = [
    bigquery.SchemaField("track_name", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("artist_name", "STRING"),
    bigquery.SchemaField("album_name", "STRING"),
    bigquery.SchemaField("duration_ms", "INTEGER"),
    bigquery.SchemaField("song_popularity", "INTEGER"),
    bigquery.SchemaField("release_date", "DATE"),
    bigquery.SchemaField("total_tracks_in_album", "INTEGER"),
    bigquery.SchemaField("album_cover", "STRING"),
    bigquery.SchemaField("artist_popularity", "INTEGER"),
    bigquery.SchemaField("artist_genres", "STRING", mode="REPEATED"),
]
table = bigquery.Table(table_ref, schema=schema)
table = bq_client.create_table(table)

df= df.to_csv("/Users/ashapondicherry/Desktop/SoundScape/test.csv")
df = pd.read_csv("/Users/ashapondicherry/Desktop/SoundScape/test.csv")
df.to_gbq(destination_table=table_ref, project_id=bq_project_id, if_exists='append')
