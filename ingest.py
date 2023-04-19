from dotenv import load_dotenv
import os
import base64
import requests
from requests import post
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util
from json.decoder import JSONDecodeError


load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = os.getenv("REDIRECT_URI")


def get_token():
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
    return {"Authorization": "Bearer " + token}



spotify_token = get_token()


os.environ["SPOTIPY_CLIENT_ID"] = client_id
os.environ["SPOTIPY_CLIENT_SECRET"] = client_secret
os.environ["SPOTIPY_REDIRECT_URI"] = redirect_uri

# scope = "user-read-playback-state"
# sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
# current_playing_track_data = sp.current_user_playing_track()
# print(current_playing_track_data)

def search_spotify_year(query, token, offset = 0, limit=50):
    search_url = "https://api.spotify.com/v1/search"

    headers = {
        "Authorization" : f"Bearer {token}"
    }
    params = {
        "q" : query,
        "type" : "track",
        "offset" : offset,
        "limit" : limit
    }

    response = requests.get(search_url, headers = headers, params = params)
    response_data = response.json()

    if 'tracks' in response_data:
        return response_data['tracks']['items']
    else:
        print(f"Error fetching data for offset {offset}: {response_data}")
        return []




def get_top_songs_year(year, token, total = 200):
    tracks = []
    query = f"year:{year}"
    limit = 50

    for offset in range(0, total, limit):
        results = search_spotify_year(query,token,offset,limit)
        tracks.extend(results)

    return tracks[:total]


start_year = 2021
end_year = 2021

top_songs_by_year = {}

for year in range(start_year, end_year + 1):
    print(f"Fetching top 200 songs for {year}...")
    top_songs_by_year[year] = get_top_songs_year(year, spotify_token)
    print(f"Completed fetching top 200 songs for {year}")

# Example: Print the top 5 songs for each year
for year, tracks in top_songs_by_year.items():
    print(f"\nTop 5 songs in {year}:")
    print(tracks[:1])
    # for i, track in enumerate(tracks[:5]):
    #     print(f"{i + 1}. {track['name']} by {track['artists'][0]['name']}")

# print the length of top_songs_by_year

for year, tracks in top_songs_by_year.items():
    print(f"\n {year}:", len(tracks))