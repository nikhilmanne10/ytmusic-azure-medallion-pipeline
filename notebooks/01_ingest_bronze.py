%pip install ytmusicapi azure-storage-file-datalake
dbutils.library.restartPython()
# CELL 3 — Full ingestion (charts + artists + songs with release year)
from ytmusicapi import YTMusic
from azure.storage.filedatalake import DataLakeServiceClient
import json
from datetime import datetime

storage_account = "adlsytmusic"
storage_key = "PASTE_YOUR_KEY_HERE"  # set via Key Vault / env var in production

countries = ["IN", "US", "GB", "BR", "JP"]
artists_to_track = [
    "Arijit Singh", "Taylor Swift", "The Weeknd",
    "Drake", "Ariana Grande", "Bad Bunny",
    "Ed Sheeran", "Beyoncé", "Bruno Mars", "Eminem"
]

ingestion_ts = datetime.utcnow().isoformat()
date_path = datetime.utcnow().strftime("%Y/%m/%d")

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=storage_key
)

def write_json_to_adls(container, file_path, data):
    file_system_client = service_client.get_file_system_client(file_system=container)
    file_client = file_system_client.get_file_client(file_path)
    content = json.dumps(data, indent=2)
    file_client.upload_data(content, overwrite=True)
    print(f"Written: {container}/{file_path}")

yt = YTMusic()

# ---- 1. Charts per country ----
charts_data = {}
for country in countries:
    try:
        charts_data[country] = yt.get_charts(country=country)
        print(f"Fetched charts for {country}")
    except Exception as e:
        charts_data[country] = {"error": str(e)}
        print(f"Failed for {country}: {e}")

charts_payload = {
    "ingested_at": ingestion_ts,
    "source": "ytmusicapi_charts",
    "charts_by_country": charts_data
}
write_json_to_adls("bronze", f"ytmusic/charts/{date_path}/charts_raw.json", charts_payload)

# ---- 2. Artist search metadata ----
artist_data = {}
artist_browse_ids = {}
for artist in artists_to_track:
    try:
        results = yt.search(artist, filter="artists", limit=1)
        if results:
            artist_data[artist] = results[0]
            artist_browse_ids[artist] = results[0]["browseId"]
        else:
            artist_data[artist] = {"error": "not found"}
        print(f"Fetched artist data for {artist}")
    except Exception as e:
        artist_data[artist] = {"error": str(e)}
        print(f"Failed for {artist}: {e}")

artist_payload = {
    "ingested_at": ingestion_ts,
    "source": "ytmusicapi_artists",
    "artists": artist_data
}
write_json_to_adls("bronze", f"ytmusic/artists/{date_path}/artists_raw.json", artist_payload)

# ---- 3. Songs per artist (with album info, for release-year lookup) ----
song_data = []
albums_to_lookup = {}

for artist, browse_id in artist_browse_ids.items():
    try:
        artist_detail = yt.get_artist(browse_id)
        songs_section = artist_detail.get("songs", {}).get("results", [])
        for song in songs_section:
            album_info = song.get("album")
            album_id = album_info.get("id") if album_info else None
            song_data.append({
                "artist_name": artist,
                "song_title": song.get("title"),
                "video_id": song.get("videoId"),
                "album_id": album_id,
                "album_name": album_info.get("name") if album_info else None,
                "year_direct": song.get("year")
            })
            if album_id:
                albums_to_lookup[album_id] = album_info.get("name")
        print(f"Fetched {len(songs_section)} songs for {artist}")
    except Exception as e:
        print(f"Failed songs for {artist}: {e}")

# ---- 4. Look up release year per unique album ----
album_years = {}
for album_id in albums_to_lookup:
    try:
        album_detail = yt.get_album(album_id)
        album_years[album_id] = album_detail.get("year")
    except Exception as e:
        album_years[album_id] = None
        print(f"Failed album lookup {album_id}: {e}")

# ---- 5. Merge year into song records ----
for song in song_data:
    if song["album_id"] and song["album_id"] in album_years:
        song["year"] = song["year_direct"] or album_years[song["album_id"]]
    else:
        song["year"] = song["year_direct"]

songs_payload = {
    "ingested_at": ingestion_ts,
    "source": "ytmusicapi_songs",
    "songs": song_data
}
write_json_to_adls("bronze", f"ytmusic/songs/{date_path}/songs_raw.json", songs_payload)

print("\n✅ Bronze ingestion complete (charts + artists + songs).")
